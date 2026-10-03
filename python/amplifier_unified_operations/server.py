"""Private stdio owner protocol; host authority is passed by authenticated callbacks."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
from filelock import FileLock
from jsonschema import Draft202012Validator,ValidationError
from .errors import ScheduleRefused
from .history import HistoricalRecords
from .quiescence import Intake
from .retention import References
from .transfer import TransferEvidence
from .scheduler import Scheduler
from .operations import Operations
from .questions import Questions
from .observations import Observations
from . import schedules_schemas,operations_schemas,questions_schemas,observations_schemas

MAX_FRAME=4_000_000

def schema(fields,required=None):return {'type':'object','properties':fields,'required':list(fields) if required is None else required,'additionalProperties':False}
def string(maximum):return {'type':'string','maxLength':maximum}

def definitions():
    values={**schedules_schemas.definitions(schema,string),**operations_schemas.definitions(schema,string),**questions_schemas.definitions(schema,string),**observations_schemas.definitions(schema,string)}
    for name in ('schedule.list','schedule.read'):
        values[name][1]['properties'].update(limit={'type':'integer','minimum':1,'maximum':100},cursor={'type':['number','null'],'minimum':0})
    for _,definition in values.values():definition['properties']['sessionId']['maxLength']=8192
    values['operations.list'][1]['properties']['cursor']={'type':'string','maxLength':1024}
    values['question.list'][1]['properties']['cursor']={'type':'string','maxLength':1024}
    values['question.list'][1]['properties']['offset']['maximum']=1000
    for name in ('observation.list','observation.observers','observation.read','observation.report'):
        values[name][1]['properties'].update(limit={'type':'integer','minimum':1,'maximum':50},cursor={'type':'string','maxLength':1024})
    return {name:{'description':row[0],'schema':row[1]} for name,row in values.items()}

class Owner:
    def __init__(self,config,host,notify):
        root=Path(config['dataDir']).expanduser().resolve();root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.lease=FileLock(str(root/'owner.lock'));self.lease.acquire(timeout=0)
        self.intake=Intake(root/'intake.sqlite3')
        self.host=host;self.notify=notify;self.schemas=definitions();self.revisions={};self.tick_task=None;self.closed=False;self.awaiting_idle=False
        self.root=root;self._historical=None
        self.questions=Questions(root/'questions.sqlite3',host,lambda sid:self.changed(sid,'questions'))
        self.schedules=Scheduler(root/'schedules.sqlite3',self.callback,lambda sid:self.changed(sid,'schedules'))
        self.operations=Operations(root/'operations.sqlite3',self.callback,lambda sid:self.changed(sid,'operations'))
        self.observations=Observations(root/'observations.sqlite3',self.callback,lambda sid:self.changed(sid,'observations'));self.observations_enabled=False
        self.transfer=TransferEvidence(root/'transfer.sqlite3',self)
        self.retention=References(self)
    async def callback(self,method,args):
        if method=='questionDependency':return self.questions.dependency(args['session'],args['id'])
        if method=='operationSources':
            own=self.observations.store.operations(args['session'],args.get('id'),args.get('limit',50),args.get('cursor')) if self.observations_enabled else []
            external=await self.host(method,args)
            if not isinstance(external,list) or len(external)>100:raise ValueError('Invalid external operation page')
            return sorted(own+external,key=lambda row:(row.get('createdAt') or 0,row['id']),reverse=True)[:args.get('limit',50)]
        return await self.host(method,args)
    async def changed(self,sid,topic):
        # This map is presentation-only and capped; authoritative revision stays on disk.
        self.revisions.pop(sid,None);self.revisions[sid]=0
        while len(self.revisions)>128:self.revisions.pop(next(iter(self.revisions)))
        await self.notify('owner/changed',{'session':sid,'topic':topic})
    def start(self):
        if self.tick_task is None:self.tick_task=asyncio.create_task(self.loop())
    async def maybe_idle(self):
        if self.awaiting_idle and not self.closed and not self.intake.calls and not self.intake.background and not self.schedules.pending and not self.observations.tasks:
            self.awaiting_idle=False
            await self.notify('owner/idle',{})
    async def loop(self):
        while not self.closed:
            if not self.intake.fence:
                self.intake.background+=1
                try:
                    try:await self.schedules.tick()
                    except asyncio.CancelledError:raise
                    except Exception as exc:self.schedules.error=type(exc).__name__
                    if self.observations_enabled:await self.observations.tick()
                finally:
                    self.intake.background-=1
                    await self.maybe_idle()
            await asyncio.sleep(2)
    async def request(self,method,params):
        if method=='quiescence.retention':return self.retention.read(params)
        if method=='quiescence.managedFiles':return self.retention.read(params,managed=True)
        if method=='quiescence.acquire':
            if params.get('purpose')=='service-stop' and getattr(self.intake,'SERVICE_STOP_VERSION',0)!=1:return {'acquired':False,'executed':False,'reason':'service-stop-unavailable'}
            value=self.intake.acquire(params,pending=len(self.schedules.pending)+len(self.observations.tasks))
            if not value['acquired']:
                self.awaiting_idle=True
                for task in self.schedules.pending|set(self.observations.tasks.values()):
                    task.add_done_callback(lambda _task:asyncio.create_task(self.maybe_idle()))
            return value
        if method=='quiescence.release':
            if params.get('purpose')=='service-stop' and getattr(self.intake,'SERVICE_STOP_VERSION',0)!=1:raise ValueError('Service-stop release unavailable in installed library')
            return self.intake.release(params)
        if method=='quiescence.inspect':return {'intakeClosed':bool(self.intake.fence),'fence':self.intake.fence,'activeRequests':self.intake.calls,'background':self.intake.background}
        readonly=self.intake.readonly(method,params)
        if self.intake.fence and not readonly and method!='start':raise ValueError('Owner intake is closed; no new operation was admitted')
        if not readonly:self.intake.calls+=1
        try:return await self._request(method,params)
        finally:
            if not readonly:
                self.intake.calls-=1
                await self.maybe_idle()
    async def _request(self,method,params):
        if method in {'exportTransferEvidence','stageTransferEvidence','activateTransferEvidence','readTransferEvidence'}:
            return getattr(self.transfer,{'exportTransferEvidence':'export','stageTransferEvidence':'stage','activateTransferEvidence':'activate','readTransferEvidence':'read'}[method])(params)
        if method=='initialize':return {'protocolVersion':1,'scheduledAdmission':'host','quiescence':{'version':1,'retentionHide':{'version':1},'managedFiles':{'version':1,'preservesCanonical':True},**({'serviceStop':{'version':1}} if getattr(self.intake,'SERVICE_STOP_VERSION',0)==1 else {})},'actions':list(self.schemas)}
        if method=='actions':return self.schemas
        if method=='start':self.observations_enabled=params.get('observations') is True;self.start();return {'started':True}
        if method=='observe':return await self.operations.observe(params['session'],params['nativeSessionId'],params['event'])
        if method=='interrupted':await self.operations.interrupted(params['session']);return {}
        if method=='questionAttention':return self.questions.attention(params['session'])
        if method=='questionDependency':return self.questions.dependency(params['session'],params['id'])
        if method=='questionDelivery':return self.questions.authorize_delivery(params['session'],params['args'])
        if method=='observationAdmission':return await self.observations.authorize(params['session'],params['args'])
        if method=='tick':await self.schedules.tick();return {}
        if method=='snapshot':
            sid=params['session'];topic=params['topic']
            if topic=='schedules':
                page=self.schedules.list(sid,{'limit':25});return {'schedules':{sid:page['items']},'schedulePage':{'nextCursor':page['nextCursor']}}
            if topic=='operations':return {'operations':{sid:await self.operations.dispatch('operations.list',{'sessionId':sid,'limit':25},'ui')}}
            if topic=='questions':
                page=self.questions.page(sid,{'limit':5});return {'questions':{sid:page['items']},'questionPage':{'nextCursor':page['nextCursor']}}
            if topic=='observations':
                page=self.observations.store.page('watch',sid,limit=5);return {'observations':{sid:page['items']},'observationPage':{'nextCursor':page['nextCursor']}}
            raise ValueError('Unknown topic')
        if method=='action':
            name=params['operation'];definition=self.schemas.get(name)
            if not definition:raise ValueError('Unknown action')
            args=params.get('args',{});sid=params['session']
            if args.get('sessionId',sid)!=sid:raise ValueError('Session argument does not match trusted context')
            args={**args,'sessionId':sid}
            try:Draft202012Validator(definition['schema']).validate(args)
            except ValidationError as error:
                if name.startswith('schedule.'):raise ScheduleRefused(error.message) from error
                raise
            origin=params.get('origin','ui')
            if origin not in {'ui','agent'}:raise ValueError('Untrusted action origin')
            if name.startswith('schedule.'):
                command=params.get('commandId')
                if not isinstance(command,str) or not 1<=len(command)<=200:raise ScheduleRefused('Stable commandId required')
                return await self.schedules.dispatch(name,args,origin,command)
            if name.startswith('operations.history.'):
                if self._historical is None:self._historical=HistoricalRecords(self.root/'legacy.sqlite3')
                return self._historical.page(sid,args) if name=='operations.history.list' else self._historical.read(sid,args['id'])
            if name.startswith('question.'):
                return await self.questions.dispatch(name,args,origin,params.get('clientId'),params.get('commandId'))
            if name.startswith('observation.'):
                return await self.observations.dispatch(name,args,origin,params.get('clientId'),params.get('commandId'))
            return await self.operations.dispatch(name,args,origin)
        raise ValueError('Unknown owner method')
    async def close(self):
        self.closed=True
        if self.tick_task:self.tick_task.cancel();await asyncio.gather(self.tick_task,return_exceptions=True)
        await self.observations.close();await self.schedules.close();await self.operations.close();self.questions.close();self.transfer.close()
        if self._historical:self._historical.close()
        self.intake.close()
        self.lease.release()

class Peer:
    def __init__(self,config):self.pending={};self.counter=0;self.tasks=set();self.closing=False;self.owner=Owner(config,self.host,self.notify)
    async def write(self,row):
        text=json.dumps(row,ensure_ascii=False,allow_nan=False)+'\n'
        if len(text.encode())>MAX_FRAME:raise ValueError('Owner frame exceeds4MB; narrow the requested page')
        sys.stdout.write(text);sys.stdout.flush()
    async def notify(self,method,params):await self.write({'jsonrpc':'2.0','method':method,'params':params})
    async def host(self,method,params):
        if self.closing:raise ValueError('Host connection closed; new work was not admitted')
        self.counter+=1;identity='host:'+str(self.counter);future=asyncio.get_running_loop().create_future();self.pending[identity]=future
        try:
            await self.write({'jsonrpc':'2.0','id':identity,'method':'host/'+method,'params':params});return await future
        finally:self.pending.pop(identity,None)
    async def handle(self,row):
        identity=row.get('id')
        if not row.get('method'):
            future=self.pending.get(identity)
            if future and not future.done():
                if row.get('error'):future.set_exception(ValueError(row['error']['message']))
                else:future.set_result(row.get('result'))
            return
        try:result=await self.owner.request(row['method'],row.get('params',{}));await self.write({'jsonrpc':'2.0','id':identity,'result':result})
        except Exception as exc:await self.write({'jsonrpc':'2.0','id':identity,'error':{'code':-32000,'message':str(exc)[:2000],**({'data':{'kind':'schedule_refused','executed':False}} if isinstance(exc,ScheduleRefused) else {})}})
    async def run(self):
        reader=asyncio.StreamReader(limit=MAX_FRAME+1);protocol=asyncio.StreamReaderProtocol(reader)
        transport,_=await asyncio.get_running_loop().connect_read_pipe(lambda:protocol,sys.stdin)
        try:
            while line:=await reader.readline():
                row=json.loads(line)
                if len(self.tasks)>=128:raise ValueError('Owner request capacity reached')
                task=asyncio.create_task(self.handle(row));self.tasks.add(task);task.add_done_callback(self.tasks.discard)
        finally:
            self.closing=True
            transport.close()
            for future in self.pending.values():
                if not future.done():future.set_exception(ValueError('Host connection lost; uncertain work is not replayed'))
            # Keep durable stores open until in-flight handlers record uncertain
            # deliveries. Cancellation must never race their final SQL writes.
            tasks=list(self.tasks)
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            await self.owner.close()

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);args=parser.parse_args()
    asyncio.run(Peer(json.loads(args.config.read_text())).run())
if __name__=='__main__':main()
