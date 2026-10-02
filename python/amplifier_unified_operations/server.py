"""Private stdio owner protocol; host authority is passed by authenticated callbacks."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
from filelock import FileLock
from jsonschema import Draft202012Validator
from .scheduler import Scheduler
from .operations import Operations
from . import schedules_schemas,operations_schemas

MAX_FRAME=4_000_000

def schema(fields,required=None):return {'type':'object','properties':fields,'required':list(fields) if required is None else required,'additionalProperties':False}
def string(maximum):return {'type':'string','maxLength':maximum}

def definitions():
    values={**schedules_schemas.definitions(schema,string),**operations_schemas.definitions(schema,string)}
    for name in ('schedule.list','schedule.read'):
        values[name][1]['properties'].update(limit={'type':'integer','minimum':1,'maximum':100},cursor={'type':['number','null'],'minimum':0})
    for _,definition in values.values():definition['properties']['sessionId']['maxLength']=8192
    values['operations.list'][1]['properties']['cursor']={'type':'string','maxLength':1024}
    return {name:{'description':row[0],'schema':row[1]} for name,row in values.items()}

class Owner:
    def __init__(self,config,host,notify):
        root=Path(config['dataDir']).expanduser().resolve();root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.lease=FileLock(str(root/'owner.lock'));self.lease.acquire(timeout=0)
        self.host=host;self.notify=notify;self.schemas=definitions();self.revisions={};self.tick_task=None;self.closed=False
        self.schedules=Scheduler(root/'schedules.sqlite3',host,self.changed)
        self.operations=Operations(root/'operations.sqlite3',host,self.changed)
    async def changed(self,sid):
        # This map is presentation-only and capped; authoritative revision stays on disk.
        self.revisions.pop(sid,None);self.revisions[sid]=0
        while len(self.revisions)>128:self.revisions.pop(next(iter(self.revisions)))
        await self.notify('owner/changed',{'session':sid})
    def start(self):
        if self.tick_task is None:self.tick_task=asyncio.create_task(self.loop())
    async def loop(self):
        while not self.closed:
            try:await self.schedules.tick()
            except asyncio.CancelledError:raise
            except Exception as exc:self.schedules.error=type(exc).__name__
            await asyncio.sleep(2)
    async def request(self,method,params):
        if method=='initialize':return {'protocolVersion':1,'scheduledAdmission':'host','actions':list(self.schemas)}
        if method=='actions':return self.schemas
        if method=='start':self.start();return {'started':True}
        if method=='observe':return await self.operations.observe(params['session'],params['nativeSessionId'],params['event'])
        if method=='interrupted':await self.operations.interrupted(params['session']);return {}
        if method=='tick':await self.schedules.tick();return {}
        if method=='snapshot':
            sid=params['session'];topic=params['topic']
            if topic=='schedules':
                page=self.schedules.list(sid,{'limit':25});return {'schedules':{sid:page['items']},'schedulePage':{'nextCursor':page['nextCursor']}}
            if topic=='operations':return {'operations':{sid:await self.operations.dispatch('operations.list',{'sessionId':sid,'limit':25},'ui')}}
            raise ValueError('Unknown topic')
        if method=='action':
            name=params['operation'];definition=self.schemas.get(name)
            if not definition:raise ValueError('Unknown action')
            args=params.get('args',{});sid=params['session']
            if args.get('sessionId',sid)!=sid:raise ValueError('Session argument does not match trusted context')
            args={**args,'sessionId':sid};Draft202012Validator(definition['schema']).validate(args)
            origin=params.get('origin','ui')
            if origin not in {'ui','agent'}:raise ValueError('Untrusted action origin')
            if name.startswith('schedule.'):
                command=params.get('commandId')
                if not isinstance(command,str) or not 1<=len(command)<=200:raise ValueError('Stable commandId required')
                return await self.schedules.dispatch(name,args,origin,command)
            return await self.operations.dispatch(name,args,origin)
        raise ValueError('Unknown owner method')
    async def close(self):
        self.closed=True
        if self.tick_task:self.tick_task.cancel();await asyncio.gather(self.tick_task,return_exceptions=True)
        await self.schedules.close();await self.operations.close();self.lease.release()

class Peer:
    def __init__(self,config):self.pending={};self.counter=0;self.tasks=set();self.owner=Owner(config,self.host,self.notify)
    async def write(self,row):
        text=json.dumps(row,ensure_ascii=False,allow_nan=False)+'\n'
        if len(text.encode())>MAX_FRAME:raise ValueError('Owner frame exceeds4MB; narrow the requested page')
        sys.stdout.write(text);sys.stdout.flush()
    async def notify(self,method,params):await self.write({'jsonrpc':'2.0','method':method,'params':params})
    async def host(self,method,params):
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
        except Exception as exc:await self.write({'jsonrpc':'2.0','id':identity,'error':{'code':-32000,'message':str(exc)[:2000]}})
    async def run(self):
        reader=asyncio.StreamReader(limit=MAX_FRAME+1);protocol=asyncio.StreamReaderProtocol(reader)
        transport,_=await asyncio.get_running_loop().connect_read_pipe(lambda:protocol,sys.stdin)
        try:
            while line:=await reader.readline():
                row=json.loads(line)
                if len(self.tasks)>=128:raise ValueError('Owner request capacity reached')
                task=asyncio.create_task(self.handle(row));self.tasks.add(task);task.add_done_callback(self.tasks.discard)
        finally:
            transport.close()
            for future in self.pending.values():
                if not future.done():future.set_exception(ValueError('Host connection lost; uncertain work is not replayed'))
            await self.owner.close()
            for task in self.tasks:task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);args=parser.parse_args()
    asyncio.run(Peer(json.loads(args.config.read_text())).run())
if __name__=='__main__':main()
