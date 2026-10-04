"""Session-scoped operation evidence and owned process controls."""
import asyncio
import base64
import json
from amplifier_operations import OperationJournal
from amplifier_operations.requests import OperationRequests
from amplifier_operations.journal import ACTIVE

class Operations:
    def __init__(self,path,host,changed):
        self.journal=OperationJournal(path,require_requests=True);self.journal.recover();self.requests=OperationRequests(self.journal)
        self.host=host;self.changed=changed;self.event=asyncio.Event();self.closed=False
    async def notify(self,sid):
        previous=self.event;self.event=asyncio.Event();previous.set();await self.changed(sid)
    async def observe(self,sid,native_id,event):
        if (event.get('source'),event.get('kind')) not in {('tool-bash','process'),('computation','kernel'),('computation','kernel-cell')}:raise ValueError('Unregistered operation producer')
        if self.closed:raise ValueError('Operation owner is closing')
        for identity in event.get('metadata',{}).get('questionIds',[]) if event.get('phase')=='started' else []:
            await self.host('questionDependency',{'session':sid,'id':identity})
        value,changed=self.journal.ingest(sid,native_id,event)
        await self.notify(sid);return {'id':value['id'],'revision':value['revision'],'duplicate':not changed}
    async def external(self,sid,identity=None,limit=50,cursor=None):
        values=await self.host('operationSources',{'session':sid,'id':identity,'limit':limit,'cursor':cursor})
        if not isinstance(values,list) or len(values)>100:raise ValueError('Invalid external operation page')
        for value in values:
            if value.get('sessionId')!=sid or not isinstance(value.get('id'),str) or ':' not in value['id']:raise ValueError('External operation owner mismatch')
        return values
    async def status(self,sid,identity):
        try:value=self.journal.status(sid,identity)
        except ValueError:
            values=await self.external(sid,identity,1)
            value=next((r for r in values if r['id']==identity),None)
            if value is None:raise ValueError('Operation not found in this conversation')
        return {**value,'revision':str(value['revision'])}
    async def read(self,args):
        sid=args['sessionId'];value=await self.status(sid,args['id'])
        if value['source'] not in {'process','kernel','kernel-cell'}:return {**value,'chunks':[],'nextCursor':args.get('cursor',0)}
        result=self.journal.read(sid,args['id'],args.get('cursor',0),args.get('maxBytes',64000));result['revision']=str(result['revision']);return result
    async def dispatch(self,action,args,origin):
        sid=args['sessionId']
        if action=='operations.list':
            limit=args.get('limit',50)
            cursor=json.loads(base64.urlsafe_b64decode(args['cursor'])) if args.get('cursor') else None
            if cursor is not None and (not isinstance(cursor,list) or len(cursor)!=2):raise ValueError('Invalid operation cursor')
            values=self.journal.page(sid,limit=limit+1,cursor=cursor)+await self.external(sid,limit=min(limit+1,100),cursor=cursor)
            values=[r for r in values if cursor is None or (r.get('createdAt') or 0,r['id'])<tuple(cursor)]
            values.sort(key=lambda r:(r.get('createdAt') or 0,r['id']),reverse=True)
            selected=values[:limit];next_cursor=base64.urlsafe_b64encode(json.dumps([selected[-1].get('createdAt') or 0,selected[-1]['id']]).encode()).decode() if len(values)>limit else None
            return {'operations':selected,'nextCursor':next_cursor,'requests':self.requests.list(sid),'capabilities':{'durableProcessOutput':True,'ptyRequiresHostOptIn':True,'nativeWindows':False}}
        if action=='operations.request':return self.requests.read(sid,args['requestId'])
        if action in {'operations.submit','operations.write'}:return await self.mutate(action,args,origin)
        if action=='operations.cancel':return await self.cancel(sid,args['id'],origin)
        if action=='operations.status':return await self.status(sid,args['id'])
        if action=='operations.wait':
            deadline=asyncio.get_running_loop().time()+args.get('waitMs',1000)/1000
            while True:
                event=self.event;result=await self.read(args)
                if str(result['revision'])!=args.get('afterRevision') or self.closed:return result
                remaining=deadline-asyncio.get_running_loop().time()
                if remaining<=0:return result
                try:await asyncio.wait_for(event.wait(),remaining)
                except TimeoutError:return result
        return await self.read(args)
    async def mutate(self,action,args,origin):
        sid=args['sessionId'];prior=self.requests.existing(sid,action,args,origin)
        if prior is not None:return prior
        if not args['requestId'].strip():raise ValueError('A stable requestId is required')
        if action=='operations.submit':
            for identity in args.get('questionIds',[]):await self.host('questionDependency',{'session':sid,'id':identity})
            payload={'command':args['command'],'questionIds':args.get('questionIds',[]),**{k:args[k] for k in ('timeout','pty') if k in args}};method='nativeControl'
        else:
            value=await self.status(sid,args['id'])
            if value['source']!='process' or not value.get('controlAvailable') or value['state'] not in ACTIVE:raise ValueError('Original process is unavailable; input not sent')
            payload={'processId':value['sourceId'],'runtimeSessionId':value['runtimeSessionId'],'ownerId':value['ownerId'],'operationId':args['id'],'stdin':args.get('stdin',''),'closeStdin':args.get('closeStdin',False)};method='nativeControlExisting'
        receipt,fresh=self.requests.begin(sid,action,args,origin)
        if not fresh:return receipt
        await self.notify(sid)
        try:
            response=await self.host(method,{'session':sid,'operation':action,'args':{**payload,'actor':origin}})
            result=response.get('result',response)
            if result.get('success') is False:return self.requests.finish(receipt,'rejected',error=result.get('error',{}))
            output=result.get('output',{})
            if result.get('success') is not True or not isinstance(output,dict) or not output.get('process_id'):raise ValueError('No confirmed native process admission evidence')
            return self.requests.finish(receipt,'accepted',operationId=output['process_id'],callId=response.get('callId'),admission={k:output[k] for k in ('process_id','owner_id','state','returncode','pty','stdin_closed','eof_semantics') if k in output})
        except BaseException:
            self.requests.finish(receipt,'outcome_unknown',message='Admission was not confirmed; no replay. Inspect original operation evidence.');raise
        finally:await self.notify(sid)
    async def cancel(self,sid,identity,origin):
        value=await self.status(sid,identity)
        if value['state'] not in ACTIVE:return value
        if value['source']=='kernel-cell':
            metadata=value['metadata'];payload={'kernelId':metadata['kernelId'],'generation':metadata['generation'],'cellId':identity};operation='kernels.interrupt'
        elif value['source']=='process':
            payload={'processId':value['sourceId'],'runtimeSessionId':value['runtimeSessionId'],'ownerId':value['ownerId'],'operationId':identity,'actor':origin};operation='operations.cancel'
        else:raise ValueError('This external receipt must be controlled through its owning capability')
        response=await self.host('nativeControlExisting',{'session':sid,'operation':operation,'args':payload})
        result=response.get('result',response)
        if result.get('success') is False:raise ValueError(result.get('error',{}).get('message','Cancellation denied'))
        return {**await self.status(sid,identity),'request':response}
    async def interrupted(self,sid):
        self.journal.recover(sid,'Owning runtime ended; outcome unobserved and no replay.');await self.notify(sid)
    async def close(self):
        self.closed=True;self.event.set();self.journal.close()
