"""Quiet watches over explicit MCP and host contracts; no app snapshots or native paths."""
import asyncio
import copy
import time
import uuid
from .observation_store import Store,digest,encoded
from .observations_schemas import CONFIG

class Observations:
    def __init__(self,path,host,changed,clock=time.time):
        self.store=Store(path);self.host=host;self.changed=changed;self.clock=clock;self.lock=asyncio.Lock();self.tasks={};self.closed=False
    async def call(self,method,sid,**args):return await self.host(method,{'session':sid,**args})
    async def state(self,sid):
        session=await self.call('inspectSession',sid);source=await self.call('readTaskState',sid)
        task=source.get('task')
        if not source.get('available') or not task or not task.get('id') or type(task.get('revision')) is not int:raise ValueError('Save an explicit native task before watching; no runtime was prepared')
        return session,task
    async def configuration(self,sid,args):
        config={key:copy.deepcopy(args[key]) for key in CONFIG}
        if len(encoded(config).encode())>32000:raise ValueError('Observation configuration exceeds32KiB')
        if args.get('presentationRequest') is not None:raise ValueError('Automatic presentation requires an explicit client grant; use the retained result reference until that client capability is connected')
        descriptor=self.store.get('observer',config['observerId'])
        await self.call('qualifiedObservation',sid,operation='check',args={'descriptor':descriptor,'configuration':config})
        if descriptor['requestArgument'] in config['args']:raise ValueError('Observation identity arguments are host-owned')
        return config
    async def review(self,sid,config):
        session,task=await self.state(sid)
        if task.get('status')!='active' or session.get('relocationFence') or session.get('configurationBusy'):raise ValueError('Task or execution context is not available for a quiet watch')
        binding={**config,'sessionId':sid,'taskId':task['id'],'taskRevision':task['revision'],'nativeSessionId':session.get('nativeSessionId'),'executionRevision':session['executionRevision'],'interruptionRevision':session['interruptionRevision'],'configurationHash':session['configurationHash']}
        return {'previewHash':digest(binding),'binding':binding,'handoff':'one finite typed explanation; no product tools','expiresInSeconds':config['durationSeconds']}
    async def authority(self,sid,args,origin,client,previous=None):
        if origin=='ui':return {'origin':'ui','clientId':client,'at':self.clock()}
        row=await self.call('readUserMessage',sid,id=args.get('sourceMessageId'))
        if not row or row.get('role')!='user' or row.get('inputOrigin') not in {'ui','user','voice'} or row.get('questionId') or row.get('scheduledRunId') or row.get('provenance',{}).get('source')!='host-admission':raise ValueError('Cite the actual host-admitted human request; imported and generated inputs cannot authorize watches')
        if previous and previous['authorization'].get('messageId')==row['id']:raise ValueError('Resume needs renewed human authority')
        return {'origin':origin,'messageId':row['id'],'textDigest':digest(row['text']),'at':self.clock()}
    async def reason(self,watch):
        try:
            session,task=await self.state(watch['sessionId'])
            if session.get('relocationFence') or session.get('configurationBusy'):return 'Execution configuration is changing'
            for field in ('executionRevision','interruptionRevision','configurationHash','nativeSessionId'):
                if session.get(field)!=watch.get(field):return 'The user stopped or changed the execution context'
            if (task['id'],task['revision'],task.get('status'))!=(watch['taskId'],watch['taskRevision'],'active'):return 'Persisted task authority changed'
            for identity in task.get('questionIds',[]):await self.call('questionDependency',watch['sessionId'],id=identity)
            authority=watch['authorization']
            if authority.get('messageId'):
                row=await self.call('readUserMessage',watch['sessionId'],id=authority['messageId'])
                if digest(row['text'])!=authority['textDigest']:return 'The authorizing human request changed'
            await self.call('qualifiedObservation',watch['sessionId'],operation='check',args={'descriptor':self.store.get('observer',watch['observerId'])})
        except Exception:return 'Saved task, question, user or qualified observer authority is unavailable; review without replay'
        return None
    async def reviewed_again(self,watch,reason):
        current=self.store.get('watch',watch['id'])
        with self.store.db:
            if current['status'] not in {'cancelled','paused','needs_review'}:
                current.update(status='needs_review',reason=reason,revision=current['revision']+1,updatedAt=self.clock());self.store.put('watch',current)
            for item in self.store.page('outbox',watch=current['id'])['items']:
                if item['phase'] in {'pending','waiting_worker'}:item.update(phase='suppressed');self.store.put('outbox',item)
        await self.changed(current['sessionId'])
    async def dispatch(self,operation,args,origin,client,command):
        sid=args['sessionId'];await self.call('inspectSession',sid)
        if operation=='observation.observers':
            page=self.store.page('observer',limit=args.get('limit',25),cursor=args.get('cursor'));items=[]
            for row in page['items']:
                try:await self.call('qualifiedObservation',sid,operation='check',args={'descriptor':row});reason=None
                except Exception:reason='Qualified source or connection is not ready'
                items.append({**row,'eligible':reason is None,'reason':reason})
            return {'observers':items,'nextCursor':page['nextCursor']}
        if operation=='observation.list':
            page=self.store.page('watch',sid,limit=args.get('limit',25),cursor=args.get('cursor'));return {'watches':page['items'],'nextCursor':page['nextCursor']}
        if operation in {'observation.read','observation.report'}:
            watch=self.store.get('watch',args['id'],sid);runs=self.store.page('run',sid,watch=watch['id'],limit=args.get('limit',25),cursor=args.get('cursor'));handoffs=self.store.page('outbox',sid,watch=watch['id'])
            return {'watch':watch,'runs':runs['items'],'nextCursor':runs['nextCursor'],'handoffs':handoffs['items']}
        if operation=='observation.request':
            row=self.store.db.execute('SELECT watch FROM commands WHERE session=? AND id=?',(sid,'observation.create:'+args['requestId'])).fetchone();return {'watch':self.store.get('watch',row['watch'],sid) if row else None}
        if operation=='observation.preview':return await self.review(sid,await self.configuration(sid,args))
        if operation=='observation.qualify':
            if origin!='ui':raise ValueError('Only an explicit operator action can qualify source code')
            descriptor=await self.call('qualifiedObservation',sid,operation='qualify',args={'descriptor':args['descriptor']});descriptor['qualifiedAt']=self.clock()
            with self.store.db:self.store.put('observer',descriptor)
            await self.changed(sid);return {'observer':descriptor}
        request=args.get('requestId')
        if not isinstance(request,str) or not 1<=len(request)<=200:raise ValueError('Stable bounded watch request identity required')
        key=operation+':'+request;intent=digest([{k:v for k,v in args.items() if k!='previewHash'},origin,client])
        async with self.lock:
            previous_result=self.store.command(sid,key,intent)
            if previous_result:return {'watch':previous_result,'duplicate':True}
            previous=None if operation=='observation.create' else self.store.get('watch',args['id'],sid)
            if previous and previous['revision']!=args['expectedRevision']:raise ValueError('Observation revision changed')
            if operation in {'observation.create','observation.resume'}:
                if previous and (previous.get('terminal') or previous['status'] not in {'paused','needs_review'}):raise ValueError('Only a nonterminal paused watch can be reviewed and resumed')
                if previous and self.store.db.execute('SELECT 1 FROM terminals WHERE watch=?',(previous['id'],)).fetchone():raise ValueError('A prior handoff cannot be rearmed')
                config=await self.configuration(sid,args);review=await self.review(sid,config)
                if review['previewHash']!=args['previewHash']:raise ValueError('Observation preview changed; review again')
                authority=await self.authority(sid,args,origin,client,previous)
                if self.store.count_active(sid)>=100:raise ValueError('This conversation has100 active watches; pause one before adding work')
                watch=copy.deepcopy(previous) if previous else {'id':str(uuid.uuid4()),'requestId':request,'createdAt':self.clock(),'sequence':0}
                watch.update(review['binding'],status='active',authorization=authority,cursor=None,nextDue=self.clock(),expiresAt=self.clock()+config['durationSeconds'])
            elif operation in {'observation.pause','observation.cancel'}:
                watch=copy.deepcopy(previous);watch['status']='paused' if operation.endswith('pause') else 'cancelled'
            else:raise ValueError('Unadvertised watch action')
            watch.update(revision=(previous['revision'] if previous else 0)+1,updatedAt=self.clock())
            with self.store.db:
                self.store.put('watch',watch);self.store.db.execute('INSERT INTO commands VALUES(?,?,?,?)',(sid,key,intent,watch['id']))
                if watch['status'] in {'paused','cancelled'}:
                    for item in self.store.page('outbox',sid,watch=watch['id'])['items']:
                        if item['phase'] in {'pending','waiting_worker'}:item.update(phase='suppressed');self.store.put('outbox',item)
            await self.changed(sid);return {'watch':watch,'duplicate':False}
    def spawn(self,key,job):
        if self.closed or key in self.tasks or len(self.tasks)>=4:return
        async def run():
            try:await job()
            except asyncio.CancelledError:raise
            except Exception:pass  # Durable owner states carry safe uncertainty; no exception-text broadcast.
            finally:self.tasks.pop(key,None)
        self.tasks[key]=asyncio.create_task(run())
    async def tick(self):
        # Interleave indexed queues so a busy source cannot starve saved results.
        watches=self.store.due(self.clock());items=self.store.pending(self.clock())
        for index in range(max(len(watches),len(items))):
            if index<len(items):
                item=items[index];self.spawn(item['watchId'],lambda item=item:self.handoff(item))
            if index<len(watches):
                watch=watches[index];self.spawn(watch['id'],lambda watch=watch:self.read(watch))
    async def drain(self):
        while self.tasks:await asyncio.gather(*list(self.tasks.values()),return_exceptions=True)
    async def read(self,watch):
        reason=await self.reason(watch)
        if reason:return await self.reviewed_again(watch,reason)
        now=self.clock();current=self.store.get('watch',watch['id'])
        if current['status']!='active' or current['revision']!=watch['revision']:return
        if now>=watch['expiresAt']:
            with self.store.db:self.store.terminal(current,{'status':'expired','summary':'Watch ended; no product outcome or cancellation is inferred','evidence':[]},now)
            await self.changed(watch['sessionId']);return
        if current['nextDue']>now:return
        run={'id':'observation:'+watch['id']+':'+str(watch['sequence']),'sessionId':watch['sessionId'],'watchId':watch['id'],'watchRevision':watch['revision'],'phase':'reading','createdAt':now}
        with self.store.db:
            current.update(sequence=current['sequence']+1,nextDue=min(now+current['intervalSeconds'],current['expiresAt']));self.store.put('watch',current);self.store.put('run',run)
        try:
            read_watch={key:watch[key] for key in ('id','revision','sessionId','observerId','target','scope','sourceId','args','readTimeoutSeconds','cursor')}
            outcome=await self.call('qualifiedObservation',watch['sessionId'],operation='read',args={'descriptor':self.store.get('observer',watch['observerId']),'watch':read_watch,'occurrenceId':run['id']})
            if len(encoded(outcome).encode())>32000 or outcome.get('status') not in {'pending','actionable','observation_failed'}:raise ValueError('Unqualified observer result')
        except asyncio.CancelledError:
            run['phase']='abandoned_read'
            with self.store.db:self.store.put('run',run)
            raise
        except Exception:outcome={'contract':'amplifier.observation.v1','status':'observation_failed','target':watch['target'],'source':{'id':watch['sourceId'],'revision':None},'semanticKey':None,'observedAt':self.clock(),'summary':'Qualified read could not establish recorded state; work was not replayed','evidence':[]}
        reason=await self.reason(watch);current=self.store.get('watch',watch['id']);now=self.clock()
        changed=current['status']!='active' or current['revision']!=watch['revision']
        with self.store.db:
            run.update(phase='discarded' if reason or changed or now>=watch['expiresAt'] else outcome['status'])
            if run['phase']!='discarded':
                run['result']=outcome;current.update(lastResult=outcome,cursor=outcome.get('cursor'),updatedAt=now);self.store.put('watch',current)
                if outcome['status']!='pending':self.store.terminal(current,outcome,now)
            elif not reason and not changed and now>=watch['expiresAt']:self.store.terminal(current,{'status':'expired','summary':'Watch expired before read returned; no product outcome is inferred','evidence':[]},now)
            self.store.put('run',run);self.store.prune(watch['id'])
        if reason:await self.reviewed_again(watch,reason)
        elif outcome['status']!='pending' or run['phase']=='discarded':await self.changed(watch['sessionId'])
    @staticmethod
    def input(watch,item):return {'commandId':item['inputId'],'watchId':watch['id'],'taskId':watch['taskId'],'taskRevision':watch['taskRevision'],'outcome':item['outcome'],'expectedExecutionRevision':watch['executionRevision'],'expectedInterruptionRevision':watch['interruptionRevision'],'expectedConfigurationHash':watch['configurationHash']}
    async def authorize(self,sid,args):
        item=self.store.get('outbox',args.get('inputId'),sid);watch=self.store.get('watch',item['watchId'],sid)
        expected={k:v for k,v in self.input(watch,item).items() if not k.startswith('expected') and k!='commandId'};expected['inputId']=item['inputId']
        if args!=expected or item['phase'] not in {'submitting','accepted'} or watch['status']!='ended':return {'admitted':False,'reason':'Exact saved watch result is no longer authorized'}
        reason=await self.reason(watch);return {'admitted':reason is None,'reason':reason}
    async def handoff(self,item):
        watch=self.store.get('watch',item['watchId']);sid=watch['sessionId']
        if item['phase']=='accepted':
            try:receipt=await self.call('waitForTurn',sid,commandId=item['inputId'],waitMs=1000)
            except Exception:self.store.phase(item,'unknown',detail='Host receipt is unavailable; admitted work will not replay');await self.changed(sid);return
            if receipt.get('workContinues'):self.store.phase(item,'accepted',nextDue=self.clock()+2);return
            if receipt.get('executed') is False and receipt.get('retryEligibility')=='waiting_worker':
                self.store.phase(item,'waiting_worker',nextDue=self.clock()+15,detail='Host confirmed no execution; exact saved input remains eligible for an existing worker');await self.changed(sid);return
            phase='completed' if receipt.get('status')=='completed' else 'unknown' if receipt.get('status')=='unknown' else 'failed'
            evidence={key:receipt[key] for key in ('generation_id','status','completed','workContinues') if key in receipt}
            if receipt.get('detail'):evidence['detail']=str(receipt['detail'])[:2000]
            self.store.phase(item,phase,receipt=evidence);await self.changed(sid);return
        if watch['status']!='ended':self.store.phase(item,'suppressed');return
        reason=await self.reason(watch)
        if reason:return await self.reviewed_again(watch,reason)
        session=await self.call('inspectSession',sid)
        if session.get('activeTurnId') or session.get('status') not in {'idle','ready'}:self.store.phase(item,item['phase'],nextDue=self.clock()+2);return
        # Explanation may use only a currently resident runtime; passive reads
        # never acquire a provider merely because a watch became terminal.
        try:await self.call('nativeControlExisting',sid,operation='task.get',args={})
        except Exception:self.store.phase(item,'waiting_worker',nextDue=self.clock()+15,detail='Result retained until this conversation has an existing runtime');return
        item=self.store.phase(item,'submitting')
        try:
            result=await self.call('submitObservation',sid,input=self.input(watch,item))
            if result.get('accepted') is True:self.store.phase(item,'accepted',receipt={'accepted':True,'turnId':result.get('turnId',item['inputId'])})
            elif result.get('accepted') is False and result.get('executed') is False and result.get('reason') in {'waiting_worker','busy'}:self.store.phase(item,'waiting_worker',nextDue=self.clock()+15,detail='Host confirmed no admission; exact saved result is retained')
            elif result.get('accepted') is False and result.get('executed') is False:self.store.phase(item,'suppressed',receipt=result)
            else:raise ValueError('No conclusive admission receipt')
        except BaseException as error:
            self.store.phase(item,'unknown',detail='Admission outcome is uncertain; the saved result will not replay')
            if isinstance(error,asyncio.CancelledError):raise
        await self.changed(sid)
    async def close(self):
        self.closed=True
        for task in list(self.tasks.values()):task.cancel()
        await asyncio.gather(*list(self.tasks.values()),return_exceptions=True)
        self.store.close()
