"""Schedule authority; native execution uses the host's common admission API."""
import asyncio
import copy
import json
import time
import uuid
from amplifier_scheduling import ScheduleStore,normalize,next_after,preview
from amplifier_scheduling.store import fingerprint
from .errors import schedule_validation

class Scheduler:
    def __init__(self,path,host,changed,clock=time.time):
        self.store=ScheduleStore(path);self.host=host;self.changed=changed;self.clock=clock
        self.pending=set();self.closed=False;self.error=None
    async def task(self,sid,config):
        if config.get('destination')=='new_task':return None
        task=(await self.host('nativeControl',{'session':sid,'operation':'task.get','args':{}})).get('task')
        if not task:raise ValueError('Save an explicit task before scheduling its continuation')
        return task
    def configuration(self,args):
        value={k:copy.deepcopy(args[k]) for k in ('prompt','kind','missedRunPolicy','notificationPolicy')}
        value.update(spec=normalize(args['spec']),destination=args.get('destination','same_task'))
        if value['destination']=='new_task':value.update(newTaskTitle=args.get('newTaskTitle') or 'Scheduled task',newTaskMaxTurns=args.get('newTaskMaxTurns',1))
        return value
    async def reviewed(self,sid,config,task):
        session=await self.host('inspectSession',{'session':sid})
        binding={'sessionId':sid,'taskId':task['id'] if task else None,'taskRevision':task['revision'] if task else None,
            'interruptionRevision':session['interruptionRevision'],'executionRevision':session['executionRevision'],
            'configurationHash':session['configurationHash'],**config}
        destination={'engineId':session.get('engineId'),'workspace':session.get('executionDirectory') or session.get('workingDirectory')}
        destination={key:value for key,value in destination.items() if isinstance(value,str) and len(value)<=8192}
        return {'previewHash':fingerprint(binding),'binding':binding,'destinationSummary':destination,'occurrences':preview(config['spec'],self.clock(),5),
            'daylightSaving':'Nonexistent local times are skipped; repeated local times run once at the first occurrence.',
            'missedRunPolicy':config['missedRunPolicy'],'notificationPolicy':config['notificationPolicy']}
    async def authorization(self,sid,args,origin,previous=None):
        if origin=='ui':return {'origin':'ui','at':self.clock()}
        if origin not in {'agent','voice','user'}:raise ValueError('Only an explicit user request can authorize scheduled work')
        source=await self.host('readUserMessage',{'session':sid,'id':args.get('sourceMessageId')})
        if not source or source.get('role')!='user' or source.get('inputOrigin') not in {'ui','user','voice'} or source.get('questionId') or source.get('scheduledRunId'):
            raise ValueError('Cite the actual user message requesting this schedule; question answers and scheduled inputs are not authorization')
        if previous and previous.get('status')=='needs_review' and (previous.get('authorization',{}).get('messageId')==source['id'] or source.get('createdAt',0)<=previous.get('authorization',{}).get('at',0)):
            raise ValueError('A new explicit user correction is required for review')
        return {'origin':origin,'messageId':source['id'],'inputOrigin':source['inputOrigin'],'messageCreatedAt':source.get('createdAt',0),
            'textDigest':fingerprint(source.get('text','')),'voiceId':source.get('voiceId'),'voiceItemId':source.get('voiceItemId'),'at':self.clock()}
    def list(self,sid,args):
        page=self.store.page(sid,limit=args.get('limit',25),after=args.get('cursor') or 0)
        return {**page,'incomingRuns':self.store.incoming(sid,limit=25),'items':[{**r,'runs':self.store.run_page(sid,r['id'],limit=10)['items']} for r in page['items']]}
    async def dispatch(self,action,args,origin,command):
        sid=args['sessionId']
        if action=='schedule.list':return self.list(sid,args)
        if action=='schedule.request':return {'receipt':self.store.command_receipt(sid,args['requestId']),'replayed':False}
        if action=='schedule.read':
            owner=sid
            try:schedule=self.store.get(sid,args['id'])
            except ValueError:
                linked=self.store.incoming(sid,args['id'],limit=1)
                if not linked:raise
                owner=linked[0]['sessionId'];schedule=self.store.get(owner,args['id'])
            page=self.store.run_page(owner,args['id'],limit=args.get('limit',25),before=args.get('cursor'))
            return {'schedule':schedule,'runs':page['items'],'nextCursor':page['nextCursor'],'readOnly':owner!=sid}
        if action in {'schedule.report','schedule.reconcile'}:return await self.run_command(action,args,origin,command)
        with schedule_validation():
            config=self.configuration(args) if action in {'schedule.preview','schedule.create','schedule.update'} else None
            previous=self.store.get(sid,args['id']) if args.get('id') else None
            if action=='schedule.resume':config=self.configuration(previous)
            task=await self.task(sid,config) if config else None
            review=await self.reviewed(sid,config,task) if config else None
            if action=='schedule.preview':return review
            provenance=await self.authorization(sid,args,origin,previous) if config else None
        def build(old):
            now=self.clock()
            if action=='schedule.create':
                if self.store.db.execute('SELECT COUNT(*) FROM schedules WHERE session_id=?',(sid,)).fetchone()[0]>=100:raise ValueError('This conversation already has 100 schedules')
                record={'id':str(uuid.uuid4()),'sessionId':sid,'createdAt':now}
            else:
                record=old
                if record['status']=='cancelled':raise ValueError('This schedule is cancelled; create a new one explicitly')
            if config:
                if task and task['status']!='active':raise ValueError('Resume the saved task explicitly before activating its schedule')
                if args['previewHash']!=review['previewHash']:raise ValueError('The task, stop intent, configuration or preview changed; preview again')
                if self.store.db.execute("SELECT 1 FROM schedule_runs WHERE schedule_id=? AND phase='unknown' LIMIT 1",(record['id'],)).fetchone():raise ValueError('Reconcile the uncertain run before resuming')
                due=next_after(config['spec'],now)
                if due is None:raise ValueError('Choose a future occurrence; past work is not replayed')
                record.update(review['binding'],status='active',nextDue=due,authorization=provenance,reviewReason=None)
            elif action=='schedule.pause':record['status']='paused'
            elif action=='schedule.cancel':record['status']='cancelled'
            else:raise ValueError('Unknown schedule action')
            record['updatedAt']=now;return record
        with schedule_validation():result=self.store.mutate(action,{**args,'authorizationOrigin':origin},command,build)
        await self.changed(sid);return result
    async def run_command(self,action,args,origin,command):
        sid=args['sessionId'];now=self.clock()
        with schedule_validation():
            run=self.store.execution_run(sid,args['runId'])
            provenance=await self.authorization(sid,args,origin) if action=='schedule.reconcile' else None
        def edit(row):
            if action=='schedule.report':
                if row['phase'] not in {'submitting','accepted','running'}:raise ValueError('Only an admitted current run can report evidence')
                if len(json.dumps(args.get('values')).encode())>64000:raise ValueError('Compared monitor values exceed64KiB')
                report={'outcome':args['outcome'],'detail':args['detail'],'source':'agent_report' if origin=='agent' else 'user_report','at':now}
                if 'values' in args:report.update(values=args['values'],valuesDigest=fingerprint(args['values']))
                row['report']=report
            else:
                if sid!=row['sessionId'] or row['phase']!='unknown':raise ValueError('Only the owning conversation can reconcile an uncertain run')
                if origin!='ui' and provenance.get('messageCreatedAt',0)<row['createdAt']:raise ValueError('Reconciliation requires a finding after the run')
                row.update(phase=args['resolution'],reconciliation={'evidence':args['evidence'],'provenance':provenance,'at':now},detail='Explicit reconciliation; input not replayed.')
            row['updatedAt']=now
        with schedule_validation():result=self.store.run_command(run['sessionId'],run['id'],args['expectedRevision'],command,fingerprint([action,args,origin]),edit)
        await self.changed(run['sessionId']);return result
    async def dependency(self,session,task,schedule):
        for key in ('interruptionRevision','executionRevision','configurationHash'):
            if session.get(key)!=schedule.get(key):return 'Saved '+key+' changed; review before continuing.'
        auth=schedule.get('authorization',{})
        if auth.get('messageId'):
            source=await self.host('readUserMessage',{'session':schedule['sessionId'],'id':auth['messageId']})
            if source is None or fingerprint(source.get('text',''))!=auth.get('textDigest'):return 'The authorizing message changed or is unavailable; review required.'
        if task:
            if (task['id'],task['revision'],task['status'])!=(schedule['taskId'],schedule['taskRevision'],'active'):return 'Saved task changed or is inactive; review required.'
            for identity in task.get('questionIds',[]):
                await self.host('questionDependency',{'session':schedule['sessionId'],'id':identity})
        return None
    async def tick(self):
        if not self.store.acquire(self.clock()):return
        for run in self.store.recovered:await self.decide(run)
        for schedule in self.store.due(self.clock(),limit=50):
            if len(self.pending)>=32:break
            sid=schedule['sessionId']
            try:
                session=await self.host('inspectSession',{'session':sid})
                if session['status']!='idle':continue
                task=await self.task(sid,schedule)
                reason=await self.dependency(session,task,schedule)
                current=self.store.get(sid,schedule['id'])
                if current['revision']!=schedule['revision'] or current['status']!='active':continue
                if reason:self.store.review(sid,schedule['id'],reason,self.clock());await self.changed(sid);continue
                run=self.store.claim(sid,schedule['id'],self.clock())
                if not run:continue
                if run['phase']=='skipped':await self.changed(sid);continue
                future=asyncio.create_task(self.execute(current,run,session,task));self.pending.add(future);future.add_done_callback(self.pending.discard)
            except Exception as exc:
                self.store.review(sid,schedule['id'],'Preparation failed before submission: '+str(exc)[:500],self.clock());await self.changed(sid)
    async def execute(self,schedule,run,session,task):
        sid=run['sessionId'];target=sid
        try:
            if schedule['destination']=='new_task':
                run=self.store.transition(sid,run['id'],['claimed'],'creating',self.clock(),creationCommandId=run['id']+':create')
                created=await self.host('createSession',{'commandId':run['id']+':create','idempotencyKey':run['id']+':create','sourceSession':sid,'expectedConfigurationHash':schedule['configurationHash'],'title':schedule['newTaskTitle']})
                if not created.get('creationConfirmed'):raise ValueError('Destination creation outcome was not confirmed')
                target=created['uri'];session=await self.host('inspectSession',{'session':target})
                run=self.store.transition(sid,run['id'],['creating'],'creating',self.clock(),destinationSessionId=target)
                task=(await self.host('nativeControl',{'session':target,'operation':'task.create','args':{'objective':schedule['prompt'],'maxTurns':schedule['newTaskMaxTurns'],'commandId':run['id']+':task','expectedRevision':0}})).get('task')
                if not task:raise ValueError('Destination task creation was not confirmed')
            current=self.store.get(sid,schedule['id'])
            owner=await self.host('inspectSession',{'session':sid})
            if not self.store.owns(self.clock()) or current['status']!='active' or current['revision']!=run['scheduleRevision'] or await self.dependency(owner,task if target==sid else None,schedule):
                run=self.store.transition(sid,run['id'],['claimed','creating'],'skipped',self.clock(),detail='Admission dependencies changed; input was not submitted.');await self.decide(run);return
            run=self.store.transition(sid,run['id'],['claimed','creating'],'submitting',self.clock(),taskId=task['id'],taskRevision=task['revision'])
            text=schedule['prompt']
            if schedule['kind']=='monitor':text+='\n\nScheduled monitor '+run['id']+'. Inspect schedule.read and record actual compared values with schedule.report. Label evidence accurately. An ending response does not complete the overall task.'
            reply=await self.host('submitScheduled',{'session':target,'input':{'commandId':run['inputId'],'text':text,'taskId':task['id'],'taskRevision':task['revision'],'kind':schedule['kind'],
                'expectedInterruptionRevision':session['interruptionRevision'],'expectedExecutionRevision':session['executionRevision'],'expectedConfigurationHash':session['configurationHash']}})
            if not reply.get('accepted'):
                run=self.store.transition(sid,run['id'],['submitting'],'skipped',self.clock(),detail=reply.get('reason','Admission rejected.'))
            else:
                admitted=await self.host('inspectSession',{'session':target})
                admitted_revision=admitted['executionRevision'] if target==sid and admitted['executionRevision']==session['executionRevision']+1 else None
                self.store.transition(sid,run['id'],['submitting'],'accepted',self.clock(),admittedExecutionRevision=admitted_revision);await self.changed(sid)
                while True:
                    result=await self.host('waitForTurn',{'session':target,'commandId':run['inputId']})
                    if not result.get('workContinues'):break
                    await asyncio.sleep(.25)
                state=result.get('state',result.get('status'));phase={'completed':'completed','failed':'failed','cancelled':'unknown','unknown':'unknown'}.get(state,'unknown')
                if phase=='completed' and admitted_revision is not None:
                    settled=await self.host('inspectSession',{'session':sid})
                    with self.store.transaction():
                        saved=self.store.get(sid,schedule['id'])
                        if saved['revision']==schedule['revision'] and saved['status']=='active' and settled['executionRevision']==admitted_revision and settled['interruptionRevision']==schedule['interruptionRevision'] and settled['configurationHash']==schedule['configurationHash']:
                            saved['executionRevision']=admitted_revision;self.store.put(saved)
                run=self.store.transition(sid,run['id'],['accepted','running'],phase,self.clock(),detail=result.get('error') or result.get('detail') or 'Host turn receipt: '+str(state),turnReceipt={k:v for k,v in result.items() if k!='text'})
            if run['phase'] in {'unknown','skipped'}:self.store.review(sid,schedule['id'],run.get('detail','Review required.'),self.clock())
            await self.decide(run)
        except BaseException as exc:
            run=self.store.transition(sid,run['id'],['claimed','creating','submitting','accepted','running'],'unknown',self.clock(),detail='Outcome uncertain; no replay: '+type(exc).__name__)
            self.store.review(sid,schedule['id'],run['detail'],self.clock());await self.decide(run)
            if isinstance(exc,asyncio.CancelledError):raise
    async def decide(self,run):
        with self.store.transaction():
            run=self.store.run(run['sessionId'],run['id'])
            if run.get('notificationDecision') is not None:return
            schedule=self.store.get(run['sessionId'],run['scheduleId']);report=run.get('report',{});changed=None;source='unreported'
            if 'valuesDigest' in report:
                row=self.store.db.execute("SELECT value FROM schedule_runs WHERE schedule_id=? AND id<>? AND phase='completed' AND json_extract(value,'$.report.valuesDigest') IS NOT NULL ORDER BY due DESC LIMIT 1",(run['scheduleId'],run['id'])).fetchone()
                previous=json.loads(row[0]) if row else None;changed=not previous or report['valuesDigest']!=previous['report']['valuesDigest'];source='compared_reported_values' if previous else 'initial_reported_values'
            elif report.get('outcome'):changed=report['outcome']=='changed';source=report['source']
            notify=run['phase'] in {'failed','unknown'} or schedule['notificationPolicy']=='always' or schedule['notificationPolicy']=='changes' and changed is True
            run['notificationDecision']={'notify':notify,'changed':changed,'source':source,'at':self.clock(),'policy':schedule['notificationPolicy']};self.store.put_run(run)
            if schedule.get('nextDue') is None and schedule['status']=='active':
                schedule.update(status='completed' if run['phase']=='completed' else 'needs_review',revision=schedule['revision']+1);self.store.put(schedule)
        await self.changed(run['sessionId'])
        if notify:await self.host('notifySchedule',{'session':run['sessionId'],'run':{'id':run['id'],'phase':run['phase'],'detail':report.get('detail') or run.get('detail'),'notificationDecision':run['notificationDecision']}})
    async def close(self):
        self.closed=True
        for task in self.pending:task.cancel()
        await asyncio.gather(*self.pending,return_exceptions=True);self.store.close()
