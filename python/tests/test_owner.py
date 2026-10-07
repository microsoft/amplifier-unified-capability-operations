import asyncio
from datetime import datetime,timezone
import json
import pytest
from amplifier_unified_operations.server import Owner

SID='ahp-session:/fixture'
class Host:
    def __init__(self):
        self.calls=[];self.session={'uri':SID,'status':'idle','interruptionRevision':0,'executionRevision':0,'configurationHash':'original'}
        self.task={'id':'task','revision':1,'status':'active','questionIds':[]};self.outcome={'status':'completed','completed':True};self.raise_submit=False
        self.source={'id':'user1','role':'user','text':'Please schedule','createdAt':90,'inputOrigin':'ui'};self.block=asyncio.Event();self.block.set()
    async def __call__(self,method,args):
        self.calls.append((method,args))
        if method=='inspectSession':return self.session.copy()
        if method=='readUserMessage':return self.source
        if method=='nativeControl':
            if args['operation']=='task.get':return {'task':self.task}
            if args['operation']=='task.create':return {'task':{'id':'new-task','revision':1,'status':'active'}}
            if args['operation']=='operations.submit':
                if self.raise_submit:raise RuntimeError('lost reply')
                return {'success':True,'output':{'process_id':'p1','owner_id':'mount1','state':'running'}}
        if method=='nativeControlExisting':raise ValueError('Owner retired; no restart')
        if method=='createSession':return {'uri':'ahp-session:/destination','creationConfirmed':True}
        if method=='submitScheduled':
            if self.raise_submit:raise RuntimeError('lost admission reply')
            self.session['executionRevision']+=1
            return {'accepted':True}
        if method=='waitForTurn':await self.block.wait();return self.outcome
        if method=='operationSources':return []
        if method=='questionDependency':raise ValueError('Question unanswered')
        if method=='notifySchedule':return {}
        raise ValueError(method)

@pytest.fixture
async def owned(tmp_path):
    host=Host();events=[]
    async def notify(method,params):events.append((method,params))
    owner=Owner({'dataDir':str(tmp_path)},host,notify);now=[100];owner.schedules.clock=lambda:now[0]
    yield owner,host,now,events
    await owner.close()

def schedule_args(**extra):
    return {'prompt':'Check status','kind':'monitor','spec':{'kind':'interval','timezone':'UTC','startAt':datetime.fromtimestamp(110,timezone.utc).isoformat(),'intervalSeconds':60},'missedRunPolicy':'latest','notificationPolicy':'changes',**extra}
async def action(owner,name,args,command='command',origin='ui'):
    return await owner.request('action',{'session':SID,'operation':name,'args':args,'commandId':command,'origin':origin})
async def create(owner,**extra):
    args=schedule_args(**extra);preview=await action(owner,'schedule.preview',args)
    result=await action(owner,'schedule.create',{**args,'expectedRevision':0,'previewHash':preview['previewHash']})
    return result['schedule']
async def settle(owner):
    await asyncio.gather(*list(owner.schedules.pending))

async def test_idle_reads_never_mount_and_topic_is_scoped(owned):
    owner,host,now,events=owned
    assert (await owner.request('snapshot',{'session':SID,'topic':'schedules'}))['schedules']=={SID:[]}
    assert (await owner.request('snapshot',{'session':SID,'topic':'operations'}))['operations'][SID]['operations']==[]
    assert not any(m=='nativeControl' for m,_ in host.calls)
    with pytest.raises(ValueError,match='trusted'):await action(owner,'schedule.list',{'sessionId':'ahp-session:/other'})

async def test_lost_schedule_response_recovers_exact_committed_result_without_new_work(owned):
    owner,host,now,_=owned
    async def dropped(_):raise RuntimeError('Lost response after durable schedule write')
    owner.schedules.changed=dropped
    with pytest.raises(RuntimeError,match='Lost response'):await create(owner)
    calls=len(host.calls)
    receipt=await action(owner,'schedule.request',{'requestId':'command'},command='inspect')
    assert receipt['replayed'] is False and receipt['receipt']['status']=='committed'
    original=receipt['receipt']['result']['schedule'];assert original['revision']==1
    assert len(host.calls)==calls and len(owner.schedules.store.list(SID))==1
    async def observed(_):pass
    owner.schedules.changed=observed
    await action(owner,'schedule.pause',{'id':original['id'],'expectedRevision':1},command='pause')
    assert (await action(owner,'schedule.request',{'requestId':'command'}))['receipt']['result']['schedule']==original
    assert (await action(owner,'schedule.request',{'requestId':'never-submitted'}))['receipt'] is None
    other=await owner.request('action',{'session':'ahp-session:/other','operation':'schedule.request','args':{'requestId':'command'},'commandId':'inspect','origin':'ui'})
    assert other['receipt'] is None

async def test_schedule_refusal_is_distinct_from_errors_after_commit(owned):
    from amplifier_unified_operations.errors import ScheduleRefused
    owner,host,now,_=owned;args=schedule_args()
    with pytest.raises(ScheduleRefused,match='preview'):
        await action(owner,'schedule.create',{**args,'expectedRevision':0,'previewHash':'stale'},command='stale')
    assert (await action(owner,'schedule.request',{'requestId':'stale'}))['receipt'] is None
    with pytest.raises(ScheduleRefused):await action(owner,'schedule.create',{},command='invalid')
    async def dropped(_):raise ValueError('Response could not be published')
    owner.schedules.changed=dropped
    with pytest.raises(ValueError) as error:await create(owner)
    assert not isinstance(error.value,ScheduleRefused)
    assert (await action(owner,'schedule.request',{'requestId':'command'}))['receipt']['status']=='committed'

async def test_review_cas_agent_provenance_and_stop_intent(owned):
    owner,host,now,_=owned;args=schedule_args();preview=await action(owner,'schedule.preview',args)
    host.source['inputOrigin']='scheduler'
    with pytest.raises(ValueError,match='actual user'):await action(owner,'schedule.create',{**args,'expectedRevision':0,'previewHash':preview['previewHash'],'sourceMessageId':'user1'},origin='agent')
    host.source['inputOrigin']='ui';host.session['interruptionRevision']=1
    with pytest.raises(ValueError,match='changed'):await action(owner,'schedule.create',{**args,'expectedRevision':0,'previewHash':preview['previewHash']})
    schedule=await create(owner)
    host.session['interruptionRevision']=2;now[0]=110;await owner.schedules.tick();await settle(owner)
    assert owner.schedules.store.get(SID,schedule['id'])['status']=='needs_review'
    assert not any(m=='submitScheduled' for m,_ in host.calls)

async def test_common_admission_waits_for_confirmed_completion_and_no_duplicate(owned):
    owner,host,now,_=owned;schedule=await create(owner);now[0]=110;host.block.clear()
    await owner.schedules.tick();await asyncio.sleep(.01)
    run=owner.schedules.store.run_page(SID)['items'][0];assert run['phase']=='accepted'
    await owner.schedules.tick();assert len([1 for m,_ in host.calls if m=='submitScheduled'])==1
    host.block.set();await settle(owner)
    run=owner.schedules.store.run(SID,run['id']);assert run['phase']=='completed' and run['notificationDecision']['source']=='unreported'
    assert run['notificationDecision']['notify'] is False
    submission=next(args for m,args in host.calls if m=='submitScheduled')
    assert submission['input']['taskId']=='task' and submission['input']['kind']=='monitor'

async def test_transport_unknown_never_replays_and_requires_reconciliation(owned):
    owner,host,now,_=owned;schedule=await create(owner);host.raise_submit=True;now[0]=110
    await owner.schedules.tick();await settle(owner);run=owner.schedules.store.run_page(SID)['items'][0]
    assert run['phase']=='unknown';assert owner.schedules.store.get(SID,schedule['id'])['status']=='needs_review'
    now[0]=170;await owner.schedules.tick();assert len([1 for m,_ in host.calls if m=='submitScheduled'])==1
    result=await action(owner,'schedule.reconcile',{'runId':run['id'],'expectedRevision':run['revision'],'resolution':'abandoned','evidence':'Inspected original effects'},command='reconcile')
    assert result['run']['phase']=='abandoned'

async def test_new_task_clone_uses_reviewed_hash_then_separate_native_task(owned):
    owner,host,now,_=owned;await create(owner,destination='new_task',newTaskMaxTurns=2);now[0]=110
    await owner.schedules.tick();await settle(owner)
    calls=[m for m,_ in host.calls];assert calls.index('createSession')<calls.index('submitScheduled')
    request=next(a for m,a in host.calls if m=='createSession');assert request['expectedConfigurationHash']=='original'
    run=owner.schedules.store.run_page(SID)['items'][0];assert run['destinationSessionId']=='ahp-session:/destination' and run['phase']=='completed'

async def test_question_dependency_blocks_before_admission(owned):
    owner,host,now,_=owned;host.task['questionIds']=['q1'];await create(owner);now[0]=110
    await owner.schedules.tick();assert not any(m=='submitScheduled' for m,_ in host.calls)
    assert owner.schedules.store.page(SID)['items'][0]['status']=='needs_review'

async def test_operation_admission_receipts_survive_transport_failure_and_actor_conflict(owned):
    owner,host,_,_=owned;host.raise_submit=True;args={'requestId':'request1','command':'a harmless fixture'}
    with pytest.raises(RuntimeError):await action(owner,'operations.submit',args)
    receipt=await action(owner,'operations.submit',args);assert receipt['state']=='outcome_unknown'
    assert len([1 for m,a in host.calls if m=='nativeControl' and a['operation']=='operations.submit'])==1
    with pytest.raises(ValueError,match='different'):await action(owner,'operations.submit',args,origin='agent')

async def test_operation_observation_cursor_and_cancel_never_mounts_cold_owner(owned):
    owner,host,_,_=owned
    event={'schemaVersion':1,'eventId':'p1:1','sequence':1,'operationId':'p1','ownerId':'mount1','source':'tool-bash','kind':'process','phase':'started','at':1}
    await owner.request('observe',{'session':SID,'nativeSessionId':'native1','event':event})
    status=await action(owner,'operations.status',{'id':'p1'});assert status['ownerId']=='mount1'
    with pytest.raises(ValueError,match='retired'):await action(owner,'operations.cancel',{'id':'p1'})
    assert not any(m=='nativeControl' for m,_ in host.calls)
    await owner.request('interrupted',{'session':SID})
    assert (await action(owner,'operations.status',{'id':'p1'}))['state']=='outcome_unknown'

async def test_recurring_schedule_advances_only_its_own_admission_revision(owned):
    owner,host,now,_=owned;schedule=await create(owner);now[0]=110
    await owner.schedules.tick();await settle(owner)
    assert owner.schedules.store.get(SID,schedule['id'])['executionRevision']==1
    now[0]=170;await owner.schedules.tick();await settle(owner)
    assert len([1 for m,_ in host.calls if m=='submitScheduled'])==2
    host.session['executionRevision']+=1
    now[0]=230;await owner.schedules.tick();await settle(owner)
    assert len([1 for m,_ in host.calls if m=='submitScheduled'])==2
    assert owner.schedules.store.get(SID,schedule['id'])['status']=='needs_review'

@pytest.mark.parametrize('intervening_execution',[False,True])
async def test_new_task_admission_captures_mounted_configuration_without_accepting_work_races(owned,intervening_execution):
    owner,host,now,_=owned
    destination={**host.session,'uri':'ahp-session:/destination','configurationHash':'unmounted'}
    async def boundary(method,args):
        if method=='inspectSession' and args['session']==destination['uri']:
            return destination.copy()
        result=await host(method,args)
        if method=='nativeControl' and args['operation']=='task.create':
            destination['configurationHash']='mounted-copy'
            if intervening_execution:destination['executionRevision']+=1
        if method=='submitScheduled':
            assert args['input']['expectedConfigurationHash']=='mounted-copy'
        return result
    owner.schedules.host=boundary
    await create(owner,destination='new_task',newTaskMaxTurns=2);now[0]=110
    await owner.schedules.tick();await settle(owner)
    submissions=[a for m,a in host.calls if m=='submitScheduled']
    assert len(submissions)==(0 if intervening_execution else 1)
    run=owner.schedules.store.run_page(SID)['items'][0]
    assert run['phase']==('unknown' if intervening_execution else 'completed')
