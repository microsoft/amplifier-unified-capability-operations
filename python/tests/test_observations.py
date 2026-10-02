import asyncio
import copy
import json
import pytest
from amplifier_unified_operations.observations import Observations

SID='ahp-session:/watch'

async def setup(tmp_path):
    now=[100.0];calls=[];changes=[];controls={'uri':SID,'status':'idle','nativeSessionId':'native','executionRevision':0,'interruptionRevision':0,'configurationHash':'config'}
    task={'id':'task','revision':1,'status':'active','questionIds':[]};outcome={'status':'pending','target':{'record':'one'},'summary':'Source pending'};behavior={}
    descriptor={'id':'observer','requestArgument':'observation'}
    async def host(method,args):
        calls.append((method,copy.deepcopy(args)))
        if method=='inspectSession':return copy.deepcopy(controls)
        if method=='readTaskState':return {'available':True,'revision':'task-revision','task':copy.deepcopy(task)}
        if method=='qualifiedObservation':
            if args['operation']=='qualify':return copy.deepcopy(descriptor)
            if args['operation']=='check':
                if behavior.get('ineligible'):raise ValueError('Source changed')
                return {'eligible':True}
            if behavior.get('afterRead'):await behavior['afterRead']()
            return copy.deepcopy(outcome)
        if method=='nativeControlExisting':
            if behavior.get('cold'):raise ValueError('No current worker')
            return {'task':task}
        if method=='submitObservation':
            supplied=args['input'];proof=await owner.authorize(SID,{'inputId':supplied['commandId'],**{k:v for k,v in supplied.items() if k!='commandId' and not k.startswith('expected')}})
            assert proof['admitted'],proof
            if behavior.get('unknown'):raise ValueError('Lost acknowledgement')
            if behavior.get('coldAdmission'):return {'accepted':False,'executed':False,'reason':'waiting_worker'}
            return {'accepted':True,'turnId':supplied['commandId']}
        if method=='waitForTurn':return behavior.get('receipt',{'status':'completed','workContinues':False})
        if method=='readUserMessage':return {'id':'human','role':'user','inputOrigin':behavior.get('origin','user'),'text':'Watch this exact source','provenance':{'source':'host-admission'}}
        if method=='questionDependency':return {'text':'Explicit answer'}
        raise AssertionError(method)
    async def changed(sid):changes.append(sid)
    owner=Observations(tmp_path/'observations.sqlite3',host,changed,clock=lambda:now[0])
    async def action(name,args,origin='ui'):
        return await owner.dispatch('observation.'+name,{'sessionId':SID,**args},origin,'client','command-'+name)
    await action('qualify',{'descriptor':{}})
    args={'observerId':'observer','target':{'record':'one'},'scope':{},'sourceId':'source','args':{},'intervalSeconds':15,'durationSeconds':120,'readTimeoutSeconds':3}
    async def create():
        preview=await action('preview',args)
        return (await action('create',{**args,'previewHash':preview['previewHash'],'requestId':'arm'}))['watch']
    async def tick():await owner.tick();await owner.drain()
    return owner,action,create,tick,args,controls,task,outcome,behavior,now,calls,changes,host,changed

async def test_pending_reads_are_quiet_and_terminal_result_has_one_finite_delivery(tmp_path):
    owner,action,create,tick,args,controls,task,outcome,behavior,now,calls,changes,_,_=await setup(tmp_path)
    try:
        watch=await create();changes.clear()
        for _ in range(3):await tick();now[0]+=16
        assert changes==[]
        assert not [call for call in calls if call[0] in {'nativeControlExisting','submitObservation','waitForTurn'}]
        assert len(owner.store.page('run',SID,watch=watch['id'])['items'])==3
        projection=owner.store.operations(SID)
        assert projection[0]['id']=='observation:'+watch['id'] and not projection[0]['controlAvailable']
        assert owner.store.operations('ahp-session:/other')==[]
        assert owner.store.operations(SID,cursor=[projection[0]['createdAt'],projection[0]['id']])==[]
        outcome.update(status='actionable',summary='Exact result established')
        await tick();await tick();await tick();await tick()
        rows=owner.store.page('outbox',SID)['items'];assert len(rows)==1 and rows[0]['phase']=='completed'
        assert len([call for call in calls if call[0]=='submitObservation'])==1
        assert all(call[1]['waitMs']==1000 for call in calls if call[0]=='waitForTurn')
        assert (await action('request',{'requestId':'arm'}))['watch']['status']=='ended'
        assert (await create())['id']==watch['id']
        with pytest.raises(ValueError,match='different intent'):
            await action('create',{**args,'durationSeconds':300,'previewHash':'ignored-for-duplicate','requestId':'arm'})
    finally:await owner.close()

@pytest.mark.parametrize('change',['stop','task','cancel','expiry','source'])
async def test_read_race_never_restores_revoked_authority(tmp_path,change):
    owner,action,create,tick,args,controls,task,outcome,behavior,now,calls,_,_,_=await setup(tmp_path)
    try:
        watch=await create();outcome['status']='actionable'
        async def changed():
            if change=='stop':controls['interruptionRevision']+=1
            elif change=='task':task['revision']+=1
            elif change=='cancel':await action('cancel',{'id':watch['id'],'expectedRevision':1,'requestId':'cancel'})
            elif change=='expiry':now[0]+=130
            else:behavior['ineligible']=True
        behavior['afterRead']=changed
        await tick()
        assert owner.store.page('run',SID)['items'][0]['phase']=='discarded'
        if change!='expiry':await tick();assert not [call for call in calls if call[0]=='submitObservation']
        else:assert owner.store.page('outbox',SID)['items'][0]['outcome']['status']=='expired'
    finally:await owner.close()

async def test_lost_handoff_ack_survives_restart_without_replay_or_rearm(tmp_path):
    owner,action,create,tick,args,_,_,outcome,behavior,_,calls,_,host,changed=await setup(tmp_path)
    watch=await create();outcome['status']='actionable';behavior['unknown']=True
    await tick();await tick();assert owner.store.page('outbox',SID)['items'][0]['phase']=='unknown';await owner.close()
    restarted=Observations(tmp_path/'observations.sqlite3',host,changed,clock=lambda:100)
    try:
        await restarted.tick();await restarted.drain()
        assert len([call for call in calls if call[0]=='submitObservation'])==1
        assert restarted.store.get('watch',watch['id'])['status']=='ended'
        with pytest.raises(ValueError,match='nonterminal'):
            await restarted.dispatch('observation.resume',{'sessionId':SID,**args,'id':watch['id'],'expectedRevision':2,'requestId':'resume','previewHash':'stale'},'ui','client','resume')
    finally:await restarted.close()

async def test_cold_result_is_saved_without_starting_a_runtime_and_rechecked_when_warm(tmp_path):
    owner,_,create,tick,_,_,_,outcome,behavior,now,calls,_,_,_=await setup(tmp_path)
    try:
        await create();outcome['status']='actionable';behavior['cold']=True
        await tick();await tick();assert owner.store.page('outbox',SID)['items'][0]['phase']=='waiting_worker'
        assert not [call for call in calls if call[0]=='submitObservation']
        behavior['cold']=False;now[0]+=16;await tick();await tick()
        assert owner.store.page('outbox',SID)['items'][0]['phase']=='completed'
        assert not [call for call in calls if call[0]=='nativeControl']
    finally:await owner.close()

async def test_observer_operator_and_human_provenance_are_not_self_attested(tmp_path):
    owner,action,_,_,args,_,_,_,behavior,_,_,_,_,_=await setup(tmp_path)
    try:
        with pytest.raises(ValueError,match='operator'):await action('qualify',{'descriptor':{}},'agent')
        review=await action('preview',args)
        behavior['origin']='question'
        with pytest.raises(ValueError,match='human'):await action('create',{**args,'previewHash':review['previewHash'],'requestId':'bad','sourceMessageId':'human'},'agent')
        with pytest.raises(ValueError,match='client grant'):await action('preview',{**args,'presentationRequest':{'kind':'browser'}})
    finally:await owner.close()

@pytest.mark.parametrize('when',['admission','receipt','generic_failure'])
async def test_only_explicit_no_effect_eligibility_retries_exact_saved_input(tmp_path,when):
    owner,_,create,tick,_,_,_,outcome,behavior,now,calls,_,_,_=await setup(tmp_path)
    try:
        await create();outcome['status']='actionable'
        if when=='admission':behavior['coldAdmission']=True
        elif when=='receipt':behavior['receipt']={'status':'failed','executed':False,'retryEligibility':'waiting_worker'}
        else:behavior['receipt']={'status':'failed'}
        await tick();await tick();await tick()
        assert owner.store.page('outbox',SID)['items'][0]['phase']==('failed' if when=='generic_failure' else 'waiting_worker')
        behavior.clear();now[0]+=16;await tick();await tick()
        admitted=[call[1]['input']['commandId'] for call in calls if call[0]=='submitObservation']
        assert len(admitted)==(1 if when=='generic_failure' else 2) and len(set(admitted))==1
    finally:await owner.close()

async def test_25k_cold_records_do_not_expand_due_scan_or_selected_pages(tmp_path):
    owner,_,create,tick,_,_,_,_,_,_,_,_,_,_=await setup(tmp_path)
    try:
        with owner.store.db:
            for number in range(25000):owner.store.put('watch',{'id':str(number),'sessionId':'ahp-session:/cold'+str(number),'status':'cancelled','createdAt':number})
        watch=await create();steps=[];owner.store.db.set_progress_handler(lambda:steps.append(1) and 0,100)
        assert owner.store.due(100)[0]['id']==watch['id'];assert len(owner.store.page('watch',SID)['items'])==1;assert len(steps)<20
        plan=owner.store.db.execute("EXPLAIN QUERY PLAN SELECT body FROM records WHERE kind='watch' AND state='active' AND due<=? ORDER BY due,id LIMIT 16",(100,)).fetchall()
        assert any('records_due' in row['detail'] for row in plan)
    finally:await owner.close()
