import json
import pytest
from amplifier_unified_operations.server import Owner
SID='ahp-session:/scheduled'
FENCE=dict(fenceId='retention',commandId='reviewed-hide',purpose='managed-files-disposal',instanceId='instance',dataScope='private')
async def forbidden(*args):raise AssertionError('No native callback or due dispatch')
async def noop(*args):pass
@pytest.mark.asyncio
async def test_future_schedule_required_question_unknown_operation_and_other_session(tmp_path):
    owner=Owner({'dataDir':str(tmp_path)},forbidden,noop)
    try:
        owner.schedules.store.db.execute('INSERT INTO schedules VALUES(?,?,?)',('future',SID,json.dumps({'status':'active','nextDue':99999999999})));owner.schedules.store.db.commit()
        owner.questions.db.execute('INSERT INTO questions VALUES(?,?,?,?,?,?)',('required',SID,'pending',0,None,json.dumps({'required':True})));owner.questions.db.commit()
        owner.operations.journal.db.execute('INSERT INTO operation_requests VALUES(?,?,?,?)',(SID,'lost','signature',json.dumps({'state':'outcome_unknown'})));owner.operations.journal.db.commit()
        family=[SID]+['ahp-session:/child-'+str(i) for i in range(100)];args={'context':FENCE,'sessions':family,'limit':101,'allocation':dict(allocationId='12345678-1234-1234-1234-123456789abc',executionDirectory='/owned/managed/files',allocationHash='a'*64,treeHash='b'*64,entryCount=1,bytes=5)}
        with pytest.raises(ValueError):await owner.request('quiescence.managedFiles',args)
        assert (await owner.request('quiescence.acquire',FENCE))['acquired']
        report=await owner.request('quiescence.managedFiles',args)
        assert report=={'coverage':'complete','protected':[{'session':SID,'reasons':['operation-unsettled','required-question','scheduled-work']}],'omissions':[]}
        with pytest.raises(ValueError):await owner.request('action',{'operation':'question.create'})
        await owner.request('start',{'observations':True})
        # Scheduler intake remains closed; a due wake cannot submit.
        assert owner.intake.fence==FENCE
        query="SELECT 1 FROM schedules WHERE session_id=? AND json_extract(value,'$.status') IN ('active','paused','needs_review') LIMIT 1"
        plan=str(owner.schedules.store.db.execute('EXPLAIN QUERY PLAN '+query,(SID,)).fetchall())
        assert 'retention_schedule' in plan
        with pytest.raises(ValueError):await owner.request('quiescence.managedFiles',{**args,'sessions':family+['ahp-session:/overflow']})
        proof={'verified':True,**FENCE,'outcome':'unchanged','receiptId':'exact-child-proof'}
        await owner.request('quiescence.release',{**FENCE,'outcome':'unchanged','proof':proof})
        with pytest.raises(ValueError):await owner.request('quiescence.managedFiles',args)
    finally:await owner.close()
