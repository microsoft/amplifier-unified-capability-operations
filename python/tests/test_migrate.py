import hashlib
import json
from pathlib import Path
import sqlite3
from filelock import FileLock,Timeout
import pytest
from amplifier_unified_operations.migrate import import_page
from amplifier_unified_operations.history import HistoricalRecords,MAX_RECORD
from amplifier_unified_operations.server import Owner

SID='ahp-session:/migrated'
def legacy(path):
    db=sqlite3.connect(path);db.execute('CREATE TABLE schedules(id TEXT,session_id TEXT,value TEXT)')
    for sid in ('chosen','elsewhere'):
        for i in range(3):
            row={'id':sid+str(i),'sessionId':sid,'status':'active','prompt':'Retained monitoring '+str(i),'unknownEffect':True}
            db.execute('INSERT INTO schedules VALUES(?,?,?)',(row['id'],sid,json.dumps(row)))
    db.commit();db.close()
def import_one(source,destination,**kw):return import_page(source,destination,source_session='chosen',session=SID,kind='schedule',**kw)

async def test_selected_pages_preserve_originals_without_live_schedule_or_host_work(tmp_path):
    source=tmp_path/'legacy.sqlite';legacy(source);before=source.read_bytes();target=tmp_path/'owner'
    first=import_one(source,target,limit=2);assert len(first['items'])==2 and first['nextCursor']==2
    second=import_one(source,target,after=first['nextCursor']);assert len(second['items'])==1 and second['nextCursor'] is None
    async def forbidden(*_):raise AssertionError('Historical inspection cannot run host work')
    owner=Owner({'dataDir':str(target)},forbidden,forbidden)
    async def action(operation,args):return await owner.request('action',{'operation':operation,'session':SID,'args':args})
    try:
        page=await action('operations.history.list',{'limit':2});assert len(page['items'])==2 and page['executionAuthority'] is False
        item=await action('operations.history.read',{'id':page['items'][0]['id']});assert item['record']['status']=='active' and item['controlAvailable'] is False and item['replayed'] is False
        assert hashlib.sha256(json.dumps(item['record']).encode()).hexdigest()==item['sha256']
        last=await action('operations.history.list',{'limit':2,'cursor':page['nextCursor']});assert len(last['items'])==1
        assert owner.schedules.store.list(SID)==[] and owner.schedules.pending==set()
        with pytest.raises(ValueError):await owner.request('action',{'operation':'operations.history.read','session':'ahp-session:/other','args':{'id':item['id']}})
        with pytest.raises(ValueError,match='scope'):owner._historical.page('ahp-session:/other',{'cursor':page['nextCursor']})
    finally:await owner.close()
    assert source.read_bytes()==before

def test_repeat_is_idempotent_and_conflicting_mapping_is_retained(tmp_path):
    source=tmp_path/'legacy.sqlite';legacy(source);target=tmp_path/'owner'
    first=import_one(source,target);second=import_one(source,target)
    assert all(row['classification']=='already-imported' for row in second['items'])
    assert [r['id'] for r in first['items']]==[r['id'] for r in second['items']]
    with pytest.raises(ValueError,match='another destination'):import_page(source,target,source_session='chosen',session='ahp-session:/other',kind='schedule')
    history=HistoricalRecords(target/'legacy.sqlite3')
    try:assert len(history.page(SID,{})['items'])==3
    finally:history.close()

def test_legacy_operation_requests_keep_request_identity_and_unknown_outcome(tmp_path):
    source=tmp_path/'operations.sqlite3'
    db=sqlite3.connect(source)
    db.execute('CREATE TABLE operation_requests(session_id TEXT,id TEXT,signature TEXT,value TEXT,PRIMARY KEY(session_id,id))')
    record={'requestId':'original-request','sessionId':'chosen','action':'operation.start',
            'state':'outcome_unknown','createdAt':1700000000,'updatedAt':1700000001}
    db.execute('INSERT INTO operation_requests VALUES(?,?,?,?)',('chosen','original-request','saved-signature',json.dumps(record)))
    db.commit();db.close();before=source.read_bytes();target=tmp_path/'owner'
    result=import_page(source,target,source_session='chosen',session=SID,kind='operation-request')
    assert result['items'][0]['classification']=='imported-historical'
    history=HistoricalRecords(target/'legacy.sqlite3')
    try:
        item=history.page(SID,{})['items'][0]
        assert item['summary']['id']=='original-request'
        saved=history.read(SID,item['id'])
        assert saved['record']==record and saved['controlAvailable'] is False
        assert saved['executionAuthority'] is False and saved['replayed'] is False
    finally:history.close()
    assert source.read_bytes()==before

def test_invalid_and_oversized_source_records_remain_classified_without_body_loading(tmp_path):
    source=tmp_path/'legacy.sqlite';legacy(source)
    db=sqlite3.connect(source);db.execute('INSERT INTO schedules VALUES(?,?,?)',('bad','chosen','{bad'))
    db.execute('INSERT INTO schedules VALUES(?,?,?)',('large','chosen','x'*(MAX_RECORD+1)));db.commit();db.close()
    before=source.read_bytes();target=tmp_path/'owner';result=import_one(source,target)
    assert [r['classification'] for r in result['items']][-2:]==['invalid-source-retained','oversize-source-retained']
    history=HistoricalRecords(target/'legacy.sqlite3')
    try:
        assert history.db.execute("SELECT original FROM imports WHERE classification='oversize-source-retained'").fetchone()==(None,)
        assert len(history.page(SID,{})['items'])==3
    finally:history.close()
    assert source.read_bytes()==before

def test_import_refuses_running_owner_and_missing_collection_is_explicit(tmp_path):
    source=tmp_path/'legacy.sqlite';legacy(source);target=tmp_path/'owner';target.mkdir()
    with FileLock(str(target/'owner.lock')):
        with pytest.raises(Timeout):import_one(source,target)
    result=import_page(source,target,source_session='chosen',session=SID,kind='question')
    assert result['tableAbsent'] is True and result['items']==[]
    with pytest.raises(ValueError,match='outside'):import_one(source,tmp_path)
