import asyncio,base64,json,sqlite3
import pytest
from amplifier_unified_operations.server import Owner
from amplifier_unified_operations.snapshots import STORES,restore_inactive

CONTEXT=dict(fenceId='held',commandId='recovery',purpose='recovery',instanceId='original',dataScope='owned')
ARGS=dict(context=CONTEXT,commandId='snapshot-one',privateContentReviewed=True)
async def forbidden(method,args):raise AssertionError('No external work: '+method)
async def quiet(*args):pass

async def test_exact_wal_images_restore_unknown_and_retained_fence_without_work(tmp_path):
    root=tmp_path/'source';owner=Owner({'dataDir':str(root)},forbidden,quiet)
    history=tmp_path/'canonical.jsonl';history.write_bytes(b'original transcript\n')
    with owner.questions.db:
        owner.questions.put({'id':'q','sessionId':'ahp-session:/source','status':'answered','createdAt':1,'question':'Original','delivery':{'status':'unknown'}})
    try:
        assert (await owner.request('quiescence.acquire',CONTEXT))['acquired']
        originals={p:p.read_bytes() for p in root.iterdir() if p.name.endswith(('.sqlite3','-wal'))}
        result=await owner.request('quiescence.snapshot',ARGS)
        assert result['status']=='captured' and not result['completeProductBackup']
        assert {r['id'] for r in result['snapshot']['stores']}==set(STORES)
        assert await owner.request('quiescence.snapshot',ARGS)==result
        window=await owner.request('snapshot.read',dict(commandId='snapshot-one',store='questions',offset=0,limit=16))
        assert base64.b64decode(window['data'])==b'SQLite format 3\0'
        def held():owner.snapshots.assert_held(CONTEXT)
        restored=restore_inactive(owner.snapshots.directory('snapshot-one')/'images',tmp_path/'inactive',manifest_sha256=result['snapshot']['manifestSha256'],assert_held=held)
        assert not restored['readyForActivation'] and not restored['completeProductRestore']
        assert all(p.read_bytes()==raw for p,raw in originals.items())
        assert history.read_bytes()==b'original transcript\n'
        await owner.request('quiescence.release',{**CONTEXT,'outcome':'unknown'})
        with pytest.raises(ValueError,match='fresh held'):await owner.request('quiescence.snapshot',{**ARGS,'commandId':'after-release'})
    finally:await owner.close()
    inactive=Owner({'dataDir':str(tmp_path/'inactive')},forbidden,quiet)
    try:
        inactive.schedules.tick=forbidden;inactive.observations.tick=forbidden
        await inactive.request('start',{'observations':True});await asyncio.sleep(.02)
        assert inactive.intake.fence==CONTEXT and not inactive.tick_task.done()
        q=inactive.questions.get('ahp-session:/source','q')
        assert q['delivery']['status']=='unknown'
        await inactive.request('quiescence.acquire',CONTEXT)
        with pytest.raises(ValueError,match='fresh held'):await inactive.request('quiescence.snapshot',{**ARGS,'commandId':'restart'})
    finally:await inactive.close()

async def test_unknown_snapshot_command_is_passive_after_failure_and_restart(tmp_path,monkeypatch):
    from amplifier_operations import snapshots
    calls=[]
    def interrupted(*args,**kwargs):calls.append(True);raise RuntimeError('Interrupted copy')
    monkeypatch.setattr(snapshots,'capture_snapshot',interrupted)
    owner=Owner({'dataDir':str(tmp_path)},forbidden,quiet)
    await owner.request('quiescence.acquire',CONTEXT)
    try:
        with pytest.raises(RuntimeError,match='Interrupted'):await owner.request('quiescence.snapshot',ARGS)
        assert (await owner.request('snapshot.inspect',{'commandId':'snapshot-one'}))['receipt']['status']=='unknown'
        assert (await owner.request('quiescence.snapshot',ARGS))['status']=='unknown'
        with pytest.raises(ValueError,match='differs'):await owner.request('quiescence.snapshot',{**ARGS,'context':{**CONTEXT,'instanceId':'other'}})
    finally:await owner.close()
    owner=Owner({'dataDir':str(tmp_path)},forbidden,quiet)
    try:assert (await owner.request('quiescence.snapshot',ARGS))['status']=='unknown';assert len(calls)==1
    finally:await owner.close()

async def test_gate_unclassified_storage_private_review_and_tampered_artifacts_refuse(tmp_path):
    owner=Owner({'dataDir':str(tmp_path)},forbidden,quiet)
    try:
        with pytest.raises(ValueError,match='fresh held'):await owner.request('quiescence.snapshot',ARGS)
        owner.intake.background=1
        assert not (await owner.request('quiescence.acquire',CONTEXT))['acquired']
        owner.intake.background=0
        await owner.request('quiescence.acquire',CONTEXT)
        with pytest.raises(ValueError,match='private-content'):await owner.request('quiescence.snapshot',{**ARGS,'privateContentReviewed':False})
        unknown=tmp_path/'unclassified';unknown.write_bytes(b'preserve original')
        with pytest.raises(ValueError,match='Unclassified'):await owner.request('quiescence.snapshot',ARGS)
        assert unknown.read_bytes()==b'preserve original';unknown.unlink()
        result=await owner.request('quiescence.snapshot',ARGS)
        images=owner.snapshots.directory('snapshot-one')/'images'
        (images/'questions.sqlite3').write_bytes(b'changed original image')
        with pytest.raises(ValueError,match='changed'):await owner.request('snapshot.read',dict(commandId='snapshot-one',store='questions',offset=0,limit=16))
        with pytest.raises(ValueError,match='differs'):restore_inactive(images,tmp_path/'inactive',manifest_sha256=result['snapshot']['manifestSha256'],assert_held=lambda:owner.snapshots.assert_held(CONTEXT))
        assert not (tmp_path/'inactive').exists()
    finally:await owner.close()

async def test_old_optional_library_never_advertises_or_grants_snapshot(tmp_path,monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules,'amplifier_operations.snapshots',None)
    owner=Owner({'dataDir':str(tmp_path)},forbidden,quiet)
    try:
        assert 'snapshots' not in (await owner.request('initialize',{}))['quiescence']
        await owner.request('quiescence.acquire',CONTEXT)
        with pytest.raises(ValueError,match='no snapshot support'):await owner.request('quiescence.snapshot',ARGS)
        assert not (tmp_path/'snapshots').exists()
    finally:await owner.close()


def test_restore_released_guard_precedes_any_manifest_io(tmp_path,monkeypatch):
    import amplifier_unified_operations.snapshots as module
    def released():raise ValueError('Original restore guard released')
    def no_io(*args,**kwargs):raise AssertionError('Manifest I/O before held guard')
    monkeypatch.setattr(module,'checked',no_io)
    with pytest.raises(ValueError,match='guard released'):
        restore_inactive(tmp_path/'snapshot',tmp_path/'inactive',manifest_sha256='a'*64,assert_held=released)
    assert not (tmp_path/'inactive').exists()

@pytest.mark.skipif(not hasattr(__import__('os'),'mkfifo'),reason='FIFO is a POSIX file type')
def test_restore_fifo_manifest_refuses_without_blocking_or_destination(tmp_path):
    import subprocess,sys,os
    snapshot=tmp_path/'snapshot';snapshot.mkdir();os.mkfifo(snapshot/'manifest.json')
    code="from amplifier_unified_operations.snapshots import restore_inactive; import sys; restore_inactive(sys.argv[1],sys.argv[2],manifest_sha256='a'*64,assert_held=lambda:None)"
    # Use the same exact installed/source module selected by the test caller;
    # timeout makes a regression fail causally rather than hang the suite.
    module_path=__import__('amplifier_unified_operations').__path__[0]
    bootstrap="import sys;sys.path.insert(0,sys.argv.pop(1));"+code
    result=subprocess.run([sys.executable,'-I','-B','-c',bootstrap,str(__import__('pathlib').Path(module_path).parent),str(snapshot),str(tmp_path/'inactive')],capture_output=True,text=True,timeout=2)
    assert result.returncode!=0 and 'Bounded regular Operations manifest required' in result.stderr
    assert not (tmp_path/'inactive').exists()
