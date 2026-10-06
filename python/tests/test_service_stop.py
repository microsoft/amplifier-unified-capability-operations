import pytest
IDENTITY=dict(installationId='installation',dataScope='private',ownerId='platform-owner',instanceId='host-one',releaseDigest='signed-release')
CONTEXT=dict(fenceId='service-fence',commandId='stop',purpose='service-stop',instanceId='host-one',dataScope='private',serviceIdentity=IDENTITY)
def proof(outcome='ready'):
    observed={**IDENTITY,'instanceId':'host-two' if outcome=='ready' else 'host-one'}
    return dict(verified=True,fenceId='service-fence',commandId='stop',outcome=outcome,instanceId=observed['instanceId'],dataScope='private',receiptId='settled',kind='service-lifecycle',serviceOutcome='resumed' if outcome=='ready' else 'stop-refused',expected=dict(IDENTITY),observed=observed,**(dict(resumeCommandId='resume',exitReceiptId='owned-exit',readyReceiptId='ready') if outcome=='ready' else dict(refusalReceiptId='no-effect')))

from amplifier_unified_operations.server import Owner
from amplifier_unified_operations.quiescence import Intake
async def forbidden(*args):raise AssertionError('No execution callback is allowed')
async def test_service_negotiation_requires_installed_foundation_contract(tmp_path,monkeypatch):
    owner=Owner({'dataDir':str(tmp_path)},forbidden,forbidden)
    try:
        assert (await owner.request('initialize',{}))['quiescence']['serviceStop']['version']==1
        monkeypatch.setattr(Intake,'SERVICE_STOP_VERSION',0)
        assert 'serviceStop' not in (await owner.request('initialize',{}))['quiescence']
        result=await owner.request('quiescence.acquire',CONTEXT);assert result['executed'] is False
        assert owner.intake.fence is None
    finally:await owner.close()
async def test_service_owner_restart_preserves_context_and_rejects_generic_proof(tmp_path):
    owner=Owner({'dataDir':str(tmp_path)},forbidden,forbidden)
    assert (await owner.request('quiescence.acquire',CONTEXT))['acquired'];await owner.close()
    owner=Owner({'dataDir':str(tmp_path)},forbidden,forbidden)
    try:
        assert owner.intake.fence['serviceIdentity']==IDENTITY
        with pytest.raises(ValueError):await owner.request('quiescence.release',{**CONTEXT,'outcome':'unchanged','proof':{'kind':'admission-refused'}})
        for field in IDENTITY:
            wrong=proof();wrong['expected'][field]='foreign'
            with pytest.raises(ValueError):await owner.request('quiescence.release',{**CONTEXT,'outcome':'ready','proof':wrong})
        exact={**CONTEXT,'outcome':'ready','proof':proof()};await owner.request('quiescence.release',exact)
        await owner.close();owner=Owner({'dataDir':str(tmp_path)},forbidden,forbidden)
        assert (await owner.request('quiescence.release',exact))['released']
        with pytest.raises(ValueError):await owner.request('quiescence.release',{**exact,'proof':{**proof(),'readyReceiptId':'changed'}})
        assert not owner.intake.fence
    finally:await owner.close()
