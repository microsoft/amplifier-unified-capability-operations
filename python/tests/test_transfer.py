import json
import pytest
from amplifier_unified_operations.server import Owner
from amplifier_portability.evidence import decode_body

SID='ahp-session:/source';DEST='ahp-session:/destination'
async def denied(method,args):raise AssertionError('Transfer must not start native or client work: '+method)
async def quiet(*args):pass

def question(owner,identity='q',text='Keep this answer'):
    record={'id':identity,'sessionId':SID,'status':'answered','createdAt':1.0,'question':text,'delivery':{'status':'unknown'}}
    with owner.questions.db:owner.questions.put(record)
    return record

async def test_history_stage_activate_restart_never_resumes_questions_schedules_or_watches(tmp_path):
    source=Owner({'dataDir':str(tmp_path/'source')},denied,quiet);dest=Owner({'dataDir':str(tmp_path/'dest')},denied,quiet)
    try:
        original=question(source)
        args={'session':SID,'transferId':'transfer'}
        evidence=await source.request('exportTransferEvidence',args)
        assert decode_body(evidence)['records'][0]['value']==original
        assert evidence['disposition']=='complete'
        question(source,'later','Does not mutate the sealed export')
        assert await source.request('exportTransferEvidence',args)==evidence
        stage={'transferId':'transfer','nativeSessionId':'native','sourceHost':'paired-source','evidence':evidence}
        assert (await dest.request('stageTransferEvidence',stage))['executionAuthority'] is False
        with pytest.raises(ValueError,match='unavailable'):await dest.request('readTransferEvidence',{'session':DEST,'transferId':'transfer'})
        activation={'session':DEST,'transferId':'transfer','evidenceHash':evidence['sha256'],'releaseHash':'a'*64}
        receipt=await dest.request('activateTransferEvidence',activation)
        assert receipt['replayed'] is False
        await dest.close();dest=Owner({'dataDir':str(tmp_path/'dest')},denied,quiet)
        assert await dest.request('activateTransferEvidence',activation)==receipt
        history=await dest.request('readTransferEvidence',{'session':DEST,'transferId':'transfer'})
        assert history['items'][0]['value']==original
        assert history['items'][0]['namespace']=='paired-source:transfer'
        assert dest.questions.page(DEST,{})['items']==[]
        assert dest.schedules.store.page(DEST)['items']==[]
        assert dest.observations.store.page('watch',DEST)['items']==[]
        with pytest.raises(ValueError,match='different'):await dest.request('activateTransferEvidence',{**activation,'session':'ahp-session:/other'})
        with pytest.raises(ValueError,match='integrity'):await dest.request('stageTransferEvidence',{**stage,'evidence':{**evidence,'sha256':'0'*64}})
    finally:await source.close();await dest.close()

async def test_bounded_evidence_is_explicitly_partial_and_cross_session_cache_rejected(tmp_path):
    source=Owner({'dataDir':str(tmp_path)},denied,quiet)
    try:
        for i in range(10):question(source,str(i),'x'*1000)
        value=await source.request('exportTransferEvidence',{'session':SID,'transferId':'small','limitBytes':1024})
        assert value['bytes']<=1024 and value['disposition']=='partial'
        assert value['omissions']==[{'collection':'question','count':10,'reason':'bounded owner evidence'}]
        with pytest.raises(ValueError,match='preview'):decode_body(value)
        with pytest.raises(ValueError,match='another'):await source.request('exportTransferEvidence',{'session':DEST,'transferId':'small'})
    finally:await source.close()
