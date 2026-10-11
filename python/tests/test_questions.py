import asyncio
import hashlib
import json
import pytest
from amplifier_unified_operations.questions import Questions

SID = 'ahp-session:/questions'


def setup(tmp_path, callback=None):
    calls, changed = [], []
    source = {'id': 'user-answer', 'role': 'user', 'createdAt': 101, 'text': 'Choose blue', 'inputOrigin': 'voice', 'voiceId': 'call', 'voiceItemId': 'item'}
    async def host(method, args):
        if method == 'readUserMessage': return source
        assert method == 'submitQuestionAnswer'
        calls.append(args)
        if callback: return await callback(args)
        request = args['input']
        proof = owner.authorize_delivery(SID, {**request, 'inputId': request['commandId']})
        assert proof['answerHash'] == hashlib.sha256(request['text'].encode()).hexdigest()
        return {'accepted': True}
    async def invalidate(sid): changed.append(sid)
    owner = Questions(tmp_path/'questions.sqlite3', host, invalidate, clock=lambda: 100)
    async def action(operation, args=None, command=None, origin='ui'):
        return await owner.dispatch('question.'+operation, {'sessionId': SID, **(args or {})}, origin, 'client', command or operation)
    return owner, action, calls, source, changed, host, invalidate


def question():
    return {'prompt': 'Which color?', 'required': True, 'dependency': 'Color selection', 'options': [{'id': 'blue', 'label': 'Blue'}]}


async def test_answer_commit_exact_proof_and_retry_are_independent_from_completion(tmp_path):
    owner, action, calls, _, changed, host, invalidate = setup(tmp_path)
    try:
        row = await action('create', question())
        with pytest.raises(ValueError, match='explicit answer'): owner.dependency(SID, row['id'])
        args = {'id': row['id'], 'expectedRevision': 1, 'optionId': 'blue'}
        answered = await action('answer', args)
        assert answered['status'] == 'answered' and answered['delivery']['status'] == 'accepted'
        assert owner.dependency(SID, row['id'])['text'] == 'Blue'
        assert (await action('answer', args)) == answered and len(calls) == 1
        with pytest.raises(ValueError, match='different'): await action('answer', {**args, 'optionId': 'other'})
        with pytest.raises(ValueError, match='differs'):
            owner.authorize_delivery(SID, {**calls[0]['input'], 'inputId': calls[0]['input']['commandId'], 'text': 'Injected input'})
        assert set(changed) == {SID}
    finally: owner.close()
    reopened = Questions(tmp_path/'questions.sqlite3', host, invalidate)
    try:
        assert reopened.get(SID, row['id'])['delivery']['status'] == 'accepted'
        assert len(calls) == 1
    finally: reopened.close()


async def test_agent_answer_requires_original_later_user_or_voice_provenance(tmp_path):
    owner, action, calls, source, _, _, _ = setup(tmp_path)
    try:
        row = await action('create', question())
        args = {'id': row['id'], 'expectedRevision': 1, 'text': 'Blue'}
        with pytest.raises(ValueError, match='actual user'): await action('answer', args, origin='agent')
        for patch in ({'inputOrigin':'unknown'}, {'inputOrigin':'scheduled'}, {'inputOrigin':'user','questionId':'prior'}, {'inputOrigin':'user','createdAt':99}):
            original = source.copy();source.update(patch)
            with pytest.raises(ValueError, match='actual user'): await action('answer', {**args, 'sourceMessageId':'user-answer'}, origin='agent')
            source.clear();source.update(original)
        answered = await action('answer', {**args, 'sourceMessageId':'user-answer'}, origin='agent')
        provenance = answered['answer']['provenance']
        assert provenance['voiceId'] == 'call' and provenance['inputOrigin'] == 'voice'
        assert 'text' not in provenance and len(provenance['textDigest']) == 64
        assert len(calls) == 1
    finally: owner.close()


async def test_unknown_delivery_and_restart_never_resend_an_answer(tmp_path):
    async def lost(_): raise RuntimeError('Lost acknowledgement after admission')
    owner, action, calls, _, _, host, invalidate = setup(tmp_path, lost)
    row = await action('create', question());args = {'id':row['id'], 'expectedRevision':1, 'text':'Blue'}
    answered = await action('answer', args)
    assert answered['delivery']['status'] == 'unknown' and len(calls) == 1
    owner.close();owner = Questions(tmp_path/'questions.sqlite3', host, invalidate)
    try:
        repeated = await owner.dispatch('question.answer', {'sessionId':SID, **args}, 'ui', 'client', 'answer')
        assert repeated['delivery']['status'] == 'unknown' and len(calls) == 1
        with pytest.raises(ValueError, match='closed'):
            await owner.dispatch('question.answer', {'sessionId':SID, **args}, 'ui', 'client', 'new-identity')
    finally: owner.close()


async def test_cancel_supersede_and_known_busy_do_not_invent_answers_or_delivery(tmp_path):
    async def busy(_): return {'accepted': False, 'executed': False, 'reason': 'Current turn is busy'}
    owner, action, calls, _, _, _, _ = setup(tmp_path, busy)
    try:
        old = await action('create', question())
        replacement = await action('supersede', {**question(), 'id':old['id'], 'expectedRevision':1})
        with pytest.raises(ValueError, match='explicit answer'): owner.dependency(SID, old['id'])
        with pytest.raises(ValueError, match='closed'): await action('answer', {'id':old['id'], 'expectedRevision':1,'text':'late'})
        answer = await action('answer', {'id':replacement['id'], 'expectedRevision':1,'text':'Blue'}, 'new-answer')
        assert answer['delivery']['status'] == 'rejected' and answer['answer']['text'] == 'Blue'
        other = await action('create', question(), 'another-question')
        await action('cancel', {'id':other['id'],'expectedRevision':1})
        with pytest.raises(ValueError, match='explicit answer'): owner.dependency(SID, other['id'])
        assert len(calls) == 1
    finally: owner.close()


async def test_cold_catalog_pages_and_restart_recovery_use_indexes(tmp_path):
    owner, _, _, _, _, host, invalidate = setup(tmp_path)
    with owner.db:
        for number in range(25000):
            row = owner.new(SID if number<75 else 'ahp-session:/cold-'+str(number), question(), 'ui', 'client')
            row.update(status='cancelled',createdAt=number)
            owner.put(row)
        active = owner.new(SID, question(), 'ui', 'client')
        active.update(status='answered', delivery={'status':'sending','inputId':'retained'})
        owner.put(active)
    plan = owner.db.execute('EXPLAIN QUERY PLAN SELECT value FROM questions WHERE session=? ORDER BY created,id LIMIT 6',(SID,)).fetchall()
    assert any('questions_scope' in row['detail'] for row in plan)
    steps=[];owner.db.set_progress_handler(lambda:steps.append(1) and 0, 100)
    first=owner.page(SID,{'limit':5});second=owner.page(SID,{'limit':5,'cursor':first['nextCursor']})
    assert len(first['items']) == len(second['items']) == 5 and len(steps)<20
    assert len({row['id'] for row in first['items']+second['items']}) == 10
    with pytest.raises(ValueError,match='scope'): owner.page('ahp-session:/different',{'cursor':first['nextCursor']})
    owner.close();owner=Questions(tmp_path/'questions.sqlite3',host,invalidate)
    try: assert owner.get(SID,active['id'])['delivery']['status']=='unknown'
    finally: owner.close()


async def test_racing_answers_have_one_durable_winner(tmp_path):
    owner, action, calls, _, _, _, _ = setup(tmp_path)
    try:
        row=await action('create',question());args={'id':row['id'],'expectedRevision':1,'text':'Blue'}
        results=await asyncio.gather(action('answer',args,'answer-a'),action('answer',args,'answer-b'),return_exceptions=True)
        assert sum(isinstance(item,dict) for item in results)==1
        assert sum(isinstance(item,ValueError) for item in results)==1 and len(calls)==1
    finally: owner.close()


async def test_closed_pages_are_newest_first_and_unconfirmed_survives_cold_history(tmp_path):
    async def lost(_): raise RuntimeError('Unknown delivery')
    owner, action, calls, _, _, host, invalidate = setup(tmp_path, lost)
    try:
        with owner.db:
            for number in range(10000):
                row = owner.new(SID, question(), 'ui', 'client')
                row.update(status='answered', createdAt=number-10000, delivery={'status':'accepted'})
                owner.put(row)
        current = await action('create', question())
        answer = await action('answer', {'id':current['id'], 'expectedRevision':1, 'text':'Keep this answer'})
        steps=[];owner.db.set_progress_handler(lambda: steps.append(1) and 0, 100)
        attention=owner.page(SID, {'status':'unconfirmed','limit':5})
        assert [q['id'] for q in attention['items']] == [current['id']]
        assert len(steps)<20
        first=owner.page(SID, {'status':'closed','limit':5})
        assert first['items'][0]['id']==current['id']
        second=owner.page(SID, {'status':'closed','limit':5,'cursor':first['nextCursor']})
        assert len({q['id'] for q in first['items']+second['items']})==10
        assert first['items'][-1]['createdAt']>second['items'][0]['createdAt']
        with pytest.raises(ValueError,match='scope'):
            owner.page(SID, {'status':'unconfirmed','cursor':first['nextCursor']})
        assert owner.page('ahp-session:/other', {'status':'unconfirmed'})['items']==[]
    finally: owner.close()
    owner=Questions(tmp_path/'questions.sqlite3',host,invalidate)
    try:
        assert owner.page(SID, {'status':'unconfirmed'})['items']==[answer]
        assert len(calls)==1
    finally: owner.close()


async def test_large_complete_questions_use_byte_bound_cursor_without_skipping(tmp_path):
    owner, _, _, _, _, _, _ = setup(tmp_path)
    try:
        with owner.db:
            for number in range(20):
                row=owner.new(SID,question(),'ui','client')
                row.update(createdAt=number,status='cancelled',prompt='🌊'*8000)
                owner.put(row)
        seen=[];cursor=None
        while True:
            page=owner.page(SID,{'limit':50,**({'cursor':cursor} if cursor else {})})
            assert len(json.dumps(page,ensure_ascii=False).encode())<600*1024
            assert all(len(row['prompt'])==8000 for row in page['items'])
            seen.extend(row['id'] for row in page['items']);cursor=page['nextCursor']
            if not cursor:break
        assert len(seen)==len(set(seen))==20
    finally:owner.close()


def test_attention_reads_only_selected_indexed_ids_with_explicit_overflow(tmp_path):
    owner, *_ = setup(tmp_path)
    try:
        # Invalid body JSON demonstrates that this read never hydrates records.
        with owner.db:
            owner.db.executemany('INSERT INTO questions VALUES(?,?,?,?,?,?)',
                [(f'outside-{i}', 'ahp-session:/other', 'pending', i, None, 'not JSON') for i in range(25000)]
                + [(f'own-{i:03}', SID, 'pending', i, None, 'not JSON') for i in range(65)]
                + [('answered', SID, 'answered', 1, None, 'not JSON')])
        result = owner.attention(SID)
        assert result['questionIds'] == [f'own-{i:03}' for i in range(64)]
        assert not result['questionsComplete'] and result['attentionCoverage']['questionsTruncated']
        plan = ' '.join(str(tuple(row)) for row in owner.db.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM questions WHERE session=? AND status='pending' ORDER BY created,id LIMIT 65", (SID,)))
        assert 'COVERING INDEX questions_status' in plan and 'SCAN questions' not in plan
        with owner.db: owner.db.execute('DELETE FROM questions WHERE id=?', ('own-064',))
        assert owner.attention(SID)['questionsComplete']
        assert owner.attention('ahp-session:/absent')['questionIds'] == []
    finally: owner.close()
