import copy
import hashlib
import json
import sqlite3

from filelock import FileLock, Timeout
import pytest

from amplifier_unified_operations.adopt_questions import adopt_questions
from amplifier_unified_operations.migrate import import_page
from amplifier_unified_operations.questions import Questions

SID = 'ahp-session:/migrated-questions'


def fixture(tmp_path):
    source = tmp_path / 'old.sqlite3'
    destination = tmp_path / 'owner'
    base = {'sessionId': 'old-chat', 'revision': 1, 'createdAt': 100, 'updatedAt': 100,
            'prompt': 'Which color?', 'options': [{'id': 'blue', 'label': 'Blue'}],
            'allowFreeText': True, 'required': True, 'dependency': 'Select a color',
            'createdBy': {'origin': 'agent', 'clientId': 'original'}, 'answer': None, 'delivery': None}
    rows = []
    for status in ['pending', 'answered', 'cancelled', 'superseded']:
        row = {**copy.deepcopy(base), 'id': 'original-' + status, 'status': status}
        if status == 'answered':
            row.update(revision=2, answer={'text': 'Blue', 'optionId': 'blue', 'questionRevision': 1,
                       'answeredAt': 101, 'provenance': {'origin': 'ui', 'clientId': 'old-user'}},
                       delivery={'status': 'sending', 'inputId': 'old-answer-input'})
        if status == 'superseded': row.update(supersededBy='original-pending', revision=2)
        rows.append(row)
    with sqlite3.connect(source) as db:
        db.execute('CREATE TABLE questions(id TEXT PRIMARY KEY,session_id TEXT,created_at REAL,value TEXT)')
        db.executemany('INSERT INTO questions VALUES(?,?,?,?)', [(r['id'], r['sessionId'], r['createdAt'], json.dumps(r)) for r in rows])
    before = source.read_bytes()
    imported = import_page(source, destination, source_session='old-chat', session=SID, kind='question')
    args = {'session': SID, 'command_id': 'adopt-original-questions',
            'records': [{'id': r['id'], 'sha256': r['sha256']} for r in imported['items']],
            'retirement_review_digest': 'a' * 64}
    return source, destination, args, before, rows


async def test_original_ids_and_answers_survive_restart_and_only_new_explicit_answer_is_sent(tmp_path):
    source, destination, args, before, rows = fixture(tmp_path)
    result = adopt_questions(destination, **args)
    assert result['pending'] == 1 and result['oldAnswersDelivered'] == 0 and not result['workReplayed']
    assert adopt_questions(destination, **args)['previouslyAdopted']
    calls = []
    async def host(method, value):
        assert method == 'submitQuestionAnswer'
        calls.append(value)
        question_owner.authorize_delivery(SID, {**value['input'], 'inputId': value['input']['commandId']})
        return {'accepted': True}
    async def changed(_): pass
    question_owner = Questions(destination / 'questions.sqlite3', host, changed)
    try:
        page = question_owner.page(SID, {'limit': 50})['items']
        assert {r['id'] for r in page} == {r['id'] for r in rows}
        old = question_owner.get(SID, 'original-answered')
        assert old['delivery']['status'] == 'unknown' and old['answer'] == rows[1]['answer']
        assert question_owner.dependency(SID, 'original-answered')['text'] == 'Blue'
        with pytest.raises(ValueError, match='retained legacy'):
            question_owner.authorize_delivery(SID, {'questionId': old['id']})
        with pytest.raises(ValueError, match='explicit answer'):
            question_owner.dependency(SID, 'original-pending')
        answer_args = {'sessionId': SID, 'id': 'original-pending', 'expectedRevision': 1, 'optionId': 'blue'}
        answered = await question_owner.dispatch('question.answer', answer_args, 'ui', 'new-client', 'one-answer')
        assert answered['delivery']['status'] == 'accepted' and len(calls) == 1
        assert (await question_owner.dispatch('question.answer', answer_args, 'ui', 'new-client', 'one-answer')) == answered
    finally: question_owner.close()
    # Retrying the migration never overwrites the newly answered question.
    assert adopt_questions(destination, **args)['previouslyAdopted']
    question_owner = Questions(destination / 'questions.sqlite3', host, changed)
    try:
        assert question_owner.get(SID, 'original-pending')['status'] == 'answered'
        assert len(calls) == 1
    finally: question_owner.close()
    assert source.read_bytes() == before


@pytest.mark.parametrize('fault', ['digest', 'scope', 'duplicate', 'bad-command', 'review', 'content'])
def test_invalid_adoption_cannot_partially_copy_authority(tmp_path, fault):
    source, destination, args, before, _ = fixture(tmp_path)
    if fault == 'digest': args['records'][-1]['sha256'] = '0' * 64
    if fault == 'scope': args['session'] = 'ahp-session:/someone-else'
    if fault == 'duplicate': args['records'].append(args['records'][0])
    if fault == 'bad-command': args['command_id'] = 'c' * 201
    if fault == 'review': args['retirement_review_digest'] = ''
    if fault == 'content':
        with sqlite3.connect(destination / 'legacy.sqlite3') as db:
            db.execute("UPDATE records SET original=json_set(original,'$.revision',0) WHERE id=?", (args['records'][-1]['id'],))
    with pytest.raises(ValueError): adopt_questions(destination, **args)
    assert not (destination / 'questions.sqlite3').exists()
    assert source.read_bytes() == before


def test_live_owner_and_conflicting_ids_or_commands_refuse_without_overwrite(tmp_path):
    _, destination, args, _, _ = fixture(tmp_path)
    with FileLock(str(destination / 'owner.lock')):
        with pytest.raises(Timeout): adopt_questions(destination, **args)
    adopt_questions(destination, **args)
    with pytest.raises(ValueError, match='different arguments'):
        adopt_questions(destination, **{**args, 'retirement_review_digest': 'b' * 64})
    with pytest.raises(ValueError, match='identity already exists'):
        adopt_questions(destination, **{**args, 'command_id': 'second-import'})
    with sqlite3.connect(destination / 'questions.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM questions').fetchone()[0] == 4
        assert db.execute('SELECT COUNT(*) FROM question_commands').fetchone()[0] == 1


def test_selected_import_leaves_unrelated_destination_delivery_unchanged(tmp_path):
    _, destination, args, _, rows = fixture(tmp_path)
    async def forbidden(*_):
        raise AssertionError('Offline adoption must not admit work')
    owner = Questions(destination / 'questions.sqlite3', forbidden, forbidden)
    unrelated = {**rows[1], 'id': 'unrelated-answer', 'sessionId': 'ahp-session:/another-chat'}
    with owner.db:
        owner.put(unrelated)
    original = owner.db.execute('SELECT value FROM questions WHERE id=?', (unrelated['id'],)).fetchone()[0]
    owner.close()
    adopt_questions(destination, **args)
    with sqlite3.connect(destination / 'questions.sqlite3') as db:
        assert db.execute('SELECT value FROM questions WHERE id=?', (unrelated['id'],)).fetchone()[0] == original
