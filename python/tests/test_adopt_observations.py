from contextlib import closing
import hashlib
import json
import sqlite3

from filelock import FileLock, Timeout
import pytest

from amplifier_unified_operations.adopt_observations import adopt_observation
from amplifier_unified_operations.history import HistoricalRecords
from amplifier_unified_operations.observation_store import Store, digest
from test_observations import setup, SID


def fixture(tmp_path, *, status='active', handoff=None):
    source = tmp_path / 'original.sqlite3'
    destination = tmp_path / 'candidate'
    watch = {'id': 'original-watch', 'requestId': 'original-request', 'sessionId': 'old-chat',
        'status': 'ended' if handoff else status, 'revision': 2, 'createdAt': 10, 'updatedAt': 20,
        'sequence': 1, 'nextDue': 25, 'expiresAt': 130, 'observerId': 'observer',
        'target': {'record': 'one'}, 'scope': {}, 'sourceId': 'source', 'args': {},
        'intervalSeconds': 15, 'durationSeconds': 120, 'readTimeoutSeconds': 3,
        'taskId': 'task', 'taskRevision': 1, 'nativeSessionId': 'old-native',
        'executionRevision': 0, 'interruptionRevision': 0,
        'presentationGrant': {'clientId': 'old-client'}, 'authorization': {'origin': 'ui', 'clientId': 'old-client'}}
    run = {'id': 'observation:original-watch:0', 'watchId': watch['id'], 'sessionId': 'old-chat',
           'watchRevision': 2, 'phase': 'reading', 'createdAt': 20, 'updatedAt': 20, 'owner': 'retired'}
    with closing(sqlite3.connect(source)) as db, db:
        db.executescript('CREATE TABLE observation_objects(kind TEXT,id TEXT,body TEXT,PRIMARY KEY(kind,id));CREATE TABLE observation_commands(id TEXT PRIMARY KEY,intent TEXT,result TEXT);CREATE TABLE observation_terminals(watch TEXT PRIMARY KEY,semantic_key TEXT,input_id TEXT UNIQUE);')
        rows = [('run', run), ('observer', {'id': 'observer', 'requestArgument': 'observation', 'qualifiedAt': 10})]
        if handoff:
            watch['terminal'] = 'actionable'
            item = {'id': 'observation:original-watch:result', 'inputId': 'observation:original-watch:result',
                'watchId': watch['id'], 'sessionId': 'old-chat', 'phase': handoff, 'presentationPhase': 'claimed',
                'outcome': {'status': 'actionable', 'summary': 'Original verified result'}, 'createdAt': 22, 'updatedAt': 22}
            rows.append(('outbox', item))
            db.execute('INSERT INTO observation_terminals VALUES(?,?,?)', (watch['id'], 'original-semantic-key', item['inputId']))
        for kind, row in [('watch', watch), *rows]:
            db.execute('INSERT INTO observation_objects VALUES(?,?,?)', (kind, row['id'], json.dumps(row)))
        db.execute('INSERT INTO observation_commands VALUES(?,?,?)', (digest(['old-chat','observation.create','original-request']), 'c' * 64, json.dumps({'id': watch['id']})))
    before = source.read_bytes()
    args = {'source_sha256': hashlib.sha256(before).hexdigest(), 'source_session': 'old-chat',
            'session': SID, 'watch_id': watch['id'], 'command_id': 'adopt-watch', 'retirement_review_digest': 'a' * 64}
    return source, destination, args, before, watch, run


async def test_old_watch_stays_quiet_until_source_qualified_and_explicitly_reviewed(tmp_path):
    source, destination, args, before, original, run = fixture(tmp_path)
    result = adopt_observation(source, destination, **args)
    assert result['retainedRuns'] == 1 and result['retainedRecords'] == 4
    assert result['activated'] is result['observerQualified'] is result['workReplayed'] is False
    store = Store(destination / 'observations.sqlite3')
    assert store.page('observer')['items'] == []
    assert store.get('run', run['id'])['phase'] == 'abandoned_read'
    store.close()
    owner, action, _, tick, config, _, _, _, _, _, calls, _, _, _ = await setup(destination)
    calls.clear()
    try:
        await tick()
        assert not calls
        row = (await action('read', {'id': original['id']}))['watch']
        assert row['status'] == 'needs_review' and row['nextDue'] is None and 'presentationGrant' not in row
        saved = await action('request', {'requestId': 'original-request'})
        assert saved['watch']['id'] == original['id'] and saved['receipt']['status'] == 'legacy_observed'
        assert 'appliedRevision' not in saved['receipt']
        with pytest.raises(ValueError, match='cannot be replayed'):
            await action('create', {**config, 'requestId': 'original-request', 'previewHash': 'unused'})
        preview = await action('preview', config)
        resumed = (await action('resume', {**config, 'id': row['id'], 'expectedRevision': row['revision'],
            'previewHash': preview['previewHash'], 'requestId': 'fresh-human-review'}))['watch']
        assert resumed['configurationHash'] == 'config' and resumed['nativeSessionId'] == 'native'
        await tick()
        assert owner.store.get('run', run['id'])['phase'] == 'abandoned_read'
        assert owner.store.get('run', 'observation:original-watch:1')['phase'] == 'pending'
        assert not any(method == 'submitObservation' for method, _ in calls)
    finally: await owner.close()
    assert adopt_observation(source, destination, **args)['previouslyAdopted']
    store = Store(destination / 'observations.sqlite3')
    assert store.get('watch', original['id'])['status'] == 'active'
    store.close()
    history = HistoricalRecords(destination / 'legacy.sqlite3')
    rows = [history.read(SID, row['id']) for row in history.page(SID, {})['items']]
    assert {r['kind'] for r in rows} == {'watch', 'run', 'observer', 'watch-command'}
    assert next(r['record'] for r in rows if r['kind'] == 'watch') == original
    assert all(r['executionAuthority'] is False for r in rows)
    history.close()
    assert source.read_bytes() == before


@pytest.mark.parametrize('phase,expected', [('pending', 'suppressed'), ('waiting_worker', 'suppressed'),
    ('submitting', 'unknown'), ('accepted', 'unknown'), ('unknown', 'unknown'),
    ('suppressed', 'suppressed'), ('completed', 'completed'), ('failed', 'failed')])
async def test_original_handoff_is_never_sent_or_rearmed(tmp_path, phase, expected):
    source, destination, args, before, watch, _ = fixture(tmp_path, handoff=phase)
    result = adopt_observation(source, destination, **args)
    assert result['retainedHandoffs'] == 1
    owner, action, _, tick, config, _, _, _, _, _, calls, _, _, _ = await setup(destination)
    calls.clear()
    try:
        await tick();assert not calls
        report = await action('report', {'id': watch['id']})
        assert report['handoffs'][0]['phase'] == expected
        assert report['handoffs'][0]['outcome']['summary'] == 'Original verified result'
        assert report['handoffs'][0]['presentationPhase'] == 'unknown'
        assert owner.store.db.execute('SELECT input FROM terminals WHERE watch=?', (watch['id'],)).fetchone()[0] == report['handoffs'][0]['inputId']
        with pytest.raises(ValueError, match='nonterminal'):
            await action('resume', {**config, 'id': watch['id'], 'expectedRevision': report['watch']['revision'],
                'previewHash': 'unused', 'requestId': 'cannot-rearm'})
    finally: await owner.close()
    assert source.read_bytes() == before


@pytest.mark.parametrize('status', ['paused', 'needs_review', 'cancelled'])
def test_inactive_status_is_preserved(tmp_path, status):
    source, destination, args, before, watch, _ = fixture(tmp_path, status=status)
    adopt_observation(source, destination, **args)
    store = Store(destination / 'observations.sqlite3')
    assert store.get('watch', watch['id'])['status'] == status
    assert store.due(999999) == store.pending(999999) == []
    store.close();assert source.read_bytes() == before


@pytest.mark.parametrize('fault', ['digest', 'sidecar', 'scope', 'bad-state', 'bad-json', 'missing-terminal', 'wrong-input', 'sequence', 'source-changed'])
def test_incomplete_or_invalid_history_cannot_create_authority(tmp_path, monkeypatch, fault):
    source, destination, args, _, _, _ = fixture(tmp_path, handoff='submitting')
    if fault == 'digest': args['source_sha256'] = '0' * 64
    elif fault == 'sidecar': source.with_name(source.name + '-wal').write_bytes(b'uncertain')
    elif fault == 'scope': args['source_session'] = 'another'
    elif fault == 'source-changed':
        from amplifier_unified_operations import adopt_observations as module
        original = module._read
        def changing(*a):
            result = original(*a)
            with source.open('ab') as f: f.write(b'changed')
            return result
        monkeypatch.setattr(module, '_read', changing)
    else:
        with closing(sqlite3.connect(source)) as db, db:
            if fault == 'bad-state': db.execute("UPDATE observation_objects SET body=json_set(body,'$.status','invalid') WHERE kind='watch'")
            if fault == 'bad-json': db.execute("UPDATE observation_objects SET body='invalid' WHERE kind='run'")
            if fault == 'missing-terminal': db.execute('DELETE FROM observation_terminals')
            if fault == 'wrong-input': db.execute("UPDATE observation_terminals SET input_id='unrelated'")
            if fault == 'sequence': db.execute("UPDATE observation_objects SET body=json_set(body,'$.sequence',0) WHERE kind='watch'")
        args['source_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(ValueError): adopt_observation(source, destination, **args)
    assert not (destination / 'observations.sqlite3').exists()


def test_conflicts_and_live_owner_preserve_both_sources(tmp_path):
    source, destination, args, before, watch, _ = fixture(tmp_path)
    destination.mkdir()
    with FileLock(str(destination / 'owner.lock')):
        with pytest.raises(Timeout): adopt_observation(source, destination, **args)
    adopt_observation(source, destination, **args)
    with pytest.raises(ValueError, match='different arguments'):
        adopt_observation(source, destination, **{**args, 'retirement_review_digest': 'd' * 64})
    with pytest.raises(ValueError, match='identity already exists'):
        adopt_observation(source, destination, **{**args, 'command_id': 'another'})
    assert source.read_bytes() == before


def test_failed_atomic_copy_leaves_only_read_only_evidence_then_retry_can_finish(tmp_path, monkeypatch):
    source, destination, args, before, watch, run = fixture(tmp_path, handoff='accepted')
    put = Store.put
    def interrupted(self, kind, row):
        put(self, kind, row)
        if kind == 'outbox': raise RuntimeError('Fixture interruption')
    with monkeypatch.context() as patch:
        patch.setattr(Store, 'put', interrupted)
        with pytest.raises(RuntimeError): adopt_observation(source, destination, **args)
    store = Store(destination / 'observations.sqlite3')
    assert store.page('watch')['items'] == store.page('run')['items'] == store.page('outbox')['items'] == []
    assert store.db.execute('SELECT count(*) FROM terminals').fetchone()[0] == 0
    store.close()
    assert adopt_observation(source, destination, **args)['retainedHandoffs'] == 1
    assert source.read_bytes() == before
