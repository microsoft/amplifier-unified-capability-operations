from contextlib import closing
import hashlib
import json
import sqlite3

from amplifier_scheduling import ScheduleStore
from filelock import FileLock, Timeout
import pytest

from amplifier_unified_operations.adopt_schedules import adopt_schedule
from amplifier_unified_operations.history import HistoricalRecords
from amplifier_unified_operations.scheduler import Scheduler
from test_owner import Host, SID


def fixture(tmp_path, status='active', phases=('completed', 'submitting')):
    source = tmp_path / 'original.sqlite3'
    destination = tmp_path / 'candidate'
    schedule = {'id': 'original-schedule', 'sessionId': 'old-chat', 'revision': 3,
                'createdAt': 10, 'updatedAt': 20, 'status': status, 'nextDue': 110,
                'prompt': 'Check the saved project', 'kind': 'monitor', 'missedRunPolicy': 'latest',
                'notificationPolicy': 'changes', 'destination': 'same_task', 'taskId': 'task',
                'taskRevision': 1, 'interruptionRevision': 0, 'executionRevision': 0,
                'authorization': {'origin': 'ui', 'at': 10},
                'spec': {'kind': 'interval', 'timezone': 'UTC', 'startAt': '1970-01-01T00:01:50+00:00', 'intervalSeconds': 60}}
    store = ScheduleStore(source)
    runs = []
    with store.transaction():
        store.put(schedule)
        for i, phase in enumerate(phases):
            row = {'id': 'original-run-' + str(i), 'scheduleId': schedule['id'], 'sessionId': 'old-chat',
                   'inputId': 'old-input-' + str(i), 'dueAt': 30 + i, 'phase': phase,
                   'revision': 2, 'createdAt': 30 + i, 'updatedAt': 40 + i, 'owner': 'retired-owner'}
            runs.append(row)
            store.put_run(row)
    store.close()
    before = source.read_bytes()
    args = {'source_sha256': hashlib.sha256(before).hexdigest(), 'source_session': 'old-chat',
            'session_mapping': {'old-chat': SID}, 'schedule_id': schedule['id'],
            'command_id': 'adopt-old-schedule', 'retirement_review_digest': 'a' * 64}
    return source, destination, args, before, schedule, runs


async def test_uncertain_history_blocks_resume_until_explicit_reconciliation_then_only_future_input_runs(tmp_path):
    source, destination, args, before, original, old_runs = fixture(tmp_path)
    result = adopt_schedule(source, destination, **args)
    assert result['retainedRuns'] == 2 and result['unknownRuns'] == 1
    assert not result['activated'] and not result['workReplayed']
    assert result['schedule']['status'] == 'needs_review' and result['schedule']['nextDue'] is None
    host = Host()
    async def changed(_): pass
    now = [200]
    owner = Scheduler(destination / 'schedules.sqlite3', host, changed, clock=lambda: now[0])
    try:
        await owner.tick()
        assert host.calls == [] and owner.store.recovered == []
        assert owner.store.run(SID, old_runs[0]['id'])['phase'] == 'completed'
        run = owner.store.run(SID, old_runs[1]['id'])
        assert run['phase'] == 'unknown' and run['inputId'] == old_runs[1]['inputId']
        schedule = owner.store.get(SID, original['id'])
        config = {'sessionId': SID, **owner.configuration(schedule)}
        preview = await owner.dispatch('schedule.preview', config, 'ui', 'preview')
        with pytest.raises(ValueError, match='uncertain run'):
            await owner.dispatch('schedule.resume', {'sessionId': SID, 'id': schedule['id'], 'expectedRevision': schedule['revision'], 'previewHash': preview['previewHash']}, 'ui', 'resume-refused')
        await owner.dispatch('schedule.reconcile', {'sessionId': SID, 'runId': run['id'], 'expectedRevision': run['revision'],
                             'resolution': 'abandoned', 'evidence': 'Inspected original effects; no retry requested'}, 'ui', 'reconcile')
        resumed = (await owner.dispatch('schedule.resume', {'sessionId': SID, 'id': schedule['id'], 'expectedRevision': schedule['revision'], 'previewHash': preview['previewHash']}, 'ui', 'resume'))['schedule']
        assert resumed['nextDue'] > now[0]
        assert not any(method == 'submitScheduled' for method, _ in host.calls)
        now[0] = resumed['nextDue']
        await owner.tick()
        import asyncio
        await asyncio.gather(*list(owner.pending))
        submissions = [value for method, value in host.calls if method == 'submitScheduled']
        assert len(submissions) == 1
        assert submissions[0]['input']['commandId'] not in {row['inputId'] for row in old_runs}
    finally:
        await owner.close()
    # A lost migration acknowledgement reads the old receipt, not a new activation.
    assert adopt_schedule(source, destination, **args)['previouslyAdopted']
    store = ScheduleStore(destination / 'schedules.sqlite3')
    assert store.get(SID, original['id'])['status'] == 'active'
    assert len(store.runs(SID)) == 3
    store.close()
    history = HistoricalRecords(destination / 'legacy.sqlite3')
    values = [history.read(SID, row['id'])['record'] for row in history.page(SID, {})['items']]
    assert values == [original, *old_runs]
    history.close()
    assert source.read_bytes() == before


@pytest.mark.parametrize('status', ['active', 'needs_review', 'paused', 'completed', 'cancelled'])
def test_all_schedule_states_are_retained_without_activation(tmp_path, status):
    source, destination, args, before, original, _ = fixture(tmp_path, status, ())
    result = adopt_schedule(source, destination, **args)
    assert result['schedule']['status'] == ('needs_review' if status in {'active', 'needs_review'} else status)
    assert result['schedule']['nextDue'] is None
    assert result['schedule']['revision'] > original['revision']
    assert not result['activated'] and source.read_bytes() == before


@pytest.mark.parametrize('fault', ['digest', 'sidecar', 'scope', 'invalid-state', 'mismatched-column', 'missing-destination', 'invalid-record'])
def test_invalid_or_incomplete_source_creates_no_schedule_authority(tmp_path, fault):
    source, destination, args, _, _, _ = fixture(tmp_path)
    if fault == 'digest': args['source_sha256'] = '0' * 64
    elif fault == 'sidecar': source.with_name(source.name + '-wal').write_bytes(b'uncertain-writes')
    elif fault == 'scope': args['source_session'] = 'another'; args['session_mapping'] = {'another': SID}
    else:
        with closing(sqlite3.connect(source)) as db, db:
            if fault == 'invalid-state': db.execute("UPDATE schedules SET value=json_set(value,'$.status','bad')")
            if fault == 'mismatched-column': db.execute("UPDATE schedule_runs SET phase='failed' WHERE id='original-run-0'")
            if fault == 'missing-destination': db.execute("UPDATE schedule_runs SET value=json_set(value,'$.destinationSessionId','old-child')")
            if fault == 'invalid-record': db.execute("UPDATE schedule_runs SET value=json_set(value,'$.revision',0) WHERE id='original-run-1'")
        args['source_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    before = source.read_bytes()
    with pytest.raises(ValueError): adopt_schedule(source, destination, **args)
    assert not (destination / 'schedules.sqlite3').exists()
    assert source.read_bytes() == before


def test_linked_destination_mapping_conflicts_and_live_owner_refuse(tmp_path):
    source, destination, args, _, _, _ = fixture(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("UPDATE schedule_runs SET value=json_set(value,'$.destinationSessionId','old-child') WHERE id='original-run-0'")
    args['source_sha256'] = hashlib.sha256(source.read_bytes()).hexdigest()
    args['session_mapping']['old-child'] = 'ahp-session:/retained-child'
    destination.mkdir()
    with FileLock(str(destination / 'owner.lock')):
        with pytest.raises(Timeout): adopt_schedule(source, destination, **args)
    adopt_schedule(source, destination, **args)
    store = ScheduleStore(destination / 'schedules.sqlite3')
    assert store.incoming('ahp-session:/retained-child')[0]['id'] == 'original-run-0'
    store.close()
    with pytest.raises(ValueError, match='different arguments'):
        adopt_schedule(source, destination, **{**args, 'retirement_review_digest': 'b' * 64})
    with pytest.raises(ValueError, match='identity already exists'):
        adopt_schedule(source, destination, **{**args, 'command_id': 'another-import'})
    store = ScheduleStore(destination / 'schedules.sqlite3')
    assert len(store.list(SID)) == 1 and len(store.runs(SID)) == 2
    store.close()


def test_interrupted_copy_retains_only_history_until_atomic_retry_succeeds(tmp_path, monkeypatch):
    source, destination, args, before, original, old_runs = fixture(tmp_path)
    put = ScheduleStore.put_run
    def interrupted(self, row):
        put(self, row)
        if row['id'] == old_runs[-1]['id']:
            raise RuntimeError('Fixture interruption after copied run')
    with monkeypatch.context() as patch:
        patch.setattr(ScheduleStore, 'put_run', interrupted)
        with pytest.raises(RuntimeError, match='Fixture interruption'):
            adopt_schedule(source, destination, **args)
    store = ScheduleStore(destination / 'schedules.sqlite3')
    assert store.list(SID) == [] and store.runs(SID) == []
    assert store.command_receipt(SID, args['command_id']) is None
    store.close()
    history = HistoricalRecords(destination / 'legacy.sqlite3')
    assert len(history.page(SID, {})['items']) == 3
    history.close()
    assert adopt_schedule(source, destination, **args)['retainedRuns'] == 2
    assert source.read_bytes() == before


@pytest.mark.parametrize('phase', ['creating', 'claimed', 'submitting', 'accepted', 'running', 'unknown'])
def test_every_potentially_admitted_phase_remains_unknown(tmp_path, phase):
    source, destination, args, _, _, old_runs = fixture(tmp_path, phases=(phase,))
    result = adopt_schedule(source, destination, **args)
    assert result['unknownRuns'] == 1
    store = ScheduleStore(destination / 'schedules.sqlite3')
    assert store.run(SID, old_runs[0]['id'])['phase'] == 'unknown'
    assert store.acquire(9999) and store.recovered == []
    assert store.due(9999) == []
    store.close()
