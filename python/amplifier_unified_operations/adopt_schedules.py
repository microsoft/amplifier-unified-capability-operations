"""Offline adoption of one reviewed legacy schedule and its complete run history.

The original source must stay retired. Only a sealed, sidecar-free SQLite image
is accepted; a digest records an operator review, not independent stop evidence.
Future runs require the ordinary current-owner preview and explicit activation.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from amplifier_scheduling import ScheduleStore, normalize
from amplifier_scheduling.store import ACTIVE_RUNS, TERMINAL_RUNS, fingerprint
from filelock import FileLock
from jsonschema import Draft202012Validator

from .adopt_questions import _digest, _schema, _time
from .history import HistoricalRecords
from .schedules_schemas import definitions

CONFIG = definitions(_schema, lambda maximum: {'type': 'string', 'maxLength': maximum})['schedule.preview'][1]
CONFIG_FIELDS = set(CONFIG['properties'])
REASON = 'Imported schedule requires review of its current conversation and future timing. Past work is never replayed.'


def _identity(value):
    return isinstance(value, str) and 1 <= len(value) <= 200 and '\0' not in value


def _source_digest(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * 1024 * 1024:
        raise ValueError('A regular sealed source image of at most 64 MiB is required')
    if any(os.path.lexists(str(path) + suffix) for suffix in ('-wal', '-shm', '-journal')):
        raise ValueError('Source image must be sealed without SQLite sidecars')
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return digest


def _read_records(path, source_session, schedule_id):
    db = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        schedules = db.execute('SELECT rowid,id,session_id,length(CAST(value AS BLOB)) FROM schedules WHERE id=?', (schedule_id,)).fetchall()
        if len(schedules) != 1 or schedules[0][2] != source_session:
            raise ValueError('Selected schedule does not belong to the mapped source conversation')
        runs = db.execute('SELECT rowid,id,session_id,length(CAST(value AS BLOB)),schedule_id,due,phase FROM schedule_runs WHERE schedule_id=? ORDER BY due,id LIMIT 10001', (schedule_id,)).fetchall()
        if len(runs) > 10000:
            raise ValueError('Selected schedule has more than 10000 runs; retain source for a reviewed larger migration')
        metadata = [('schedule', 'schedules', schedules[0]), *[('schedule-run', 'schedule_runs', row) for row in runs]]
        if any(type(row[3]) is not int or not 1 <= row[3] <= 128 * 1024 for _, _, row in metadata) or sum(row[3] for _, _, row in metadata) > 32 * 1024 * 1024:
            raise ValueError('Selected schedule history exceeds the bounded complete representation')
        originals = []
        for kind, table, row in metadata:
            raw = db.execute('SELECT value FROM ' + table + ' WHERE rowid=?', (row[0],)).fetchone()[0]
            value = json.loads(raw)
            if not isinstance(value, dict) or (value.get('id'), value.get('sessionId')) != (row[1], source_session) or row[2] != source_session:
                raise ValueError('Legacy schedule identity or conversation does not match its stored record')
            if kind == 'schedule-run' and (value.get('scheduleId'), value.get('dueAt'), value.get('phase')) != row[4:]:
                raise ValueError('Legacy run identity or phase does not match its stored record')
            originals.append({'kind': kind, 'row': row[0], 'raw': raw, 'value': value, 'sha256': hashlib.sha256(raw.encode()).hexdigest()})
        return originals
    finally:
        db.close()


def _convert(originals, mapping, source_digest):
    converted = []
    for source in originals:
        row = copy.deepcopy(source['value'])
        if (not _identity(row['id']) or type(row.get('revision')) is not int or row['revision'] < 1
                or not _time(row.get('createdAt')) or not _time(row.get('updatedAt'))):
            raise ValueError('Unsupported legacy schedule or run revision/timestamps')
        row['sessionId'] = mapping[row['sessionId']]
        if source['kind'] == 'schedule':
            config = {key: row[key] for key in CONFIG_FIELDS if key in row}
            Draft202012Validator(CONFIG).validate(config)
            row['spec'] = normalize(row['spec'])
            if row.get('status') not in {'active', 'paused', 'needs_review', 'completed', 'cancelled'} or not row['prompt'].strip():
                raise ValueError('Unsupported legacy schedule state or instructions')
            if row['status'] in {'active', 'needs_review'}:
                row.update(status='needs_review', reviewReason=REASON)
            row.update(nextDue=None, revision=row['revision'] + 1)
            # Old execution configuration is evidence, never new execution authority.
            row.pop('newTaskConfiguration', None)
        else:
            phase = row.get('phase')
            if phase not in {*ACTIVE_RUNS, *TERMINAL_RUNS} or not _time(row.get('dueAt')) or not _identity(row.get('inputId')):
                raise ValueError('Unsupported legacy run state, due time or input identity')
            target = row.get('destinationSessionId')
            if target is not None:
                if target not in mapping:
                    raise ValueError('Every linked run destination needs an explicit reviewed conversation mapping')
                row['destinationSessionId'] = mapping[target]
            if phase in ACTIVE_RUNS and phase != 'unknown':
                row.update(phase='unknown', revision=row['revision'] + 1,
                           detail='Original scheduled input may have been admitted. Inspect and reconcile; never replay.')
            row.pop('owner', None)
        row['legacyImport'] = {'sourceSha256': source_digest, 'recordSha256': source['sha256'],
                               'sourceSession': source['value']['sessionId'], 'originalPhase': source['value'].get('phase'),
                               'originalStatus': source['value'].get('status')}
        if len(json.dumps(row, allow_nan=False).encode()) > 128 * 1024:
            raise ValueError('Migrated schedule record exceeds the complete record bound')
        converted.append(row)
    return converted


def _retain(history, originals, source_id, source_session, session):
    with history.db:
        previous = history.db.execute('SELECT session FROM mappings WHERE source=? AND original=?', (source_id, source_session)).fetchone()
        if previous and previous[0] != session:
            raise ValueError('Source conversation already maps to another destination')
        history.db.execute('INSERT OR IGNORE INTO mappings VALUES(?,?,?)', (source_id, source_session, session))
        for row in originals:
            key = hashlib.sha256(json.dumps([source_id, row['kind'], row['row'], row['sha256'], session]).encode()).hexdigest()
            summary = {k: str(row['value'][k])[:500] for k in ('id', 'prompt', 'status', 'phase') if k in row['value']}
            history.db.execute('INSERT OR IGNORE INTO records(id,session,kind,source,source_row,digest,summary,original) VALUES(?,?,?,?,?,?,?,?)',
                               (key, session, row['kind'], source_id, row['row'], row['sha256'], json.dumps(summary), row['raw']))


def adopt_schedule(source, destination, *, source_sha256, source_session, session_mapping,
                   schedule_id, command_id, retirement_review_digest):
    if (not _digest(source_sha256) or not _digest(retirement_review_digest)
            or not all(_identity(v) for v in (source_session, schedule_id, command_id))
            or not isinstance(session_mapping, dict) or not 1 <= len(session_mapping) <= 101
            or source_session not in session_mapping
            or any(not _identity(k) or not _identity(v) or not v.startswith('ahp-session:/') for k, v in session_mapping.items())
            or len(set(session_mapping.values())) != len(session_mapping)):
        raise ValueError('Exact source review, distinct conversation mappings and bounded command required')
    source = Path(source).absolute()
    if source.resolve(strict=True) != source:
        raise ValueError('Sealed source must not be reached through symlinks')
    root = Path(destination).resolve()
    if source.is_relative_to(root):
        raise ValueError('Source must be outside the destination owner')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    session = session_mapping[source_session]
    signature = fingerprint(['schedule.legacyImport', str(source), source_sha256, source_session,
                             session_mapping, schedule_id, retirement_review_digest])
    store = history = None
    with FileLock(str(root / 'owner.lock'), timeout=0):
        gate = root / 'intake.sqlite3'
        if gate.exists():
            db = sqlite3.connect(gate.as_uri() + '?mode=ro', uri=True)
            try:
                if db.execute('SELECT 1 FROM fence LIMIT 1').fetchone():
                    raise ValueError('Destination owner has a retained quiescence fence')
            finally:
                db.close()
        if _source_digest(source) != source_sha256:
            raise ValueError('Reviewed source image changed')
        originals = _read_records(source, source_session, schedule_id)
        converted = _convert(originals, session_mapping, source_sha256)
        if _source_digest(source) != source_sha256:
            raise ValueError('Source image changed during adoption review')
        try:
            store = ScheduleStore(root / 'schedules.sqlite3')
            with store.transaction():
                prior = store.db.execute('SELECT fingerprint,result FROM schedule_commands WHERE id=?', (command_id,)).fetchone()
                if prior:
                    if prior[0] != signature:
                        raise ValueError('Migration command has different arguments')
                    return {**json.loads(prior[1]), 'previouslyAdopted': True}
                if store.db.execute('SELECT 1 FROM schedules WHERE id=?', (schedule_id,)).fetchone():
                    raise ValueError('Schedule identity already exists; both sources retained')
                if store.db.execute('SELECT COUNT(*) FROM schedules WHERE session_id=?', (session,)).fetchone()[0] >= 100:
                    raise ValueError('Destination conversation already has 100 schedules')
                for run in converted[1:]:
                    if store.db.execute('SELECT 1 FROM schedule_runs WHERE id=? OR (schedule_id=? AND due=?)', (run['id'], schedule_id, run['dueAt'])).fetchone():
                        raise ValueError('Scheduled run identity already exists; both sources retained')
                # Historical originals may commit before authority; a crash can only
                # leave display-only evidence. Repeating this retention is idempotent.
                history = HistoricalRecords(root / 'legacy.sqlite3')
                _retain(history, originals, hashlib.sha256(str(source).encode()).hexdigest(), source_session, session)
                store.put(converted[0])
                for run in converted[1:]:
                    store.put_run(run)
                result = {'commandId': command_id, 'schedule': converted[0], 'retainedRuns': len(converted) - 1,
                          'unknownRuns': sum(r.get('phase') == 'unknown' for r in converted[1:]),
                          'retirementReviewDigest': retirement_review_digest, 'sourceSha256': source_sha256,
                          'workReplayed': False, 'sourceRetained': True, 'activated': False}
                store.db.execute('INSERT INTO schedule_commands VALUES(?,?,?)', (command_id, signature, json.dumps(result)))
                return result
        finally:
            if history: history.close()
            if store: store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--request', required=True)
    args = parser.parse_args()
    path = Path(args.request)
    info = path.lstat()
    if (not path.is_absolute() or path.resolve() != path or not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_size > 65536):
        raise ValueError('Private bounded operator request required')
    request = json.loads(path.read_text())
    if not isinstance(request, dict) or set(request) != {'source_sha256', 'source_session', 'session_mapping', 'schedule_id', 'command_id', 'retirement_review_digest'}:
        raise ValueError('Exact schedule migration request required')
    print(json.dumps(adopt_schedule(args.source, args.destination, **request)))


if __name__ == '__main__':
    main()
