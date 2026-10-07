"""Offline adoption of a complete reviewed legacy watch without restarting it.

The source must remain retired. Source qualification, admission and presentation
grants are historical evidence only; any future watch needs fresh owner review.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from filelock import FileLock
from jsonschema import Draft202012Validator

from .adopt_questions import _digest, _schema, _time
from .adopt_schedules import _identity, _retain, _source_digest
from .history import HistoricalRecords
from .observation_store import Store, digest, encoded
from .observations_schemas import CONFIG, definitions

WATCH_CONFIG = definitions(_schema, lambda maximum: {'type': 'string', 'maxLength': maximum})['observation.preview'][1]
REASON = 'Imported watch is inactive. Qualify its current source and review the task before explicitly resuming. Past results are never resent.'


def _read(path, source_session, watch_id):
    db = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        if db.execute("SELECT 1 FROM observation_objects WHERE kind IN ('watch','run','outbox') AND NOT json_valid(body) LIMIT 1").fetchone():
            raise ValueError('Malformed source records prevent complete watch review')
        # Size checks precede fetching original JSON bodies.
        rows = db.execute("SELECT rowid,kind,id,length(CAST(body AS BLOB)) FROM observation_objects WHERE (kind='watch' AND id=?) OR (kind IN ('run','outbox') AND CASE WHEN json_valid(body) THEN json_extract(body,'$.watchId') END=?) LIMIT 10001", (watch_id, watch_id)).fetchall()
        if len(rows) > 10000 or any(type(r[3]) is not int or not 1 <= r[3] <= 128 * 1024 for r in rows) or sum(r[3] for r in rows) > 32 * 1024 * 1024:
            raise ValueError('Selected complete watch history exceeds its bounded representation')
        originals = []
        for position, kind, identity, _ in rows:
            raw = db.execute('SELECT body FROM observation_objects WHERE rowid=?', (position,)).fetchone()[0]
            value = json.loads(raw)
            if (not isinstance(value, dict) or value.get('id') != identity
                    or value.get('sessionId') != source_session
                    or kind != 'watch' and value.get('watchId') != watch_id):
                raise ValueError('Watch identity or conversation does not match its stored record')
            originals.append({'kind': kind, 'row': position, 'raw': raw, 'value': value,
                              'sha256': hashlib.sha256(raw.encode()).hexdigest()})
        watches = [r for r in originals if r['kind'] == 'watch']
        if len(watches) != 1:
            raise ValueError('Exactly one watch in the mapped source conversation is required')
        watch = watches[0]['value']
        # Preserve the source descriptor and command receipts without granting
        # executable observer authority in the destination.
        descriptor = db.execute("SELECT rowid,length(CAST(body AS BLOB)) FROM observation_objects WHERE kind='observer' AND id=?", (watch.get('observerId'),)).fetchone()
        if descriptor:
            if not 1 <= descriptor[1] <= 128 * 1024:
                raise ValueError('Original source descriptor exceeds its record bound')
            raw = db.execute('SELECT body FROM observation_objects WHERE rowid=?', (descriptor[0],)).fetchone()[0]
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get('id') != watch.get('observerId'):
                raise ValueError('Original source descriptor identity changed')
            originals.append({'kind': 'observer', 'row': descriptor[0], 'raw': raw, 'value': value,
                              'sha256': hashlib.sha256(raw.encode()).hexdigest()})
        commands = db.execute("SELECT rowid,length(CAST(result AS BLOB))+length(CAST(id AS BLOB))+length(CAST(intent AS BLOB)) FROM observation_commands WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.id') END=? LIMIT 10001", (watch_id,)).fetchall()
        if len(commands) > 10000 or any(not 1 <= r[1] <= 128 * 1024 for r in commands):
            raise ValueError('Original watch command history exceeds its record bound')
        for position, _ in commands:
            identity, intent, result = db.execute('SELECT id,intent,result FROM observation_commands WHERE rowid=?', (position,)).fetchone()
            raw = encoded({'id': identity, 'intent': intent, 'result': json.loads(result)})
            if not _digest(identity) or not _digest(intent) or len(raw.encode()) > 128 * 1024:
                raise ValueError('Unsupported original watch command receipt')
            originals.append({'kind': 'watch-command', 'row': position, 'raw': raw, 'value': json.loads(raw),
                              'sha256': hashlib.sha256(raw.encode()).hexdigest()})
        terminal = db.execute('SELECT semantic_key,input_id FROM observation_terminals WHERE watch=?', (watch_id,)).fetchone()
        if terminal:
            raw = encoded({'watchId': watch_id, 'semanticKey': terminal[0], 'inputId': terminal[1]})
            originals.append({'kind': 'watch-terminal', 'row': 0, 'raw': raw, 'value': json.loads(raw),
                              'sha256': hashlib.sha256(raw.encode()).hexdigest()})
        if sum(len(r['raw'].encode()) for r in originals) > 32 * 1024 * 1024:
            raise ValueError('Complete watch evidence exceeds 32 MiB')
        return originals, terminal
    finally:
        db.close()


def _convert(originals, terminal, session, source_digest):
    watch = next(r['value'] for r in originals if r['kind'] == 'watch')
    Draft202012Validator(WATCH_CONFIG).validate({key: watch[key] for key in (*CONFIG, 'sessionId') if key in watch})
    if (watch.get('status') not in {'active', 'paused', 'needs_review', 'cancelled', 'ended'}
            or type(watch.get('revision')) is not int or watch['revision'] < 1
            or type(watch.get('sequence')) is not int or watch['sequence'] < 0
            or not _identity(watch.get('requestId')) or not _identity(watch.get('taskId'))
            or type(watch.get('taskRevision')) is not int or watch['taskRevision'] < 1):
        raise ValueError('Unsupported original watch state or identity')
    outboxes = [r['value'] for r in originals if r['kind'] == 'outbox']
    if (len(outboxes) > 1 or bool(outboxes) != bool(terminal)
            or terminal and (not _identity(terminal[1]) or outboxes[0].get('inputId') != terminal[1]
                             or outboxes[0]['id'] != terminal[1] or not watch.get('terminal'))
            or watch.get('terminal') and not terminal or watch['status'] == 'ended' and not terminal):
        raise ValueError('Complete original handoff and terminal tombstone must agree')
    converted = []
    for original in originals:
        kind = original['kind']
        if kind not in {'watch', 'run', 'outbox'}:
            continue
        row = copy.deepcopy(original['value'])
        if not _identity(row['id']) or not _time(row.get('createdAt')) or not _time(row.get('updatedAt', row['createdAt'])):
            raise ValueError('Unsupported observation identity or timestamps')
        row['sessionId'] = session
        row.pop('owner', None)
        if kind == 'watch':
            if row['status'] in {'active', 'needs_review'}:
                row.update(status='needs_review', reason=REASON)
            row.update(nextDue=None, revision=row['revision'] + 1)
            row.pop('presentationGrant', None)
        elif kind == 'run':
            if row.get('phase') not in {'reading', 'abandoned_read', 'discarded', 'pending', 'actionable', 'observation_failed'}:
                raise ValueError('Unsupported original observation read phase')
            prefix = 'observation:' + watch['id'] + ':'
            ordinal = row['id'].removeprefix(prefix)
            if not row['id'].startswith(prefix) or not ordinal.isdecimal() or int(ordinal) >= watch['sequence']:
                raise ValueError('Saved watch sequence could overwrite an original read')
            if row['phase'] == 'reading': row['phase'] = 'abandoned_read'
        else:
            if row.get('phase') not in {'pending', 'waiting_worker', 'submitting', 'accepted', 'unknown', 'suppressed', 'completed', 'failed'}:
                raise ValueError('Unsupported original observation handoff phase')
            if row['phase'] in {'pending', 'waiting_worker'}:
                row.update(phase='suppressed', detail='Original result retained without admission during migration.')
            elif row['phase'] in {'submitting', 'accepted'}:
                row.update(phase='unknown', detail='Original result may have been admitted; inspect without replay.')
            if row.get('presentationPhase') == 'claimed': row['presentationPhase'] = 'unknown'
            row['nextDue'] = None
        row['legacyImport'] = {'sourceSha256': source_digest, 'recordSha256': original['sha256'],
            'sourceSession': original['value']['sessionId'], 'originalStatus': original['value'].get('status'),
            'originalPhase': original['value'].get('phase')}
        if len(encoded(row).encode()) > 128 * 1024:
            raise ValueError('Migrated observation exceeds its complete record bound')
        converted.append((kind, row))
    return converted


def adopt_observation(source, destination, *, source_sha256, source_session, session,
                      watch_id, command_id, retirement_review_digest):
    if (not all(_identity(v) for v in (source_session, session, watch_id, command_id))
            or not session.startswith('ahp-session:/') or not _digest(source_sha256)
            or not _digest(retirement_review_digest)):
        raise ValueError('Exact source review, mapped conversation and retirement review required')
    source = Path(source).absolute()
    if source.resolve(strict=True) != source:
        raise ValueError('Sealed source must not be reached through symlinks')
    root = Path(destination).resolve()
    if source.is_relative_to(root): raise ValueError('Source must be outside the destination owner')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    signature = digest(['observation.legacyImport', str(source), source_sha256, source_session,
                        session, watch_id, retirement_review_digest])
    command = 'observation.legacyImport:' + command_id
    store = history = None
    with FileLock(str(root / 'owner.lock'), timeout=0):
        gate = root / 'intake.sqlite3'
        if gate.exists():
            db = sqlite3.connect(gate.as_uri() + '?mode=ro', uri=True)
            try:
                if db.execute('SELECT 1 FROM fence LIMIT 1').fetchone():
                    raise ValueError('Destination owner has a retained quiescence fence')
            finally: db.close()
        if _source_digest(source) != source_sha256: raise ValueError('Reviewed source image changed')
        originals, terminal = _read(source, source_session, watch_id)
        converted = _convert(originals, terminal, session, source_sha256)
        if _source_digest(source) != source_sha256: raise ValueError('Source image changed during review')
        try:
            store = Store(root / 'observations.sqlite3', recover=False)
            with store.db:
                store.db.execute('BEGIN IMMEDIATE')
                prior = store.db.execute('SELECT fingerprint,receipt FROM commands WHERE session=? AND id=?', (session, command)).fetchone()
                if prior:
                    if prior[0] != signature: raise ValueError('Migration command has different arguments')
                    return {**json.loads(prior[1]), 'previouslyAdopted': True}
                for kind, row in converted:
                    if store.db.execute('SELECT 1 FROM records WHERE kind=? AND id=?', (kind, row['id'])).fetchone():
                        raise ValueError('Observation identity already exists; both sources retained')
                if store.db.execute("SELECT count(*) FROM records WHERE kind='watch' AND session=?", (session,)).fetchone()[0] >= 100:
                    raise ValueError('Destination conversation already has 100 saved watches')
                commands = [r['value'] for r in originals if r['kind'] == 'watch-command']
                for old in commands:
                    if store.db.execute('SELECT 1 FROM commands WHERE session=? AND id=?', (session, 'observation.legacyCommand:' + old['id'])).fetchone():
                        raise ValueError('Original watch command identity already exists')
                if store.db.execute('SELECT 1 FROM terminals WHERE watch=? OR input=?', (watch_id, terminal[1] if terminal else None)).fetchone():
                    raise ValueError('Original terminal identity already exists')
                history = HistoricalRecords(root / 'legacy.sqlite3')
                _retain(history, originals, hashlib.sha256(str(source).encode()).hexdigest(), source_session, session)
                for kind, row in converted: store.put(kind, row)
                if terminal: store.db.execute('INSERT INTO terminals VALUES(?,?)', (watch_id, terminal[1]))
                for old in commands:
                    saved = {'status': 'legacy_observed', 'watchId': watch_id, 'sourceSession': source_session,
                             'sourceSha256': source_sha256, 'originalCommandId': old['id'], 'replayed': False}
                    store.db.execute('INSERT INTO commands VALUES(?,?,?,?,?)',
                        (session, 'observation.legacyCommand:' + old['id'], old['intent'], watch_id, encoded(saved)))
                result = {'commandId': command_id, 'sessionId': session, 'watchId': watch_id,
                    'retainedRecords': len(originals), 'retainedRuns': sum(k == 'run' for k, _ in converted),
                    'retainedHandoffs': sum(k == 'outbox' for k, _ in converted),
                    'retirementReviewDigest': retirement_review_digest, 'sourceSha256': source_sha256,
                    'sourceRetained': True, 'activated': False, 'workReplayed': False, 'observerQualified': False}
                store.db.execute('INSERT INTO commands VALUES(?,?,?,?,?)', (session, command, signature, watch_id, encoded(result)))
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
    path = Path(args.request); info = path.lstat()
    if (not path.is_absolute() or path.resolve() != path or not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_size > 65536):
        raise ValueError('Private bounded operator request required')
    request = json.loads(path.read_text())
    if not isinstance(request, dict) or set(request) != {'source_sha256', 'source_session', 'session', 'watch_id', 'command_id', 'retirement_review_digest'}:
        raise ValueError('Exact watch migration request required')
    print(json.dumps(adopt_observation(args.source, args.destination, **request)))


if __name__ == '__main__': main()
