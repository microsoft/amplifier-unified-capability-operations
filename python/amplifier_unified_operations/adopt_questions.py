"""Offline adoption of reviewed legacy questions; never deliver an old answer.

The distribution owns the stopped-source proof and exact native-to-AHP mapping.
This interface accepts only immutable records already retained by migrate.py.
It is deliberately absent from the client/agent RPC surface.
"""
import argparse
import copy
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3

from filelock import FileLock
from jsonschema import Draft202012Validator

from .history import HistoricalRecords
from .migrate import identity
from .questions import Questions, encoded
from .questions_schemas import definitions


def _schema(properties, required=None):
    return {'type': 'object', 'properties': properties, 'required': required or list(properties),
            'additionalProperties': False}


CONTENT = definitions(_schema, lambda maximum: {'type': 'string', 'maxLength': maximum})['question.create'][1]


def _digest(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value)


def _time(value):
    return type(value) in {int, float} and math.isfinite(value) and value >= 0


def _convert(retained, session):
    old = retained['record']
    if (retained['kind'] != 'question' or not isinstance(old, dict)
            or not isinstance(old.get('id'), str) or not 1 <= len(old['id']) <= 100
            or type(old.get('revision')) is not int or old['revision'] < 1
            or old.get('status') not in {'pending', 'answered', 'cancelled', 'superseded'}
            or not _time(old.get('createdAt')) or not _time(old.get('updatedAt'))):
        raise ValueError('Unsupported legacy question; original retained for inspection')
    fields = {'sessionId', 'prompt', 'required', 'dependency', 'options', 'allowFreeText'}
    content = {key: old[key] for key in fields if key in old}
    Draft202012Validator(CONTENT).validate(content)
    options = old.get('options', [])
    if (not old['prompt'].strip() or not old['dependency'].strip()
            or len({row['id'] for row in options}) != len(options)
            or any(not row['id'].strip() or not row['label'].strip() for row in options)
            or not options and old.get('allowFreeText', True) is False):
        raise ValueError('Unsupported legacy question content')
    answer, delivery = old.get('answer'), old.get('delivery')
    if old['status'] == 'answered':
        if (not isinstance(answer, dict) or not isinstance(answer.get('text'), str)
                or not 1 <= len(answer['text']) <= 8000
                or type(answer.get('questionRevision')) is not int
                or not 1 <= answer['questionRevision'] < old['revision']
                or not _time(answer.get('answeredAt')) or not isinstance(answer.get('provenance'), dict)
                or not isinstance(delivery, dict) or delivery.get('status') not in {'sending', 'accepted', 'unknown', 'rejected'}
                or not isinstance(delivery.get('inputId'), str) or not delivery['inputId']):
            raise ValueError('Unsupported legacy answer; original retained for inspection')
    elif answer is not None or delivery is not None:
        raise ValueError('Unanswered question has uncertain answer data; inspect original')
    if old['status'] == 'superseded' and (not isinstance(old.get('supersededBy'), str) or not old['supersededBy']):
        raise ValueError('Superseded question requires its original replacement identity')
    row = copy.deepcopy(old)
    row.update(sessionId=session, options=options, allowFreeText=old.get('allowFreeText', True),
               answer=answer, delivery=copy.deepcopy(delivery),
               legacyImport={'recordId': retained['id'], 'sha256': retained['sha256'],
                             'sourceSession': old['sessionId'], 'deliveryRetained': old['status'] == 'answered'})
    if delivery and delivery['status'] == 'sending':
        row['delivery'].update(status='unknown', message='Original delivery was uncertain at migration; answer retained without replay.')
    if len(encoded(row).encode()) > 128 * 1024:
        raise ValueError('Legacy question exceeds the destination complete record bound')
    return row


def adopt_questions(destination, *, session, command_id, records, retirement_review_digest):
    """Copy an exact reviewed page into question authority, atomically and once.

    Pending questions remain pending with original IDs/revisions. Answered,
    cancelled and superseded records remain closed. No admission or callback is
    possible. The source owner must stay retired; its retirement review is an
    operator assumption, not an independently observed OS process proof.
    """
    identity(session); identity(command_id)
    if not session.startswith('ahp-session:/') or len(command_id) > 200 or not _digest(retirement_review_digest):
        raise ValueError('Exact destination, bounded command and retirement review required')
    if (not isinstance(records, list) or not 1 <= len(records) <= 50
            or any(not isinstance(row, dict) or set(row) != {'id', 'sha256'}
                   or not _digest(row['id']) or not _digest(row['sha256']) for row in records)
            or len({row['id'] for row in records}) != len(records)):
        raise ValueError('One to fifty distinct reviewed historical records required')
    root = Path(destination).resolve(strict=True)
    signature = hashlib.sha256(encoded([session, records, retirement_review_digest]).encode()).hexdigest()
    history = questions = None
    with FileLock(str(root / 'owner.lock'), timeout=0):
        gate = root / 'intake.sqlite3'
        if gate.exists():
            control = sqlite3.connect(gate.as_uri() + '?mode=ro', uri=True)
            try:
                if control.execute('SELECT 1 FROM fence LIMIT 1').fetchone():
                    raise ValueError('Destination owner has a retained quiescence fence')
            finally:
                control.close()
        try:
            history = HistoricalRecords(root / 'legacy.sqlite3')
            converted = []
            for selected in records:
                original = history.read(session, selected['id'])
                raw = history.db.execute('SELECT original FROM records WHERE session=? AND id=?',
                                         (session, selected['id'])).fetchone()[0]
                if original['sha256'] != selected['sha256'] or hashlib.sha256(raw.encode()).hexdigest() != selected['sha256']:
                    raise ValueError('Reviewed historical question changed')
                converted.append(_convert(original, session))
            if len({row['id'] for row in converted}) != len(converted):
                raise ValueError('Different originals share a question identity; inspect before adoption')
            async def forbidden(*_):
                raise AssertionError('Question migration cannot call a host or admit input')
            questions = Questions(root / 'questions.sqlite3', forbidden, forbidden, recover_deliveries=False)
            with questions.db:
                questions.db.execute('BEGIN IMMEDIATE')
                prior = questions.db.execute('SELECT fingerprint,operation,result FROM question_commands WHERE session=? AND command=?',
                                             (session, command_id)).fetchone()
                if prior:
                    if prior['fingerprint'] != signature or prior['operation'] != 'question.legacyImport':
                        raise ValueError('Migration command has different arguments')
                    return {**json.loads(prior['result']), 'previouslyAdopted': True}
                for row in converted:
                    if questions.db.execute('SELECT 1 FROM questions WHERE id=?', (row['id'],)).fetchone():
                        raise ValueError('Question identity already exists; original and destination retained')
                for row in converted:
                    questions.put(row)
                result = {'commandId': command_id, 'sessionId': session, 'questionIds': [r['id'] for r in converted],
                          'pending': sum(r['status'] == 'pending' for r in converted),
                          'retirementReviewDigest': retirement_review_digest, 'oldAnswersDelivered': 0,
                          'workReplayed': False, 'sourceRetained': True}
                questions.db.execute('INSERT INTO question_commands VALUES(?,?,?,?,?,?)',
                                     (session, command_id, signature, 'question.legacyImport', converted[0]['id'], encoded(result)))
                return result
        finally:
            if questions: questions.close()
            if history: history.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--request', required=True)
    args = parser.parse_args()
    # Private operator request; never load configuration or code from the source.
    import os
    import stat
    path = Path(args.request)
    info = path.lstat()
    if (not path.is_absolute() or path.resolve() != path or not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid() or info.st_size > 32768):
        raise ValueError('Private bounded operator request required')
    request = json.loads(path.read_text())
    if not isinstance(request, dict) or set(request) != {'session', 'command_id', 'records', 'retirement_review_digest'}:
        raise ValueError('Exact question migration request required')
    print(json.dumps(adopt_questions(args.destination, **request)))


if __name__ == '__main__':
    main()
