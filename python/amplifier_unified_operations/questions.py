"""Durable scoped questions. Answers preserve provenance and never grant permission."""
import asyncio
import base64
import copy
import hashlib
import hmac
import json
import os
import re
from pathlib import Path
import secrets
import sqlite3
import stat
import time
import uuid


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)



_TABLES = {'questions': 'CREATE TABLE questions (\n'
              '                id TEXT PRIMARY KEY, session TEXT NOT NULL, status TEXT NOT NULL,\n'
              '                created REAL NOT NULL, delivery TEXT, value TEXT NOT NULL)',
 'question_commands': 'CREATE TABLE question_commands (\n'
                      '                session TEXT NOT NULL, command TEXT NOT NULL, fingerprint '
                      'TEXT NOT NULL,\n'
                      '                operation TEXT NOT NULL, question TEXT NOT NULL, result '
                      'TEXT NOT NULL,\n'
                      '                PRIMARY KEY(session,command))'}

def _open_authority(path):
    """Validate retained authority read-only before opening a writer."""
    path = Path(path).absolute()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    for candidate in (path, *(Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal"))):
        if os.path.lexists(candidate) and not stat.S_ISREG(candidate.lstat().st_mode):
            raise ValueError("Authority database and sidecars must be regular files, without symlinks")
    new = not os.path.lexists(path)
    if new and any(os.path.lexists(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError("Retained authority sidecar has no main database")
    if not new:
        reader = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            names = tuple(_TABLES)
            found = {name: (kind, sql) for name, kind, sql in reader.execute(
                "SELECT name,type,substr(sql,1,4097) FROM sqlite_master WHERE name IN (" + ",".join("?" for _ in names) + ")", names)}
            normalize = lambda sql: re.sub(r"\s+", "", sql).lower().replace("ifnotexists", "")
            for name, expected in _TABLES.items():
                actual = found.get(name)
                if actual is None or actual[0] != "table" or not isinstance(actual[1], str) or len(actual[1]) >= 4097:
                    raise ValueError("Missing or unsupported authority schema: " + name)
                if normalize(actual[1]) != normalize(expected):
                    raise ValueError("Unsupported authority schema: " + name)
                reader.execute("SELECT * FROM " + name + " LIMIT 0")
        finally:
            reader.close()
    else:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
    db = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)
    try:
        path.chmod(0o600)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        if new:
            db.executescript("BEGIN;" + ";".join(_TABLES.values()) + ";COMMIT;")
        db.executescript('CREATE INDEX IF NOT EXISTS questions_scope ON questions(session,created,id);CREATE INDEX IF NOT EXISTS questions_status ON questions(session,status,created,id);CREATE INDEX IF NOT EXISTS questions_delivery ON questions(delivery);')
        db.executescript("CREATE INDEX IF NOT EXISTS questions_closed ON questions(session,created,id) WHERE status<>'pending';"
                         "CREATE INDEX IF NOT EXISTS questions_unconfirmed ON questions(session,created,id) WHERE status='answered' AND coalesce(delivery,'unknown')<>'accepted';")
    except BaseException:
        db.close()
        raise
    return db


class Questions:
    def __init__(self, path, host, changed, clock=time.time, *, recover_deliveries=True):
        self.host, self.changed, self.clock = host, changed, clock
        self.lock = asyncio.Lock()
        self.secret = secrets.token_bytes(32)
        self.db = _open_authority(path)
        self.db.row_factory = sqlite3.Row
        # A running owner reconciles interrupted sends. Offline selected imports
        # must leave unrelated destination records unchanged.
        if recover_deliveries:
            with self.db:
                self.db.execute("""UPDATE questions SET delivery='unknown', value=json_set(value,
                    '$.delivery.status','unknown','$.delivery.message',
                    'Owner restarted before delivery confirmation; answer retained without replay.')
                    WHERE delivery='sending'""")

    def get(self, sid, identity):
        row = self.db.execute('SELECT value FROM questions WHERE session=? AND id=?', (sid, identity)).fetchone()
        if row is None:
            raise ValueError('Question does not belong to this conversation')
        return json.loads(row['value'])

    def put(self, value):
        raw = encoded(value)
        if len(raw.encode()) > 128 * 1024:
            raise ValueError('Question exceeds the complete record bound')
        self.db.execute('INSERT OR REPLACE INTO questions VALUES(?,?,?,?,?,?)',
            (value['id'], value['sessionId'], value['status'], value['createdAt'], (value.get('delivery') or {}).get('status'), raw))

    def attention(self, sid):
        """Selected indexed attention IDs only; never decode question bodies."""
        rows = self.db.execute(
            "SELECT id FROM questions WHERE session=? AND status='pending' ORDER BY created,id LIMIT 65",
            (sid,),
        ).fetchall()
        complete = len(rows) <= 64
        return {'questionIds': [row['id'] for row in rows[:64]],
                'questionsComplete': complete,
                'attentionCoverage': {'questions': True, 'questionsTruncated': not complete}}

    def page(self, sid, args):
        limit, offset = args.get('limit', 25), args.get('offset', 0)
        if type(limit) is not int or not 1 <= limit <= 50 or type(offset) is not int or not 0 <= offset <= 1000:
            raise ValueError('Use bounded question pages and cursors beyond offset 1000')
        status = args.get('status')
        if status is not None and status not in {'pending', 'answered', 'cancelled', 'superseded', 'closed', 'unconfirmed'}:
            raise ValueError('Invalid question status')
        descending = status == 'closed'
        scope = hashlib.sha256(encoded([sid, status]).encode()).hexdigest()
        where, values = ['session=?'], [sid]
        if status == 'closed':
            where.append("status<>'pending'")
        elif status == 'unconfirmed':
            where.append("status='answered' AND coalesce(delivery,'unknown')<>'accepted'")
        elif status:
            where.append('status=?'); values.append(status)
        if args.get('cursor'):
            if offset:
                raise ValueError('A question cursor cannot be combined with an offset')
            try:
                cursor = args['cursor']
                if not isinstance(cursor, str) or len(cursor) > 1024:
                    raise ValueError()
                raw = base64.urlsafe_b64decode(cursor)
                if not hmac.compare_digest(raw[:32], hmac.new(self.secret, raw[32:], hashlib.sha256).digest()):
                    raise ValueError()
                page = json.loads(raw[32:])
                if page['scope'] != scope:
                    raise ValueError()
                op = '<' if descending else '>'
                where.append(f'(created,id){op}(?,?)'); values.extend([page['at'], page['id']])
            except (ValueError, TypeError, KeyError):
                raise ValueError('Question cursor is invalid, belongs to another scope, or expired after restart') from None
        rows, total, more = [], 0, False
        order = 'created DESC,id DESC' if descending else 'created,id'
        index = ' INDEXED BY questions_' + status if status in {'closed', 'unconfirmed'} else ''
        for row in self.db.execute('SELECT * FROM questions' + index + ' WHERE ' + ' AND '.join(where) + ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', (*values, limit + 1, offset)):
            size = len(row['value'].encode())
            if len(rows) >= limit or (rows and total + size > 512 * 1024):
                more = True
                break
            rows.append(row); total += size
        next_cursor = None
        if more:
            last = rows[-1]
            raw = encoded({'scope': scope, 'at': last['created'], 'id': last['id']}).encode()
            next_cursor = base64.urlsafe_b64encode(hmac.new(self.secret, raw, hashlib.sha256).digest() + raw).decode()
        return {'items': [json.loads(row['value']) for row in rows], 'nextCursor': next_cursor,
                'nextOffset': offset + len(rows) if next_cursor and not args.get('cursor') else None}

    def dependency(self, sid, identity):
        record = self.get(sid, identity)
        if record['status'] != 'answered':
            raise ValueError('Dependent work needs an explicit answer; pending, cancelled and superseded are not answers')
        return copy.deepcopy(record['answer'])

    def new(self, sid, args, origin, client):
        prompt, dependency = args['prompt'].strip(), args['dependency'].strip()
        options, allow_text = copy.deepcopy(args.get('options', [])), args.get('allowFreeText', True)
        if not prompt or not dependency or (not options and not allow_text):
            raise ValueError('Provide a question, its dependency, and options or free text')
        if any(not row['id'].strip() or not row['label'].strip() for row in options) or len({row['id'] for row in options}) != len(options):
            raise ValueError('Question options require unique nonempty IDs and labels')
        now = self.clock()
        return {'id': str(uuid.uuid4()), 'sessionId': sid, 'revision': 1, 'status': 'pending',
                'createdAt': now, 'updatedAt': now, 'prompt': prompt, 'dependency': dependency,
                'options': options, 'allowFreeText': allow_text, 'required': args['required'],
                'createdBy': {'origin': origin, 'clientId': client}, 'answer': None, 'delivery': None}

    @staticmethod
    def delivery_input(record):
        return {'commandId': record['delivery']['inputId'], 'text': f'Answer to “{record["prompt"]}”:\n{record["answer"]["text"]}\n\nFor: {record["dependency"]}',
                'questionId': record['id'], 'questionRevision': record['answer']['questionRevision'],
                'answerProvenance': record['answer']['provenance']}

    def authorize_delivery(self, sid, args):
        record = self.get(sid, args.get('questionId'))
        if record.get('legacyImport', {}).get('deliveryRetained'):
            raise ValueError('A retained legacy answer cannot authorize another delivery')
        if record['status'] != 'answered' or (record.get('delivery') or {}).get('status') not in {'sending', 'accepted'}:
            raise ValueError('No confirmed saved answer permits this delivery')
        expected = self.delivery_input(record)
        if (args.get('inputId') != expected['commandId'] or args.get('text') != expected['text']
                or args.get('questionRevision') != expected['questionRevision']
                or args.get('answerProvenance') != expected['answerProvenance']):
            raise ValueError('Question delivery differs from the exact saved answer')
        return {'admitted': True, 'inputId': expected['commandId'], 'questionId': record['id'],
                'questionRevision': expected['questionRevision'], 'answerHash': hashlib.sha256(expected['text'].encode()).hexdigest()}

    async def dispatch(self, operation, args, origin, client, command):
        sid = args['sessionId']
        if operation == 'question.read': return self.get(sid, args['id'])
        if operation == 'question.list': return self.page(sid, args)
        if not isinstance(command, str) or not 1 <= len(command) <= 200:
            raise ValueError('Stable bounded question command identity required')
        signature = hashlib.sha256(encoded([operation, args, origin, client]).encode()).hexdigest()
        async with self.lock:
            prior = self.db.execute('SELECT * FROM question_commands WHERE session=? AND command=?', (sid, command)).fetchone()
            if prior:
                if prior['fingerprint'] != signature: raise ValueError('Question command identity reused with different contents or actor')
                return self.get(sid, prior['question']) if operation == 'question.answer' else json.loads(prior['result'])
            replacement = None
            if operation == 'question.create':
                count = self.db.execute("SELECT count(*) FROM questions WHERE session=? AND status='pending'", (sid,)).fetchone()[0]
                if count >= 32: raise ValueError('Resolve or cancel one of the 32 pending questions first')
                record = self.new(sid, args, origin, client)
            else:
                record = self.get(sid, args['id'])
                if record['status'] != 'pending' or record['revision'] != args['expectedRevision']:
                    raise ValueError('Question changed or is already closed; answer was not delivered')
                if operation == 'question.cancel':
                    record.update(status='cancelled', cancellation={'reason': args.get('reason', ''), 'origin': origin, 'clientId': client})
                elif operation == 'question.supersede':
                    replacement = self.new(sid, args, origin, client); replacement['supersedes'] = record['id']
                    record.update(status='superseded', supersededBy=replacement['id'])
                elif operation == 'question.answer':
                    if ('optionId' in args) == ('text' in args): raise ValueError('Choose exactly one option or a free-text answer')
                    option = next((row for row in record['options'] if row['id'] == args.get('optionId')), None)
                    answer = option['label'] if option else args.get('text', '').strip()
                    if ('optionId' in args and option is None) or ('text' in args and (not record['allowFreeText'] or not answer)):
                        raise ValueError('Use an offered option or allowed nonempty free text')
                    provenance = {'origin': origin, 'clientId': client, 'via': 'ui'}
                    if args.get('sourceMessageId'):
                        source = await self.host('readUserMessage', {'session': sid, 'id': args['sourceMessageId']})
                        if not source or source.get('role') != 'user' or source.get('inputOrigin') not in {'ui', 'user', 'voice'} or source.get('questionId') or source.get('scheduledRunId') or source.get('createdAt', 0) < record['createdAt']:
                            raise ValueError('Cite an actual user response after this question, not a generated input')
                        provenance.update(messageId=source['id'], inputOrigin=source['inputOrigin'], textDigest=hashlib.sha256(source.get('text', '').encode()).hexdigest(),
                                          via='call' if source.get('inputOrigin') == 'voice' else 'chat')
                        for key in ('voiceId', 'voiceItemId'):
                            if source.get(key): provenance[key] = source[key]
                    elif origin != 'ui': raise ValueError('An agent must cite the actual user answer')
                    record.update(status='answered', answer={'text': answer, 'optionId': option['id'] if option else None,
                        'questionRevision': record['revision'], 'answeredAt': self.clock(), 'provenance': provenance},
                        delivery={'status': 'sending', 'inputId': 'question:' + record['id'] + ':answer', 'updatedAt': self.clock()})
                else: raise ValueError('Unadvertised question operation')
                record.update(revision=record['revision'] + 1, updatedAt=self.clock())
            result = replacement or record
            with self.db:
                self.put(record)
                if replacement: self.put(replacement)
                self.db.execute('INSERT INTO question_commands VALUES(?,?,?,?,?,?)', (sid, command, signature, operation, result['id'], encoded(result)))
            await self.changed(sid)
            if operation == 'question.answer':
                try:
                    admitted = await self.host('submitQuestionAnswer', {'session': sid, 'input': self.delivery_input(record)})
                    if admitted.get('accepted') is True: delivery = {'status': 'accepted'}
                    elif admitted.get('accepted') is False and admitted.get('executed') is False:
                        delivery = {'status': 'rejected', 'message': str(admitted.get('reason', 'Answer saved; delivery not admitted'))[:4000]}
                    else: raise ValueError('No conclusive question admission evidence')
                except BaseException as error:
                    delivery = {'status': 'unknown', 'message': str(error)[:4000] or 'Delivery interrupted; no replay'}
                    if isinstance(error, asyncio.CancelledError):
                        record['delivery'].update(delivery, updatedAt=self.clock())
                        with self.db: self.put(record)
                        raise
                record['delivery'].update(delivery, updatedAt=self.clock())
                with self.db: self.put(record)
                await self.changed(sid)
            return copy.deepcopy(result)

    def close(self):
        self.db.close()
