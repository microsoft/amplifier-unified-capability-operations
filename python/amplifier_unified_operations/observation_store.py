"""Indexed private watch state; original history and native controls remain external."""
import base64
import hashlib
import hmac
import json
import os
import re
from pathlib import Path
import secrets
import sqlite3
import stat


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(value):return hashlib.sha256(encoded(value).encode()).hexdigest()


_TABLES = {'records': 'CREATE TABLE records(kind TEXT,id TEXT,session TEXT,watch TEXT,state TEXT,due '
            'REAL,created REAL,body TEXT,PRIMARY KEY(kind,id))',
 'commands': 'CREATE TABLE commands(session TEXT,id TEXT,fingerprint TEXT,watch TEXT,receipt '
             'TEXT,PRIMARY KEY(session,id))',
 'terminals': 'CREATE TABLE terminals(watch TEXT PRIMARY KEY,input TEXT UNIQUE)'}

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
    legacy = False
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
                if name == "commands" and normalize(actual[1]) == normalize('CREATE TABLE commands(session TEXT,id TEXT,fingerprint TEXT,watch TEXT,PRIMARY KEY(session,id))'):
                    legacy = True
                elif normalize(actual[1]) not in {normalize(expected), normalize('CREATE TABLE commands(session TEXT,id TEXT,fingerprint TEXT,watch TEXT,PRIMARY KEY(session,id), receipt TEXT)')}:
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
        elif legacy:
            db.execute("ALTER TABLE commands ADD COLUMN receipt TEXT")
        db.executescript("CREATE INDEX IF NOT EXISTS records_scope ON records(kind,session,created,id);CREATE INDEX IF NOT EXISTS records_watch ON records(kind,watch,created,id);CREATE INDEX IF NOT EXISTS records_due ON records(kind,state,due,id);CREATE INDEX IF NOT EXISTS outbox_due ON records(due,id) WHERE kind='outbox' AND state IN ('pending','waiting_worker','accepted');")
    except BaseException:
        db.close()
        raise
    return db


class Store:
    def __init__(self,path,*,recover=True):
        self.db = _open_authority(path)
        self.db.row_factory = sqlite3.Row
        self.secret = secrets.token_bytes(32)
        # The parent owns an exclusive process lease before constructing this store.
        if recover:
            with self.db:
                self.db.execute("UPDATE records SET state='abandoned_read',body=json_set(body,'$.phase','abandoned_read') WHERE kind='run' AND state='reading'")
                self.db.execute("UPDATE records SET state='unknown',body=json_set(body,'$.phase','unknown','$.detail','Owner restarted; admission is uncertain and will not replay') WHERE kind='outbox' AND state='submitting'")
    def put(self,kind,row):
        raw=encoded(row)
        if len(raw.encode())>128*1024:raise ValueError('Complete observation record exceeds128KiB')
        self.db.execute('INSERT OR REPLACE INTO records VALUES(?,?,?,?,?,?,?,?)',(kind,row['id'],row.get('sessionId'),row.get('watchId'),row.get('phase',row.get('status')),row.get('nextDue',0),row.get('createdAt',row.get('qualifiedAt',0)),raw))
        return row
    def get(self,kind,identity,sid=None):
        row=self.db.execute('SELECT session,body FROM records WHERE kind=? AND id=?',(kind,identity)).fetchone()
        if row is None or sid is not None and row['session']!=sid:raise ValueError('Observation record is unavailable in this conversation')
        return json.loads(row['body'])
    def page(self,kind,sid=None,*,limit=25,cursor=None,watch=None):
        if type(limit) is not int or not 1<=limit<=50:raise ValueError('Observation page limit must be1..50')
        scope=digest([kind,sid,watch]);where=['kind=?'];values=[kind]
        for name,value in [('session',sid),('watch',watch)]:
            if value is not None:where.append(name+'=?');values.append(value)
        if cursor:
            try:
                if not isinstance(cursor,str) or len(cursor)>1024:raise ValueError()
                raw=base64.urlsafe_b64decode(cursor)
                if not hmac.compare_digest(raw[:32],hmac.new(self.secret,raw[32:],hashlib.sha256).digest()):raise ValueError()
                part=json.loads(raw[32:])
                if part['scope']!=scope:raise ValueError()
                where.append('(created>? OR (created=? AND id>?))');values.extend([part['at'],part['at'],part['id']])
            except (ValueError,KeyError,TypeError):raise ValueError('Invalid or expired observation page cursor') from None
        rows=[];size=0;more=False
        for row in self.db.execute('SELECT created,id,body FROM records WHERE '+' AND '.join(where)+' ORDER BY created,id LIMIT ?',(*values,limit+1)):
            cost=len(row['body'].encode())
            if len(rows)>=limit or rows and size+cost>512*1024:more=True;break
            rows.append(row);size+=cost
        next_cursor=None
        if more:
            last=rows[-1];raw=encoded({'scope':scope,'at':last['created'],'id':last['id']}).encode();next_cursor=base64.urlsafe_b64encode(hmac.new(self.secret,raw,hashlib.sha256).digest()+raw).decode()
        return {'items':[json.loads(row['body']) for row in rows],'nextCursor':next_cursor}
    def due(self,now):
        return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM records WHERE kind='watch' AND state='active' AND due<=? ORDER BY due,id LIMIT 16",(now,))]
    def pending(self,now):
        return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM records WHERE kind='outbox' AND state IN ('pending','waiting_worker','accepted') AND due<=? ORDER BY due,id LIMIT 16",(now,))]
    def count_active(self,sid):return self.db.execute("SELECT count(*) FROM records WHERE kind='watch' AND session=? AND state='active'",(sid,)).fetchone()[0]
    def operations(self,sid,identity=None,limit=50,cursor=None):
        """Bounded metadata projection, not deserialized watch/result histories."""
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('Operation page limit must be1..100')
        where=["kind='watch'","session=?"];values=[sid]
        if identity:
            if not identity.startswith('observation:'):return []
            where.append('id=?');values.append(identity.removeprefix('observation:'))
        if cursor:
            where.append("(created, 'observation:'||id)<(?,?)");values.extend(cursor)
        rows=self.db.execute("SELECT id,created,state,json_extract(body,'$.revision') revision,json_extract(body,'$.updatedAt') updated,json_extract(body,'$.terminal') terminal FROM records WHERE "+' AND '.join(where)+' ORDER BY created DESC,id DESC LIMIT ?',(*values,limit))
        states={'active':'running','paused':'paused','needs_review':'unknown','cancelled':'cancelled','ended':'completed'}
        return [{'id':'observation:'+r['id'],'sessionId':sid,'source':'observation','sourceId':r['id'],'kind':'observation','state':states.get(r['state'],'unknown'),'revision':str(r['revision']),'createdAt':r['created'],'updatedAt':r['updated'],'controlAvailable':False,'title':'Quiet watch','metadata':{'watchId':r['id'],'watchStatus':r['state'],'terminal':r['terminal'],'owner':'observations','meaning':'Source watch state; does not establish explanation or underlying product completion'}} for r in rows]
    def command(self,sid,identity,intent):
        row=self.db.execute('SELECT fingerprint,watch FROM commands WHERE session=? AND id=?',(sid,identity)).fetchone()
        if row and row['fingerprint']!=intent:raise ValueError('Observation request identity reused with different intent')
        return self.get('watch',row['watch'],sid) if row else None
    def legacy_request(self,sid,operation,request):
        # Original command IDs hash the old native scope. Preserve that identity
        # across the explicit AHP mapping, without inventing a missing revision.
        sources=self.db.execute("SELECT DISTINCT json_extract(body,'$.legacyImport.sourceSession') FROM records WHERE kind='watch' AND session=? AND json_extract(body,'$.legacyImport.sourceSession') IS NOT NULL",(sid,))
        for source, in sources:
            key='observation.legacyCommand:'+digest([source,operation,request])
            row=self.db.execute('SELECT watch,receipt FROM commands WHERE session=? AND id=?',(sid,key)).fetchone()
            if row:return {'watch':self.get('watch',row['watch'],sid),'receipt':json.loads(row['receipt'])}
        return None
    def phase(self,item,phase,**extra):
        item={**item,'phase':phase,**extra}
        with self.db:self.put('outbox',item)
        return item
    def terminal(self,watch,outcome,now):
        if self.db.execute('SELECT 1 FROM terminals WHERE watch=?',(watch['id'],)).fetchone():return
        identity='observation:'+watch['id']+':result'
        self.db.execute('INSERT INTO terminals VALUES(?,?)',(watch['id'],identity))
        watch.update(status='ended',revision=watch['revision']+1,terminal=outcome['status'],updatedAt=now)
        self.put('watch',watch);self.put('outbox',{'id':identity,'inputId':identity,'watchId':watch['id'],'sessionId':watch['sessionId'],'phase':'pending','outcome':outcome,'createdAt':now})
    def prune(self,watch):
        self.db.execute("DELETE FROM records WHERE kind='run' AND watch=? AND state!='reading' AND id NOT IN (SELECT id FROM records WHERE kind='run' AND watch=? ORDER BY created DESC,id DESC LIMIT 100)",(watch,watch))
    def close(self):self.db.close()
