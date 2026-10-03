"""Transfer observations are immutable history, never scheduler or control authority."""
import hashlib
import json
import sqlite3
from amplifier_portability.evidence import envelope, decode_body, canonical_bytes

OWNER='unified.operations'
# JSON escaping in the private 4 MB transport requires a smaller source bound.
MAX_BYTES=1024*1024

def identity(value):
    if not isinstance(value,str) or not value or len(value)>8192:raise ValueError('Bounded transfer identity required')
    return value

class TransferEvidence:
    def __init__(self,path,owner):
        self.owner=owner;self.db=sqlite3.connect(path)
        path.chmod(0o600)
        self.db.executescript('''PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS evidence(id TEXT PRIMARY KEY,source TEXT NOT NULL,native TEXT NOT NULL,hash TEXT NOT NULL,envelope TEXT NOT NULL,session TEXT,release TEXT);
          CREATE INDEX IF NOT EXISTS evidence_session ON evidence(session,id);
          CREATE TABLE IF NOT EXISTS exports(id TEXT PRIMARY KEY,session TEXT NOT NULL,envelope TEXT NOT NULL);''')
    def export(self,args):
        sid=identity(args['session']);tid=identity(args['transferId'])
        limit=min(args.get('limitBytes',MAX_BYTES),MAX_BYTES)
        if type(limit) is not int or limit<1024:raise ValueError('Evidence budget must be at least 1024 bytes')
        prior=self.db.execute('SELECT session,envelope FROM exports WHERE id=?',(tid,)).fetchone()
        if prior:
            if prior[0]!=sid:raise ValueError('Transfer identity belongs to another conversation')
            value=json.loads(prior[1])
            if value['bytes']>limit:raise ValueError('Stored evidence exceeds the requested bound')
            if args.get('expectedRevision',value['revision'])!=value['revision']:raise ValueError('Evidence revision changed')
            return value
        o=self.owner;records=[];omissions=[];body={'sourceSession':sid,'records':records,'historicalOnly':True}
        # Indexed selected-conversation queries; never enumerate global history.
        sources=[('schedule',o.schedules.store.db,'SELECT value FROM schedules WHERE session_id=?'),
         ('schedule-run',o.schedules.store.db,'SELECT value FROM schedule_runs WHERE session_id=?'),
         ('operation',o.operations.journal.db,'SELECT value FROM operations WHERE session_id=?'),
         ('operation-request',o.operations.journal.db,'SELECT value FROM operation_requests WHERE session_id=?'),
         ('question',o.questions.db,'SELECT value FROM questions WHERE session=?')]
        sources.extend((kind,o.observations.store.db,"SELECT body FROM records WHERE kind='"+kind+"' AND session=?") for kind in ('observer','watch','run','outbox'))
        used=len(canonical_bytes(body))
        for kind,db,query in sources:
            # Count through the same selected scope for exact explicit omissions.
            total=db.execute('SELECT count(*) FROM ('+query+')',(sid,)).fetchone()[0];copied=0
            for row in db.execute(query+' ORDER BY rowid LIMIT 501',(sid,)):
                if len(records)>=500:break
                record={'kind':kind,'value':json.loads(row[0])};cost=len(canonical_bytes(record))+1
                if used+cost>limit:break
                records.append(record);used+=cost;copied+=1
            if copied<total:omissions.append({'collection':kind,'count':total-copied,'reason':'bounded owner evidence'})
        output=o.operations.journal.db.execute('SELECT count(*) FROM operation_output WHERE operation_id IN (SELECT id FROM operations WHERE session_id=?)',(sid,)).fetchone()[0]
        if output:omissions.append({'collection':'operation-output','count':output,'reason':'source output remains on source disk; imported records do not claim complete output'})
        events=o.operations.journal.db.execute('SELECT count(*) FROM operation_events WHERE operation_id IN (SELECT id FROM operations WHERE session_id=?)',(sid,)).fetchone()[0]
        if events:omissions.append({'collection':'operation-events','count':events,'reason':'selected current operation values only; original events remain on source disk'})
        commands=o.questions.db.execute('SELECT count(*) FROM question_commands WHERE session=?',(sid,)).fetchone()[0]
        if commands:omissions.append({'collection':'question-command-receipts','count':commands,'reason':'historical question values only; commands are not executable on destination'})
        revision=hashlib.sha256(canonical_bytes(body)).hexdigest()
        if args.get('expectedRevision',revision)!=revision:raise ValueError('Evidence revision changed')
        result=envelope(OWNER,revision,body,omissions=omissions,limit_bytes=limit)
        with self.db:self.db.execute('INSERT INTO exports VALUES(?,?,?)',(tid,sid,json.dumps(result,ensure_ascii=False)))
        return result
    def stage(self,args):
        tid=identity(args['transferId']);source=identity(args['sourceHost']);native=identity(args['nativeSessionId']);value=args['evidence']
        body=decode_body(value,expected_owner=OWNER,limit_bytes=MAX_BYTES,accept_partial=True)
        if not isinstance(body,dict) or body.get('historicalOnly') is not True or not isinstance(body.get('records'),list) or len(body['records'])>500:raise ValueError('Invalid historical owner evidence')
        prior=self.db.execute('SELECT source,native,hash FROM evidence WHERE id=?',(tid,)).fetchone()
        if prior and tuple(prior)!=(source,native,value['sha256']):raise ValueError('Transfer evidence identity changed')
        with self.db:self.db.execute('INSERT OR IGNORE INTO evidence VALUES(?,?,?,?,?,NULL,NULL)',(tid,source,native,value['sha256'],json.dumps(value,ensure_ascii=False)))
        return {'owner':OWNER,'revision':value['revision'],'sha256':value['sha256'],'staged':True,'executionAuthority':False}
    def activate(self,args):
        tid=identity(args['transferId']);sid=identity(args['session']);release=args['releaseHash']
        if not isinstance(release,str) or len(release)!=64 or any(c not in '0123456789abcdef' for c in release):raise ValueError('Verified release hash required')
        prior=self.db.execute('SELECT hash,session,release FROM evidence WHERE id=?',(tid,)).fetchone()
        if not prior or prior[0]!=args['evidenceHash']:raise ValueError('Staged evidence hash mismatch')
        if prior[1] is not None and tuple(prior[1:])!=(sid,release):raise ValueError('Evidence already activated under different authority')
        with self.db:self.db.execute('UPDATE evidence SET session=?,release=? WHERE id=?',(sid,release,tid))
        return {'owner':OWNER,'transferId':tid,'session':sid,'evidenceHash':prior[0],'releaseHash':release,'replayed':False,'executionAuthority':False}
    def read(self,args):
        sid=identity(args['session']);tid=identity(args['transferId']);offset=args.get('offset',0);limit=args.get('limit',10)
        if type(offset) is not int or offset<0 or type(limit) is not int or not 1<=limit<=25:raise ValueError('Bounded history page required')
        row=self.db.execute('SELECT source,envelope,release FROM evidence WHERE id=? AND session=?',(tid,sid)).fetchone()
        if row is None:raise ValueError('Imported evidence unavailable in this conversation')
        value=json.loads(row[1]);body=decode_body(value,expected_owner=OWNER,limit_bytes=MAX_BYTES,accept_partial=True);selected=[];size=0
        for record in body['records'][offset:offset+limit]:
            cost=len(canonical_bytes(record))
            if selected and size+cost>256*1024:break
            selected.append({'namespace':row[0]+':'+tid,'historicalOnly':True,'executionAuthority':False,**record});size+=cost
        next_offset=offset+len(selected)
        return {'owner':OWNER,'sourceHost':row[0],'releaseHash':row[2],'items':selected,'nextOffset':next_offset if next_offset<len(body['records']) else None,'omissions':value['omissions'],'executionAuthority':False}
    def close(self):self.db.close()
