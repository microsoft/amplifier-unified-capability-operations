"""Indexed retained legacy observations; never scheduling or process authority."""
import base64
import json
import sqlite3

MAX_RECORD=256*1024

class HistoricalRecords:
    def __init__(self,path):
        self.db=sqlite3.connect(path);path.chmod(0o600)
        self.db.executescript('''PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS mappings(source TEXT,original TEXT,session TEXT NOT NULL,PRIMARY KEY(source,original));
          CREATE TABLE IF NOT EXISTS records(seq INTEGER PRIMARY KEY,id TEXT UNIQUE,session TEXT,kind TEXT,source TEXT,source_row INTEGER,digest TEXT,summary TEXT,original TEXT);
          CREATE INDEX IF NOT EXISTS history_session ON records(session,seq);
          CREATE INDEX IF NOT EXISTS history_kind ON records(session,kind,seq);
          CREATE TABLE IF NOT EXISTS imports(source TEXT,kind TEXT,source_row INTEGER,digest TEXT,classification TEXT,original TEXT,PRIMARY KEY(source,kind,source_row,digest));''')
    def page(self,session,args):
        limit=args.get('limit',25);kind=args.get('kind');after=0
        if type(limit) is not int or not 1<=limit<=50:raise ValueError('History page must contain1..50 records')
        if args.get('cursor'):
            if not isinstance(args['cursor'],str) or len(args['cursor'])>24000:raise ValueError('Invalid history cursor')
            value=json.loads(base64.urlsafe_b64decode(args['cursor']))
            if not isinstance(value,list) or len(value)!=3 or value[:2]!=[session,kind] or type(value[2]) is not int or value[2]<0:raise ValueError('History cursor belongs to another scope')
            after=value[2]
        where='session=? AND seq>?';params=[session,after]
        if kind:where+=' AND kind=?';params.append(kind)
        rows=self.db.execute('SELECT seq,id,kind,source,source_row,digest,summary FROM records WHERE '+where+' ORDER BY seq LIMIT ?',(*params,limit+1)).fetchall()
        items=[{'id':r[1],'kind':r[2],'source':r[3],'sourceRow':r[4],'sha256':r[5],'summary':json.loads(r[6]),'historicalOnly':True,'executionAuthority':False} for r in rows[:limit]]
        cursor=base64.urlsafe_b64encode(json.dumps([session,kind,rows[limit-1][0]]).encode()).decode() if len(rows)>limit else None
        return {'items':items,'nextCursor':cursor,'historicalOnly':True,'executionAuthority':False}
    def read(self,session,identity):
        if not isinstance(identity,str) or len(identity)!=64:raise ValueError('Exact historical record required')
        row=self.db.execute('SELECT kind,source,digest,original FROM records WHERE session=? AND id=?',(session,identity)).fetchone()
        if not row:raise ValueError('Historical record does not belong to this conversation')
        return {'id':identity,'kind':row[0],'source':row[1],'sha256':row[2],'record':json.loads(row[3]),'historicalOnly':True,'executionAuthority':False,'controlAvailable':False,'replayed':False}
    def close(self):self.db.close()
