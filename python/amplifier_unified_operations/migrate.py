"""Explicit offline import of selected legacy evidence. No work is resumed."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
from filelock import FileLock
from .history import HistoricalRecords,MAX_RECORD

COLLECTIONS={
 'schedule':('schedules','value','session_id',None),
 'schedule-run':('schedule_runs','value','session_id',None),
 'operation':('operations','value','session_id',None),
 'operation-request':('operation_requests','value','session_id',None),
 'question':('questions','value','session_id',None),
 **{kind:('observation_objects','body',"CASE WHEN json_valid(body) THEN json_extract(body,'$.sessionId') END",kind) for kind in ('observer','watch','run','outbox')},
}

def identity(value):
    if not isinstance(value,str) or not value or len(value)>8192 or '\0' in value:raise ValueError('Explicit bounded session identity required')
    return value

def import_page(source,destination,*,source_session,session,kind,after=0,limit=25):
    """Selected keyset page; preserve originals, bind mapping, never create live rows."""
    identity(source_session);identity(session)
    if not session.startswith('ahp-session:/'):raise ValueError('Explicit destination AHP session mapping required')
    if kind not in COLLECTIONS or type(after) is not int or after<0 or type(limit) is not int or not 1<=limit<=50:raise ValueError('Supported collection and bounded row page required')
    source=Path(source).resolve(strict=True);destination=Path(destination).resolve()
    if source.is_relative_to(destination):raise ValueError('Source must be outside the destination owner')
    destination.mkdir(parents=True,exist_ok=True,mode=0o700)
    source_id=hashlib.sha256(str(source).encode()).hexdigest();table,column,scope,subkind=COLLECTIONS[kind]
    with FileLock(str(destination/'owner.lock'),timeout=0):
        gate=destination/'intake.sqlite3'
        if gate.exists():
            control=sqlite3.connect(gate.as_uri()+'?mode=ro',uri=True)
            try:
                if control.execute('SELECT 1 FROM fence LIMIT 1').fetchone():raise ValueError('Destination owner has a retained quiescence fence')
            finally:control.close()
        history=HistoricalRecords(destination/'legacy.sqlite3');legacy=None
        try:
            legacy=sqlite3.connect(source.as_uri()+'?mode=ro',uri=True);legacy.execute('PRAGMA query_only=ON');legacy.execute('BEGIN')
            if not legacy.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():return {'items':[],'nextCursor':None,'sourceRetained':True,'tableAbsent':True,'executionAuthority':False}
            where=scope+'=? AND rowid>?';params=[source_session,after]
            if subkind:where+=' AND kind=?';params.append(subkind)
            # Never transfer an oversized body into Python merely to classify it.
            rows=legacy.execute(f'SELECT rowid,length(CAST({column} AS BLOB)),typeof({column}) FROM {table} WHERE {where} ORDER BY rowid LIMIT ?',(*params,limit+1)).fetchall()
            with history.db:
                bound=history.db.execute('SELECT session FROM mappings WHERE source=? AND original=?',(source_id,source_session)).fetchone()
                if bound and bound[0]!=session:raise ValueError('Source session already maps to another destination; inspect the retained mapping')
                history.db.execute('INSERT OR IGNORE INTO mappings VALUES(?,?,?)',(source_id,source_session,session))
            items=[]
            for rowid,size,storage in rows[:limit]:
                raw=None;digest='';record_id=None;classification='oversize-source-retained' if type(size) is int and size>MAX_RECORD else 'invalid-source-retained'
                if storage=='text' and type(size) is int and size<=MAX_RECORD:
                    raw=legacy.execute(f'SELECT {column} FROM {table} WHERE rowid=?',(rowid,)).fetchone()[0]
                    digest=hashlib.sha256(raw.encode()).hexdigest()
                    try:
                        value=json.loads(raw)
                        if not isinstance(value,dict) or value.get('sessionId')!=source_session or not isinstance(value.get('id'),str):raise ValueError('Unbound source record')
                        record_id=hashlib.sha256(json.dumps([source_id,kind,rowid,digest,session]).encode()).hexdigest()
                        summary={key:str(value[key])[:500] for key in ('id','title','name','prompt','state','phase','status') if key in value}
                        prior=history.db.execute('SELECT id FROM records WHERE id=?',(record_id,)).fetchone()
                        classification='already-imported' if prior else 'imported-historical'
                    except (ValueError,TypeError):classification='invalid-source-retained';record_id=None
                with history.db:
                    if record_id:history.db.execute('INSERT OR IGNORE INTO records(id,session,kind,source,source_row,digest,summary,original) VALUES(?,?,?,?,?,?,?,?)',(record_id,session,kind,source_id,rowid,digest,json.dumps(summary),raw))
                    history.db.execute('INSERT OR IGNORE INTO imports VALUES(?,?,?,?,?,?)',(source_id,kind,rowid,digest,classification,raw))
                items.append({'row':rowid,'id':record_id,'classification':classification,'sha256':digest or None})
            return {'source':source_id,'sourceSession':source_session,'session':session,'kind':kind,'items':items,'nextCursor':rows[limit-1][0] if len(rows)>limit else None,'sourceRetained':True,'executionAuthority':False,'replayed':False,'omissions':['Process output chunks, event journals, opaque unscoped command receipts and invalid unscopable observation rows remain in the source.','Historical active or pending states are evidence only; they cannot execute, deliver answers or resume a schedule.']}
        finally:
            if legacy:legacy.close()
            history.close()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('source','destination','source-session','session'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--kind',choices=COLLECTIONS,required=True);parser.add_argument('--after',type=int,default=0);parser.add_argument('--limit',type=int,default=25)
    args=vars(parser.parse_args());source=args.pop('source');destination=args.pop('destination')
    print(json.dumps(import_page(source,destination,**args),indent=2))
if __name__=='__main__':main()
