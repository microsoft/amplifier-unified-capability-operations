"""Bounded, held-only retention proof. Never executes or hydrates native history."""
KEYS=('fenceId','commandId','purpose','instanceId','dataScope')
def selected(intake,args):
    context=args.get('context');fence=intake.fence
    if not isinstance(context,dict) or context.get('purpose')!='retention-hide' or not fence or any(fence.get(k)!=context.get(k) for k in KEYS) or fence.get('state','held')!='held' or fence.get('phase','held')!='held':
        raise ValueError('Exact held retention intake required')
    sessions=args.get('sessions');limit=args.get('limit',101)
    if type(limit) is not int or not 1<=limit<=101 or not isinstance(sessions,list) or not 1<=len(sessions)<=limit or len(set(sessions))!=len(sessions) or any(not isinstance(s,str) or not s.startswith('ahp-session:/') or len(s)>8192 or any(ord(c)<32 for c in s) for s in sessions):
        raise ValueError('At most101 distinct explicit conversations required')
    return sessions

def result(sessions,check,omissions=()):
    return {'coverage':'partial' if omissions else 'complete','protected':[{'session':s,'reasons':reasons} for s in sessions if (reasons:=sorted(set(check(s))))],'omissions':list(omissions)}

def exists(db,sql,args=()):return db.execute(sql,args).fetchone() is not None

class References:
    def __init__(self,owner):
        self.owner=owner
        for db,statements in [
          (owner.schedules.store.db,["CREATE INDEX IF NOT EXISTS retention_schedule ON schedules(session_id,json_extract(value,'$.status'))", "CREATE INDEX IF NOT EXISTS retention_runs ON schedule_runs(session_id,phase)", "CREATE INDEX IF NOT EXISTS retention_incoming ON schedule_runs(json_extract(value,'$.destinationSessionId'),phase)"]),
          (owner.questions.db,["CREATE INDEX IF NOT EXISTS retention_questions ON questions(session,status,json_extract(value,'$.required'),delivery)","CREATE INDEX IF NOT EXISTS retention_answers ON questions(session,delivery)"]),
          (owner.operations.journal.db,["CREATE INDEX IF NOT EXISTS retention_operations ON operations(session_id,json_extract(value,'$.state'))", "CREATE INDEX IF NOT EXISTS retention_requests ON operation_requests(session_id,json_extract(value,'$.state'))"]),
          (owner.observations.store.db,["CREATE INDEX IF NOT EXISTS retention_watches ON records(session,kind,state)"])
        ]:
            with db:
                for sql in statements:db.execute(sql)
    def read(self,args):
        sessions=selected(self.owner.intake,args);o=self.owner
        def check(s):
            reasons=[]
            tests=[
              (o.schedules.store.db,"SELECT 1 FROM schedules WHERE session_id=? AND json_extract(value,'$.status') IN ('active','paused','needs_review') LIMIT 1",'scheduled-work'),
              (o.schedules.store.db,"SELECT 1 FROM schedule_runs WHERE session_id=? AND phase IN ('creating','claimed','submitting','accepted','running','unknown') LIMIT 1",'scheduled-admission'),
              (o.schedules.store.db,"SELECT 1 FROM schedule_runs WHERE json_extract(value,'$.destinationSessionId')=? AND phase IN ('creating','claimed','submitting','accepted','running','unknown') LIMIT 1",'scheduled-admission'),
              (o.questions.db,"SELECT 1 FROM questions WHERE session=? AND status='pending' AND json_extract(value,'$.required')=1 LIMIT 1",'required-question'),
              (o.questions.db,"SELECT 1 FROM questions WHERE session=? AND delivery IN ('sending','accepted','unknown') LIMIT 1",'question-delivery'),
              (o.operations.journal.db,"SELECT 1 FROM operations WHERE session_id=? AND json_extract(value,'$.state') IN ('queued','running','cancel_requested','interrupted','unknown') LIMIT 1",'operation-unsettled'),
              (o.operations.journal.db,"SELECT 1 FROM operation_requests WHERE session_id=? AND json_extract(value,'$.state') IN ('admitting','outcome_unknown') LIMIT 1",'operation-unsettled'),
              (o.observations.store.db,"SELECT 1 FROM records WHERE session=? AND kind='watch' AND state IN ('active','paused') LIMIT 1",'observation-watch'),
              (o.observations.store.db,"SELECT 1 FROM records WHERE session=? AND kind='outbox' AND state IN ('pending','waiting_worker','submitting','accepted','unknown') LIMIT 1",'observation-delivery')]
            for db,sql,reason in tests:
                if exists(db,sql,(s,)):reasons.append(reason)
            return reasons
        return result(sessions,check)
