def definitions(schema, string):
    sid = {'sessionId': string(200)}
    identity = {**sid, 'id': string(200)}
    revision = {'expectedRevision': {'type': 'integer', 'minimum': 0}}
    fields = {'prompt': {**string(16000), 'minLength': 1}, 'spec': {'type': 'object'}, 'kind': {'enum': ['task', 'monitor']}, 'missedRunPolicy': {'enum': ['skip', 'latest']}, 'notificationPolicy': {'enum': ['changes', 'always', 'failures_only']}}
    destination = {'destination': {'enum': ['same_task', 'new_task']}, 'newTaskTitle': string(200), 'newTaskMaxTurns': {'type': 'integer', 'minimum': 1, 'maximum': 100}}
    authorization = {'previewHash': string(100), 'sourceMessageId': string(200)}
    return {
        'schedule.list': ('List saved schedules in any registered conversation in this library without selecting it or starting work. Peer reads are read-only; changes remain scoped to the calling conversation.', schema(sid)),
        'schedule.read': ('Read a schedule and its bounded durable run history in any registered conversation in this library without starting work. Peer reads are read-only. Unknown is never replayed.', schema(identity)),
        'schedule.request': ('Inspect the original committed result for one exact schedule command without replay. A missing receipt does not prove the command was never admitted.', schema({**sid,'requestId':string(200)})),
        'schedule.preview': ('Preview concrete timezone occurrences for an explicit user-requested schedule. destination defaults to same_task; new_task explicitly creates a fresh user-owned task for every run using the reviewed source configuration. Does not save or submit work.', schema({**sid, **fields, **destination}, [*sid, *fields])),
        'schedule.create': ('Save and activate exactly the reviewed prompt/timing for an explicit user request. Agents must cite the actual user sourceMessageId. A pending question never authorizes a schedule. Tool permissions still apply at execution.', schema({**sid, **revision, **fields, **destination, **authorization}, [*sid, *revision, *fields, 'previewHash'])),
        'schedule.update': ('Review and replace the saved prompt/timing against the latest task correction. Requires exact revision, concrete preview and renewed explicit user provenance.', schema({**identity, **revision, **fields, **destination, **authorization}, [*identity, *revision, *fields, 'previewHash'])),
        'schedule.pause': ('Pause future due runs. Current admitted work can finish; no rollback is claimed.', schema({**identity, **revision})),
        'schedule.resume': ('Deliberately resume after concrete review. Requires current preview and explicit user provenance; does not replay past uncertain runs.', schema({**identity, **revision, **authorization}, [*identity, *revision, 'previewHash'])),
        'schedule.cancel': ('Cancel future due runs while preserving run evidence. Does not undo admitted work.', schema({**identity, **revision})),
        'schedule.report': ('Record attributable monitor evidence for this scheduled run. Compared values are retained; agent-reported unchanged is labelled as a claim, not proof. This does not complete the task or grant permission.', schema({**sid, 'runId': string(200), **revision, 'outcome': {'enum': ['changed', 'unchanged']}, 'detail': string(4000), 'values': {}}, [*sid, 'runId', *revision, 'outcome', 'detail'])),
        'schedule.reconcile': ('Explicitly reconcile an uncertain run as completed or abandoned with user-provided evidence. Never resubmits the old input. Schedule resume remains separate.', schema({**sid, 'runId': string(200), **revision, 'resolution': {'enum': ['completed', 'abandoned']}, 'evidence': {**string(4000), 'minLength': 1}, 'sourceMessageId': string(200)}, [*sid, 'runId', *revision, 'resolution', 'evidence'])),
    }
