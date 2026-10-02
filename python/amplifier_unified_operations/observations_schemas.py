"""Unified quiet observation contracts, MIT source 0c878427."""
CONFIG=("observerId","target","scope","sourceId","args","intervalSeconds","durationSeconds","readTimeoutSeconds")
def definitions(schema, string):
    sid = {'sessionId': string(200)}
    identity = {**sid, 'id': string(200)}
    config = {'observerId': string(200), 'target': {'type': 'object'}, 'scope': {'type': 'object'}, 'sourceId': string(2000), 'args': {'type': 'object'},
        'intervalSeconds': {'type': 'integer', 'minimum': 15, 'maximum': 3600}, 'durationSeconds': {'type': 'integer', 'minimum': 30, 'maximum': 86400},
        'readTimeoutSeconds': {'type': 'integer', 'minimum': 1, 'maximum': 30}}
    config['presentationRequest'] = {'anyOf': [{'type': 'null'}, {'type': 'object'}]}
    review = {'previewHash': string(100), 'requestId': string(200), 'sourceMessageId': string(200)}
    revision = {'expectedRevision': {'type': 'integer', 'minimum': 1}}
    return {
        'observation.qualify': ('Operator-only source qualification of an already installed and connected read-only observer; annotations alone do not qualify.', schema({**sid, 'descriptor': {'type': 'object'}})),
        'observation.observers': ('Read qualified observation contracts and current eligibility without connecting or preparing a provider.', schema(sid)),
        'observation.preview': ('Preview a user-requested quiet watch against persisted task authority. Does not prepare a worker or provider.', schema({**sid, **config, 'sourceMessageId': string(200)}, [*sid, *CONFIG])),
        'observation.create': ('Arm the exact reviewed quiet observation. Cite actual human sourceMessageId; stable requestId returns the original watch on retry, never rearms it.', schema({**sid, **config, **review}, [*sid, *CONFIG, 'previewHash', 'requestId'])),
        'observation.request': ('Read one exact watch command receipt and current watch without replay.', schema({**sid, 'requestId': string(200),'operation':{'enum':['observation.create','observation.pause','observation.cancel','observation.resume']}},[*sid,'requestId'])),
        'observation.list': ('Read quiet watches for this conversation.', schema(sid)),
        'observation.read': ('Read exact quiet watch and bounded audit; never run the observer.', schema(identity)),
        'observation.report': ('Read the same durable observation report; source callbacks cannot author results through this action.', schema(identity)),
        'observation.pause': ('Pause checks and suppress an unadmitted handoff. Watched work is not stopped.', schema({**identity, **revision, 'requestId': string(200)})),
        'observation.cancel': ('Cancel this watch without cancelling or rolling back watched work.', schema({**identity, **revision, 'requestId': string(200)})),
        'observation.resume': ('Resume a nonterminal watch only after fresh review and renewed human authority. Unknown/terminal handoffs never replay.', schema({**identity, **revision, **config, **review}, [*identity, *revision, *CONFIG, 'previewHash', 'requestId'])),
    }
