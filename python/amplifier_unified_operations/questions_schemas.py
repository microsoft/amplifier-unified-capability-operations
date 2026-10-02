"""Question action schema extracted from Amplifier Unified; MIT."""
def definitions(schema, string):
    identity = {"sessionId": {**string(200), "minLength": 1}, "id": {**string(100), "minLength": 1}}
    revision = {"expectedRevision": {"type": "integer", "minimum": 1}}
    content = {
        "prompt": {**string(8000), "minLength": 1},
        "options": {"type": "array", "maxItems": 8, "items": schema({
            "id": {**string(100), "minLength": 1}, "label": {**string(200), "minLength": 1},
            "description": string(1000)}, ["id", "label"])},
        "allowFreeText": {"type": "boolean"},
        "required": {"type": "boolean"},
        "dependency": {**string(1000), "minLength": 1},
    }
    return {
        "question.create": ("Ask a durable question in this conversation. required=true blocks only the named dependency, not independent work. This is never permission approval. Keep a stable command ID for retries.", schema({"sessionId": identity["sessionId"], **content}, ["sessionId", "prompt", "required", "dependency"])),
        "question.list": ("Read this conversation's saved questions without selecting it or starting work. Pending is not answered, regardless of elapsed time.", schema({"sessionId": identity["sessionId"], "status": {"enum": ["pending", "answered", "cancelled", "superseded"]}, "offset": {"type": "integer", "minimum": 0}, "limit": {"type": "integer", "minimum": 1, "maximum": 50}}, ["sessionId"])),
        "question.read": ("Read one exact question, its answer provenance and separate runtime delivery state.", schema(identity)),
        "question.answer": ("Record an explicit answer once for this question revision. Choose optionId or text. Agents must cite sourceMessageId from a user message or voice transcript in this conversation. Answering never grants permission. Duplicate answers never replay work.", schema({**identity, **revision, "optionId": string(100), "text": string(8000), "sourceMessageId": string(200)}, ["sessionId", "id", "expectedRevision"])),
        "question.cancel": ("Cancel a pending question at the exact revision. Cancellation is not an answer and does not authorize the dependent work.", schema({**identity, **revision, "reason": string(1000)}, ["sessionId", "id", "expectedRevision"])),
        "question.supersede": ("Replace a pending question atomically. Late answers to the old question are rejected. The replacement has its own ID and revision.", schema({**identity, **revision, **content}, ["sessionId", "id", "expectedRevision", "prompt", "required", "dependency"])),
    }
