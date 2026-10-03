def definitions(schema, string):

    common = {"sessionId": string(200), "id": string(200)}
    read = {
        **common,
        "cursor": {"type": "integer", "minimum": 0},
        "maxBytes": {"type": "integer", "minimum": 4096, "maximum": 100000},
    }
    return {
        "operations.history.list": (
            "Page explicitly imported legacy evidence for this conversation. Historical states never run work or grant process control.",
            schema({"sessionId": string(8192), "kind": {"enum": ["schedule","schedule-run","operation","operation-request","question","observer","watch","run","outbox"]}, "cursor": string(24000), "limit": {"type":"integer","minimum":1,"maximum":50}}, ["sessionId"]),
        ),
        "operations.history.read": (
            "Read one immutable selected legacy record. Its active or pending source status is historical only; nothing is resumed or answered.",
            schema({"sessionId": string(8192), "id": {"type":"string","pattern":"^[a-f0-9]{64}$"}}, ["sessionId","id"]),
        ),
        "operations.submit": (
            "Start an owned command through the existing shell approval path. Keep requestId stable when retrying; unknown requests never replay. questionIds binds only work depending on those exact answers.",
            schema({"sessionId": string(200), "requestId": string(200), "command": string(65536),
                    "timeout": {"type": "integer", "minimum": 1, "maximum": 3600},
                    "pty": {"type": "boolean"},
                    "questionIds": {"type": "array", "maxItems": 32, "uniqueItems": True,
                                    "items": {"type": "string", "minLength": 1, "maxLength": 200}}},
                   ["sessionId", "requestId", "command"]),
        ),
        "operations.write": (
            "Write input to the exact owned process through existing shell approvals. Raw input also requires the trusted mount policy. Keep requestId stable; uncertain input is never automatically retried.",
            schema({**common, "requestId": string(200), "stdin": string(65536), "closeStdin": {"type": "boolean"}},
                   ["sessionId", "id", "requestId"]),
        ),
        "operations.request": (
            "Read the saved admission or input receipt without replaying it.",
            schema({"sessionId": string(200), "requestId": string(200)}, ["sessionId", "requestId"]),
        ),
        "operations.list": (
            "Read observed operations for a conversation without selecting or running it. Existing Smart Tool receipts and workers share this view.",
            schema(
                {
                    "sessionId": string(200),
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                ["sessionId"],
            ),
        ),
        "operations.status": (
            "Read durable operation status and actual completion evidence; unknown outcomes are never inferred as success.",
            schema(common, ["sessionId", "id"]),
        ),
        "operations.read": (
            "Read bounded durable output by cursor. A gap explicitly identifies missing or expired evidence.",
            schema(read, ["sessionId", "id"]),
        ),
        "operations.wait": (
            "Wait for an actual change to the observed operation. Does not start, resume, cancel or replay work.",
            schema(
                {
                    **read,
                    "afterRevision": string(200),
                    "waitMs": {"type": "integer", "minimum": 0, "maximum": 60000},
                },
                ["sessionId", "id"],
            ),
        ),
        "operations.cancel": (
            "Request cancellation of an owned live process through existing shell hooks/approvals. A request is not proof of cancellation; read the observed final state.",
            schema(common, ["sessionId", "id"]),
        ),
    }
