# Amplifier Unified operations capability

Independent product owner for durable schedules and process evidence. Python
uses `amplifier-scheduling` and `amplifier-operations` through their public package
APIs. No AppService, native Python engine or host-private imports. MIT provenance:
original `amplifier_web/{schedules,operations,operation_requests}.py` action and
admission contracts at `828d2bff97a77fbdbd0f06429369f7ff1f4abf42`.

Build with `uv sync --directory python`, `npm ci`, `npm test`. The development
lock resolves the optional libraries from Foundation's candidate branch
`unified/shared-application-libraries-20261002`, with explicit package subdirectories.
Move that development reference to Foundation main after the library PR lands.
Distribution installs independent qualified wheels. No sibling source checkout
is required. Run the private child owner as:
`amplifier-unified-operations --config /owned/operations.json`, where the file
contains `{ "dataDir": "/owned/operations" }`. It takes an exclusive process
lock, uses SQLite WAL/FULL, and never retries admitted work after transport loss.

```js
import {createOperationsCapabilities} from '@amplifier/unified-operations-capabilities';
const provider = createOperationsCapabilities({
  ...ownerContext,
  owner: {command: '/owned/venv/bin/amplifier-unified-operations',
          args: ['--config', '/owned/operations.json']}
});
await provider.start();
```

`ownerContext` supplies public host callbacks `inspectSession`,
`readSessionContext`, `nativeControl`, `nativeControlExisting`, `createSession`,
`submitScheduled`, `waitForTurn`, and `onInvalidate`. `start()` launches only an
indexed due scheduler; idle/list reads never start native runtimes. Native
`operations.observe` host bridge routes to
`provider.observe(authorizedSessionURI, runtimeSessionId, event)`; runtime loss
routes to `provider.interrupted(uri)`. Advertise native host feature `operations`
only when that callback is actually wired. `close()` releases the owner and
marks interrupted submissions unknown without replay.

Optional callbacks: `readUserMessage(uri,id)` for bounded targeted authorization
history; absent it uses the host's bounded selected context and refuses unseen
older provenance. `questionDependency(uri,id)` must validate actual answers;
absence blocks dependent work. `operationSources(uri,{id?,limit,cursor?})` reads
external owners' bounded receipts without copying their authority. Each returned
record must contain its scoped sessionId and namespaced id, state/revision and
createdAt. `cursor` is `[createdAt,id]`, exclusive descending. Absent this callback
only native operation receipts appear. `notifySchedule(uri,run)` delivers a
persisted notification decision; delivery is optional and never fabricated.

The async `actionSchemas()` returns `{action:{description,schema}}`. `manifest`,
`read`, and `action` implement the public capability seam. Context origin is
trusted `ui` or `agent`; args never grant origin or session authority. All original
schedule and operation action names/required fields are retained. Runtime-specific
operations require negotiated native controls. Process write/cancel exclusively
use `nativeControlExisting`; a cold/retired runtime is never mounted to control a
historical process. Duplicate request IDs return their original durable receipt;
changed arguments/actor are rejected.

Topic `schedules`: `{schedules:{[uri]:rows},schedulePage:{nextCursor}}`, initial25
rows/last10 runs each. `schedule.list/read` accept `limit<=100,cursor` and return
explicit `nextCursor`; cursors and edit drafts belong to the client. Linked fresh
task sessions may read their source schedule as readOnly and report only their
own admitted run. Topic `operations`: `{operations:{[uri]:{operations,requests,
nextCursor,capabilities}}}`. `operations.list` accepts an opaque bounded cursor.
Output reads remain byte-bounded and expose gaps honestly. Topic records are
bounded768KiB; huge selected records require a narrower action read, not full app
state. Schedule run history remains on disk by default, with bounded page/due
queries; no boot full-history materialization.

Schedule preview binds task revision, host stop/execution/configuration revisions,
explicit time zone/DST policy and exact prompt. Agent mutations require the actual
user message, excluding scheduled inputs/question answers. The deterministic run
is durably claimed before common host admission. New-task creation uses reviewed
native configuration cloning and a stable creation command, never prompt replay.
A host-admitted receipt is not completion: the owner waits for the host's terminal
receipt, keeps waiting while workContinues, and preserves unknown results for
explicit reconciliation. Reports record evidence/compared-value digests without
turning an agent's unchanged claim into proof or completing the overall task.

Validation is isolated fixture acceptance, not live-model/account/deployment
acceptance. Legacy registration/history import is a separate distribution job;
existing native input/history authority remains unchanged. UI placement, actual
notification delivery, questions/MCP/worker receipt composition, and old schedule
migration require their corresponding registered owners/integration. No missing
callback is represented as a successful external action.
