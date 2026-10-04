# Amplifier Unified operations capability

Independent product owner for durable questions, schedules and process evidence. Python
uses `amplifier-scheduling` and `amplifier-operations` through their public package
APIs. No AppService, native Python engine or host-private imports. MIT provenance:
original `amplifier_web/{schedules,operations,operation_requests}.py` action and
admission contracts at `828d2bff97a77fbdbd0f06429369f7ff1f4abf42`.

Build with `uv sync --directory python`, `npm ci`, `npm test`. The development
lock resolves optional libraries from Foundation main with explicit package
subdirectories; the receipt records the qualified revision without a standing pin.
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
older provenance. The built-in question owner validates dependencies against saved answers. `operationSources(uri,{id?,limit,cursor?})` reads
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
notification delivery, MCP/worker receipt composition, and old schedule
migration require their corresponding registered owners/integration. No missing
callback is represented as a successful external action.

Questions preserve six shared action names and a selected-session `questions`
topic: `{questions:{[uri]:records},questionPage:{nextCursor}}`. The initial topic
contains at most five complete records; `question.list` accepts status, limit<=50,
and opaque scope-bound cursors, with a 512KiB complete-record page bound. Clients
keep editing drafts locally and query pending questions explicitly. Answers use
revision checks, an exact user/voice evidence reference for agents, and durable
command identities. Saved answer and runtime delivery are separate states.

Wire `submitQuestionAnswer(uri,input)` through public host admission and route
`questions.delivery.admit` to `authorizeQuestionDelivery(uri,args)` using the
authenticated session, never a supplied selector. Native dependency admission
uses `questionDependency(uri,id)`. Advertise native feature `questions` only
when these callbacks are wired. The native proof binds exact saved text, input
identity, question revision and answer provenance. No generic native control
may bypass it. Busy admission may retain a rejected answer; an uncertain
acknowledgement or restart never resends one. Host acceptance is not execution
completion. Legacy question import and delivery into an active turn remain
separate integration work.

## Quiet watches

Observations are advertised only when the composer supplies `readTaskState`,
`qualifiedObservation`, and `submitObservation`. The first is a passive ACP storage
read; the second belongs to the installed MCP broker; the third is a typed host
admission. Generic prompts or native control are never substitutes.

The `observations` topic contains five selected-session watches plus a cursor.
`observation.list`, `observers`, `read`, and `report` page at most 50 complete records
and 512 KiB. Due work uses indexed queries, four concurrent jobs, bounded source
reads, and short receipt polls. Watch metadata also appears in `operations.list`;
its terminal state describes source observation, not model or product completion.
Control a watch through its owning actions, not generic process cancellation.

Pending results are quiet. Task, interruption, execution, human-request, question,
source, and account authority are checked again after each read and before a typed
handoff. A terminal result is committed before delivery. Cold runtimes leave the
result saved. Only an explicit host `executed:false` / `retryEligibility:waiting_worker`
receipt can make an already reserved input eligible again, with the same input ID.
Unknown admissions never replay. Restart abandons interrupted reads and preserves
uncertain deliveries. Expiration does not imply underlying work cancellation.

Agent-created watches must cite an actual host-admitted human request; imported or
generated messages cannot grant authority. Operator source qualification is a
separate explicit action. Automatic presentation currently refuses requests until
an actual client grant is connected. Legacy watch databases are not yet imported.

Qualification: 36 Python cases and three real stdio owner tests pass, including
25,000 cold records, typed reentrant admission, task/stop/source/cancel/expiry races,
known cold refusal, and uncertain-restart recovery. Actual MCP SDK and native
runtime paths have separate owner checks; the composed graph has its own acceptance.

`observation.request {requestId,operation?}` defaults to creation and also reads
exact pause/cancel/resume receipts. `receipt.appliedRevision` is immutable; `watch`
is current state and may have changed later. Pre-receipt candidate records return
`legacy_observed`, never a claim that a later command caused the current state.
The native authorizer verifies the exact host `observationAdmission` proof,
including the sole execution revision advance caused by its own typed admission.
A known no-effect receipt can retain only its explicitly proved revision advance;
any intervening unrelated execution still revokes the watch.

Transfer composition may call `exportTransferEvidence`, `stageTransferEvidence`,
`activateTransferEvidence` and `readTransferEvidence` directly. These are private
owner hooks, never public client actions. The portability owner verifies signed
pairing, byte-preserving evidence and the release before staging or activation.
The returned owner is `unified.operations`; exports cap at 1 MiB to preserve the
4 MB private JSON transport bound. Omitted history is counted explicitly and
requires preview acceptance. Each transfer ID seals one exact export.

Imported schedule, operation, question and watch records live in a separate
immutable history database. Source-host plus transfer-ID namespaces distinguish
original IDs. Import never inserts into scheduler, question-delivery, operation
control, or observation outbox tables. Unknown source outcomes remain unknown.
Reads return selected bounded pages; history confers no execution authority.

`readQuestionAttention(session)` exposes at most 64 pending question IDs from a
covering disk index for a selected coordination wait. It never loads question
bodies or starts native work. `questionsComplete` and `questionsTruncated` make
overflow explicit; a truncated or unavailable owner is not evidence of no
attention. Composition combines this with actual host approvals and task state.

Schedule mutation recovery uses `schedule.request {requestId}` in the selected
session. `requestId` is the original capability commandId. The returned
`receipt` is either null or `{commandId,status:"committed",result}` containing
the exact saved schedule/run result, even after later edits. Inspection does
not invoke native callbacks, start work or publish a new schedule snapshot.
Clients retain their pending intent privately and inspect without reissuing a
create/update. A missing receipt alone does not prove upstream admission failed.
Schedule schema/review validation and rolled-back Foundation mutations return a
typed known refusal (`accepted:false`, `executed:false`). Response publication
errors after a successful commit remain unknown and recover through the original
owner receipt. A client may settle a missing owner receipt only with an exact
scoped terminal failed host receipt; it never repeats the original command.

## Retained legacy evidence

The explicit offline `python -I -m amplifier_unified_operations.migrate` command
accepts `--source` (a retained SQLite database), `--destination` (the independent
owner directory), `--source-session`, `--session` (the reviewed AHP mapping),
`--kind`, and optional `--after`/`--limit`. It refuses a running destination owner.
Only selected-session records are read, in pages of at most 50; each record is
bounded before hydration. Original source bytes are never changed. Conflicting
identity mappings, malformed and oversized rows remain classified and retained.

Imported schedule/run, operation/request, question and observation records are
immutable history, exposed through `operations.history.list/read`. They are never
inserted into active stores. Source active/pending/unknown states cannot resume a
schedule, deliver an answer, recreate a process or confer tool authority. Process
output chunks, event journals, opaque unscoped receipts and unscopable malformed
observation rows remain in the source and are explicit omissions. No host startup
import or automatic legacy work replay is supported.

Schedule preview optionally supplies a bounded trusted `destinationSummary`
(engine and workspace). The reviewed configuration hash remains an admission
guard; raw provider credentials or private configuration are not exposed.
This uses Foundation scheduling's public `command_receipt` API.

## Held quiescence for updates and recovery

`quiescenceParticipant(ownerId)` is a trusted composition port, separate from
advertised user/agent actions. Its private Python owner durably closes intake
only when admitted effects, background scheduler ticks, and detached schedule
and observation jobs are absent. A future due schedule is retained and pauses
while the fence is held. Active native processes are independently covered by
the host's native retirement proof; historical unknown receipts do not imply
live work and are never retried by this port.

Receipt and bounded evidence reads remain available through the exact
`quiescenceAccess` declarations. Preview actions that may contact a native task
owner are not classified as passive reads. A retained fence survives owner
restart, also blocks offline legacy imports, and can reopen only with the exact
host-verified outcome proof (or the host's pre-effect admission refusal). An
unknown outcome keeps intake closed. `reconcileRelease` never reruns acquisition
or a mutation. Reusing a released fence is rejected.

## Persistent service stop and resume

The trusted `quiescenceParticipant()` advertises `serviceStop: {version: 1}`.
For `purpose: 'service-stop'`, the host supplies the normal fence context plus
`serviceIdentity: {installationId, dataScope, ownerId, instanceId, releaseDigest}`.
The owner copies and retains the complete identity with its durable intake fence;
restarting a process does not open intake or replay work. The host authenticates
platform evidence. This participant validates its exact binding, not a PID,
caller-provided claim of idleness, or socket presence.

A conclusive release needs `kind: 'service-lifecycle'`, the retained `expected`
identity, the exact `observed` identity, and the ordinary verified fence receipt.
`outcome: 'ready'` requires a distinct instance, `serviceOutcome: 'resumed'`,
`resumeCommandId`, `exitReceiptId`, and `readyReceiptId`. `outcome: 'unchanged'`
requires the same instance, `serviceOutcome: 'stop-refused'`, and
`refusalReceiptId`. The two receipt sets cannot be mixed. A changed release
identity, proof, or outcome refuses; exact completed retries are passive.
Generic update/recovery evidence cannot release a service fence.

`{kind: 'admission-refused'}` is restricted to the original live lease's partial
acquisition rollback. It is not a restart reconciliation mechanism. An unknown
outcome keeps intake closed, and passive inspection/receipt routes stay usable.
No service stop, platform process signaling, authenticated supervisor, or remote
provider safety is implemented by this participant itself.

The private Python owner advertises this support only when the installed optional Foundation `DurableIntakeFence.SERVICE_STOP_VERSION` is 1. The Node bridge negotiates it before acquisition; an older library is a known refusal. The lock tracks current actions, scheduler work and detached owned jobs, not historical uncertain receipts alone. Foundation remains an optional library dependency and is not imported by generic host code.


## Held retention inspection

The actual quiescence participant advertises `retentionHide: {version: 1}`.
Acquire it with purpose `retention-hide` and the exact coordinator fence context.
The returned live lease exposes
`inspectRetentionReferences({sessions: ["ahp-session:/..."], limit: 101})`.
The family must contain 1–101 distinct explicit session URIs. The result is
`{coverage: "complete" | "partial", protected: [{session, reasons}], omissions}`.
Only the original held lease can inspect; release, uncertainty or replacement
invalidates that authority. Release reconciliation requires the same authenticated
coordinator proof and never retries effects. A pre-effect admission rollback is
allowed only on the original live lease. The purpose grants no mutation rights.

Inspection reads bounded indexed owner metadata while intake remains closed.
It never starts a native worker, scans canonical histories, deletes product records,
or interprets absence of a runtime as absence of deferred work. `complete` describes
this owner's reference coverage, not permission to hide or delete a conversation.
The coordinator must hold every configured owner, check all results, and use the
native history owner's separate preservation-first hide boundary.

Future or paused schedules, unresolved scheduled admissions, required unanswered questions, uncertain answer delivery, unfinished operations, watches and pending observation delivery protect their explicit conversations. These are indexed reads; no due work is run to determine eligibility.

## Managed-file disposal protection

`quiescenceParticipant.managedFiles = {version: 1, preservesCanonical: true}` is a separate contract from `retentionHide`. It holds the real owner intake with purpose `managed-files-disposal`, preserving every owner record. The acquired lease exposes:

```ts
inspectManagedFilesReferences({
  sessions: [rootSession, ...descendants], // explicit AHP URIs, at most 101
  limit: 101,
  allocation: {allocationId, executionDirectory, allocationHash, treeHash, entryCount, bytes}
}) // {coverage: 'complete' | 'partial', protected: [{session, reasons}], omissions: [...] }
```

The allocation comes from the trusted host's native-reviewed managed allocation, never a browser path. It is bound with the exact selected family on the first inspection of the live lease; a different review is refused. This read does not grant file deletion. The coordinator must bind release to its exact durable managed-files effect receipt, keep unknown fences held, and refuse partial/protected coverage. Lease inspectors expire before release begins, including unknown release; admission-refused rollback is available only on the original live lease. Restart reconciliation requires the existing exact verified release proof.

Future active/paused/review-needed schedules, active schedule admissions, required unanswered questions, answer delivery, uncertain operations and observation watches/outboxes protect selected conversations. Completed process output is stored by canonical native/operation owners, not lazily read from the execution directory. No task is started to inspect references.

Inspection uses bounded selected metadata or indexed overlap probes, not native transcripts, a global history projection, or worker/model startup. Existing retention-hide and service-stop lifecycles are unchanged. The package tests cover the independently installed owner transport and held/unknown/exact-release behavior; full host/native/browser disposal acceptance remains a composition responsibility.

### Trusted Operations-store recovery images

A freshly acquired recovery lease may expose `captureSnapshot({commandId,
privateContentReviewed: true})` when the installed optional Foundation Operations
library advertises snapshot version 1. The coordinator must authenticate the
caller and review private-content inclusion. Retained fences after restart never
grant a fresh snapshot lease, and releasing a lease retires its capture authority.
This hook composes with the existing product archive/recovery coordinator; it is
not a separate backup action or an activation API.

Capture covers exactly `intake`, `schedules`, `operations`, `questions`,
`observations`, `transfer` and optional `legacy` SQLite stores. Writers, scheduled
admission and observation work remain held for the complete set. Unknown storage
in the owner root refuses capture. Committed WAL content becomes sealed SQLite
images; the original database/WAL bytes and canonical history remain preserved.
Images total at most 64 MiB and capture has a thirty-second deadline. Original
commands retain an unknown receipt before any image effect, and exact retries
inspect that receipt without repeating capture. Completed receipts identify each
image, missing stores and the manifest digest.

Trusted composition can use `inspectOwnerSnapshot(commandId)` and bounded
`readOwnerSnapshot({commandId,store,offset,limit})` windows (at most 64 KiB).
`amplifier_unified_operations.snapshots.restore_inactive` requires the exact seven
store census, an authenticated reviewed manifest digest and a caller-held guard;
it restores into a new inactive destination without overwriting source state or
starting work. The original intake fence and unknown outcomes remain durable.
A hash alone does not authenticate an archive. Complete product backup/restore
remains false until the product coordinator also accounts for canonical native
history, external process authority, retained snapshot artifacts, every other
owner, and the complete signed runtime-code graph. No external-root discovery,
new process activation, TTL fence release or replay is provided here.

Artifact export verifies a selected sealed file against its original receipt and
manifest digest once per opened descriptor. At most eight descriptors are retained
in a bounded LRU, sufficient for one seven-store set plus its manifest. Each
window checks the regular single-link file's descriptor and path identity before
and after reading, including inode, size, modification time and change time. A
mutated, replaced, shared or missing file refuses further reads even if its old
modification time is restored. Closing or restarting the owner discards every
descriptor. No image bodies or history records are cached. This relies on the same
trusted owner-private sealed namespace as Foundation capture, not protection from
same-user kernel tampering. Complete single-set export reads each image once for
verification and once for its windows, rather than hashing the whole image on
every 64 KiB page.
