# Unified operations capability contract — v1

Local entry for the [vision](../docs/VISION.md), existing public APIs and inherited
family obligations. This packet introduces no new wire schema, execution authority
or product scope and does not self-ratify a future feature. Reviewed family source:
`625af2f2832fafc8152f6078061d626f160362a4` in the approved shared checkout.

## Authority and interfaces

Upstream [AHP](https://microsoft.github.io/agent-host-protocol/) and
[ACP](https://agentclientprotocol.com/protocol/overview) own their protocol semantics.
This owner supplies optional product capabilities through the public host seam;
it is not an AHP server or an ACP runtime. Reference definitions rather than copying them:
[Node interfaces and manifest](../src/index.ts), Python
[schedule](../python/amplifier_unified_operations/schedules_schemas.py),
[operation](../python/amplifier_unified_operations/operations_schemas.py),
[question](../python/amplifier_unified_operations/questions_schemas.py) and
[observation](../python/amplifier_unified_operations/observations_schemas.py) schemas.
The [README](../README.md) documents callback wiring, bounded pages, import CLI and
trusted quiescence/transfer hooks. Those private hooks do not grant client authority.

Family references below retain their own ownership:
[HP](https://github.com/microsoft/amplifier-unified/blob/main/contracts/host-protocol.v1.md),
[AP](https://github.com/microsoft/amplifier-unified/blob/main/contracts/agent-protocol.v1.md),
[WS](https://github.com/microsoft/amplifier-unified/blob/main/contracts/working-set.v1.md),
[CB](https://github.com/microsoft/amplifier-unified/blob/main/contracts/component-boundaries.v1.md),
[CS](https://github.com/microsoft/amplifier-unified/blob/main/contracts/client-state.v1.md),
[CX](https://github.com/microsoft/amplifier-unified/blob/main/contracts/client-experience.v1.md),
[CD](https://github.com/microsoft/amplifier-unified/blob/main/contracts/client-development.v1.md),
[MA](https://github.com/microsoft/amplifier-unified/blob/main/contracts/maintenance.v1.md).

## Inherited promises and falsifiers

| Local promise | Family basis | Broken when | Executable checks |
| --- | --- | --- | --- |
| UO1: Scope actions to authenticated session and actor; require exact saved authorization and reviewed dependencies before effects. | HP1/5, AP1/3/5, CS1, CB2/3 | Supplied args grant another session, question answers grant unrelated consent, or a generic native control bypasses authority. | [Owner](../python/tests/test_owner.py), [questions](../python/tests/test_questions.py), [observations](../python/tests/test_observations.py), [stdio bridge](../test/owner.test.mjs) |
| UO2: Persist original command identities and outcomes; distinguish saved, admitted, terminal and unknown. Unknown/restart never automatically repeats effects. | HP4/6, AP4/6, CS4/7, CX2/3/6 | A lost acknowledgement starts a second process/turn, saved answer is reported as completed work, or cancellation request invents a terminal result. | [Owner](../python/tests/test_owner.py), [questions](../python/tests/test_questions.py), [installed native composition](../test/native.integration.test.mjs) |
| UO3: Read selected indexed pages and bounded output; passive inspection stays cold. Small changes do not project unrelated histories. | HP2, WS2–5, CS2/3, CB1 | Listing starts a dormant runtime, reads hydrate all historical records, or an output gap is concealed. | [Question cold/attention checks](../python/tests/test_questions.py), [observation scale checks](../python/tests/test_observations.py), [owner output checks](../python/tests/test_owner.py) |
| UO4: Preserve canonical records and exact imported evidence. Explicit legacy import is historical-only, with omissions reported. | WS1/6, AP6, CS7, MA10/11 | Import resumes a schedule, delivers an old answer, recreates a process, mutates source bytes or hides unsupported omissions. | [All legacy collections](../python/tests/test_migrate.py), [transfer evidence](../python/tests/test_transfer.py) |
| UO5: Quiet observations recheck task/stop/source authority and distinguish source result from model/task completion. | HP5/6, AP4, CS9, CX6/7 | Pending results become generic prompts, unqualified accounts confer authority, or unknown handoff replays. | [Observations](../python/tests/test_observations.py), [stdio observation](../test/owner.test.mjs) |
| UO6: Hold real intake through maintenance; retain deferred references and uncertain fences. Reads do not grant disposal authority. | CB7, MA1–6/11, WS1/4, CS7 | A future schedule or unresolved decision loses its files, a replacement process opens intake, or altered proof releases a held fence. | [Quiescence](../python/tests/test_quiescence.py), [retention](../python/tests/test_retention.py), [managed files](../python/tests/test_managed_files.py), [installed service lifecycle](../test/service-stop.test.mjs) |

## Consumer obligations

Host composition supplies authenticated scope, common admission, terminal receipt
reads and existing-runtime process controls through the declared callbacks. Advertise
native features only when the actual callbacks are connected; optional absence is
explicit. Maintain exactly one canonical native execution owner. Never mount a cold
runtime to write/cancel a historical process. Scope external receipt sources and
source/account qualification to their public owners; do not copy their authority.

Clients retain drafts and unresolved intent privately, inspect original owner/host
receipts and display saved, rejected, unknown and completed states distinctly.
Do not translate an unavailable question delivery into steering or another prompt.
Use declared page/byte limits and cursors; capability failure stays scoped.

Maintenance composition covers every configured participant, binds exact reviewed
scope and outcome proof, and refuses partial/protected coverage. This participant's
inspection is not deletion permission. Distribution records compatible installed
peers, package hashes and fresh-worker adoption separately (CB4–6, CD1–6).

## Acceptance and open gaps

[Installed qualification](../qualification/installed-native.json) records 62 Python
and eight Node checks with no skips for the exact implementation candidate
`949f66e758a58265aca550e617952eef98f97a60`, including actual Core/Foundation
execution with an offline provider. This documentation successor inherits that
evidence only for byte-identical implementation and test files; no tests were rerun
for prose. Source/fixture, installed peer, browser/device/account and deployed
adoption evidence remain separate (CD5/6).

Active-turn question execution is **unimplemented**, under the current native
`questionTurns.active:false` boundary. Saving/refusing busy delivery is qualified;
it does not satisfy active delivery. A future operation preserving original-generation
authority and answer provenance is a **proposal requiring cross-owner review**, not
an obligation ratified by this document. No fallback or replay is authorized.

Actual MCP/native quiet-watch composition, notification delivery, browser/TUI
presentation, physical devices, real provider/accounts, Linux service activation
and live adoption need their separate owning acceptance. Offline historical import
does not establish production user-data migration. No desired promise is weakened
because those checks are absent, and the six pending family F18 decisions remain pending.
