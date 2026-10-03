# Amplifier Unified operations vision

This is the local application of the approved [family vision](https://github.com/microsoft/amplifier-unified/blob/main/docs/architecture/VISION.md)
and [repository ownership map](https://github.com/microsoft/amplifier-unified/blob/main/docs/architecture/repositories.md),
reviewed from the shared checkout at `625af2f2832fafc8152f6078061d626f160362a4`.
It records inherited direction, not a new ratification or an acceptance receipt.
The [v1 contract entry](../contracts/operations.v1.md) connects it to executable checks.

## Purpose and desired outcomes

People can schedule authorized work, inspect durable operations, resolve questions
and watch for meaningful changes without keeping an interface connected. Accepted
work and its evidence survive transport loss and restart; uncertain work is never
repeated to discover whether it happened. A saved answer, admitted input, completed
turn and completed task remain distinct facts.

Deferred work and unresolved decisions protect their conversations and managed
files. History remains available without keeping dormant runtimes or a global
application snapshot resident. Small updates and selected queries do bounded work.
Quiet watches remain quiet until a qualified meaningful result or required action.

Question handling should preserve the meaning of required decisions wherever work
is running. Existing idle delivery and active-turn refusal are implementation facts,
not a redefinition of that desired experience. Active-turn delivery still needs a
reviewed host/native contract carrying exact authority and provenance.

## Scope and ownership

This repository owns Unified scheduling/admission policy, operation presentation
and receipts, questions, quiet observations, explicit historical-only imports and
its held maintenance participant. It composes Foundation's public scheduling and
operation libraries. The host owns common execution admission and AHP delivery;
the ACP adapter owns native composition, execution and canonical history. Clients
own drafts, placement and rendering; MCP owns qualified source/account observation.
Distribution owns complete composition, release and deployment evidence.

It does not own a replacement AHP/ACP protocol, a second native resume store,
Foundation mechanisms, automatic historical replay, client-local draft storage,
filesystem deletion authority or an always-hot global service tree. Full native
Core/Foundation composition remains behind ACP; extraction does not authorize a
substitute lite engine or silent feature retirement.

Changes to shared meaning identify the affected family clauses and consumers
before integration. New delivery or authority semantics remain proposals until
approved by their owners and the product direction; passing tests cannot approve them.
