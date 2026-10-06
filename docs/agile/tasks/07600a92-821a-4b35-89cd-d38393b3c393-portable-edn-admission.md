---
uuid: 07600a92-821a-4b35-89cd-d38393b3c393
title: Reject nonportable identifier serialization before ledger append
status: incoming
priority: high
points: 3
labels: clio, serialization, portable, ledger, planning
---

## Context

[Clio issue 1](https://github.com/open-hax/clio/issues/1) records a concrete
current-writer defect at `788cdd3434615a7932b924e68520dbf7f88408c2`:
`runtime/append!` accepts programmatic numeric-leading relation keywords and
returns `:appended`, then `ledger/read-ledger` rejects the emitted row under
NBB 1.4.207. Historical NBB 1.3.204 reads those same bytes; that permissiveness
is not portable EDN proof. [Foresight issue 135](https://github.com/open-hax/foresight/issues/135)
separately owns explicit reconciliation of its immutable archaeology corpus.

There is no board configuration or existing card at this baseline. This incoming
Markdown artifact is supported authoring input, not evidence of board admission,
validation or a ready transition. Implementation waits for qualified planning
review and an actual lawful ready event through Rheos.

## Outcome

A candidate with a nonportable printed identifier is refused before any event
bytes reach an authoritative ledger. Lawful candidates still append and round-trip
through the canonical reader without changing their identity or hash protocol.

## Scope

- Specify a pure portable-identifier admission law in the existing Clio law/shape
  seam. Cover keyword and symbol namespace/name components, recursively in the
  supported data collections, using the EDN lexical contract rather than the
  current host reader's permissiveness.
- Compose that admission with existing schema, identity and canonical-data laws.
  At the existing ledger adapter, validate candidate serialization through the
  canonical EDN boundary before committing bytes under the existing inode lock.
- Assess schema-store and projection serialization boundaries. A shared applicable
  check may be reused there; any materially larger change becomes a linked card
  and explicit reviewed coverage decision, not a silent omission or scope expansion.
- Add shared law fixtures first in red, then the minimal pure law and existing
  adapter integration in green. Keep runtime-specific operations in extern/infra.

## Non-goals

No new reader, parser, serializer authority, ledger transport or Foresight bypass.
No relaxed one-form/EDN checks, runtime downgrade, production operations, package
policy changes or schema hash-protocol migration. No rewrite, normalization,
replacement IDs or automatic admission of historical unreadable corpus records.

## Acceptance criteria

1. Both observed programmatic keyword names fail with structured diagnostics
   before append. Nested map keys/values, set members and sequential elements
   cannot bypass the law. Invalid symbol and namespace/name lexical mutations are
   also covered; permitted identifier punctuation has positive fixtures.
2. The pure law gives the same verdict under NBB, compiled CLJS and the declared
   JVM/Babashka runner. Historical permissive NBB results remain recorded and do
   not waive the portable lexical requirement.
3. Real-adapter fixtures assert exact unchanged ledger bytes on refusal, including
   a valid existing prefix. Accepted controls read back equal through
   `clio.shape.edn/read-one` and `clio.infra.ledger/read-ledger`.
4. Existing retry idempotence, event-ID collision, stream-slot conflict, lock
   ownership, prefix preservation, causal validation and historical schema checks
   remain covered and pass. Candidate serialization checks do not release a held
   inode lock through an extra path-based file read.
5. Lawful existing canonical preimages and schema roots remain byte-identical.
   Strict reader behavior stays intact. Historical artifacts retain every byte,
   event ID, schema reference and causal fact; root corpus reconciliation remains
   owned by Foresight135 and its separate reviewed contract.
6. Review settles the three-point estimate, supported recursive/lexical boundary,
   diagnostic precedence and adjacent-writer coverage before Rheos readiness.
   If adjacent adapters require a larger slice, split and link it explicitly.
7. From a cold isolated implementation revision, run existing lint, NBB, Shadow
   CLJS and Babashka suites and the real append/read-back fixtures. Record exact
   runtimes, revision, native run IDs and actual test/assertion totals in receipts.

## Verification

The [planning evidence](../../notes/portable-edn-admission-plan.md) links the
actual canonical reproduction, source ownership and retained raw fixture hashes.
Those observations demonstrate the current defect, not a repaired red/green run.
Private fixtures never touch shared caches, services, runtime installation or
historical corpus files. Candidate jobs need no signing/deployment secret.

## Risks

Stable canonical hashing does not imply printable EDN. A host round trip may
accept nonportable tokens. Traversal must cover keys as well as values without
executing arbitrary objects. Adjacent writers may need a separately estimated
slice. This repository has no configured board/ready authority yet; a local FSM
or alternate board implementation cannot clear that prerequisite.
