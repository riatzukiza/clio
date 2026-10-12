# Clio portable EDN admission: planning evidence

[Canonical Clio issue 1](https://github.com/open-hax/clio/issues/1) owns future
writer prevention. [Foresight issue 135](https://github.com/open-hax/foresight/issues/135)
owns append-only reconciliation of its historical corpus. The
[incoming three-point card](../agile/tasks/07600a92-821a-4b35-89cd-d38393b3c393-portable-edn-admission.md)
requires native planning qualification and lawful Rheos readiness before implementation.

## Exact mapping and isolation

Observed 2026-10-06: `riatzukiza/clio` is the accepted personal development fork,
with GitHub parent/network source `open-hax/clio`; both default main refs resolve
to `788cdd3434615a7932b924e68520dbf7f88408c2`, so there is no divergence or fork
history to synchronize. The accepted Foresight map is at personal sync PR3 head
`96a6dca24cb7a14b041bdd6e3e7922c568238da9`, `config/dev-origins.edn`.
Personal development publication precedes separately qualified origin release.

New independent clone: `clio-1-prevention-plan.git`; new worktree:
`/home/err/.codex/parallel-goal/child-prs-20261006/clio-1-prevention-plan-pr`.
The baseline has no AGENTS.md, local skill, board configuration, neighboring
card, workflow file or receipt ledger. GitHub lists no registered workflows,
main is unprotected, and auto-merge is disabled. These are observed baseline
facts, not protection qualification or authorization to change settings. No
inherited merge caller was found. This PR changes planning/evidence only.

## Actual canonical reproduction

The reproduction ran in a separate owned audit directory, with every acquired
Clio source byte compared against the current/pinned Git revision above. It used
Node v24.14.1, exact NBB1.4.207, the declared Malli0.16.4 source, dynaload0.3.5 and
fs-ext-extra-prebuilt2.2.9. Source/dependency copies, cache, schema directory and
empty disposable ledgers were private. The original runtime installation was
invoked read-only. No namespace was mocked or reader substituted.

Through `runtime/open` followed by actual `runtime/append!` and
`ledger/append-event!`, both programmatic values below returned `:appended`:

```clojure
(keyword "relation" "03f0493a-consumes-08b-route-pressure")
(keyword "relation" "2fd1f2d9-continues-261aa432-inventory")
```

Immediate canonical `ledger/read-ledger` rejected both newly emitted lines as
`:clio.ledger/invalid-edn`, caused by invalid keyword tokens. A letter-prefixed
`r03f0493a-consumes-08b-route-pressure` control appended and read one event.
The exact same three emitted files read one event each through the same Clio API
under historical NBB1.3.204. This is a reader-profile observation, not portable
validity or a proposed downgrade. See issue1 for the minimal real-API reproduction.

Retained local reproduction hashes:

| Artifact | SHA-256 |
| --- | --- |
| Probe | `18fb2a8b87e47e8de35758a499ea52ce7200896f02d8e05aa7035724c9867f94` |
| NBB1.4.207 output | `87a5cb16433ae289d75c0058c0340470e9854981d31eb5689927d7df93cb1709` |
| First emitted ledger | `be0d161d304a7f1981b8b054ed129900e1b9ddfeb5eb3391b705161a9d0448be` |
| Second emitted ledger | `d35939a9f54e91986a95daa03389d7a2fa348bc0871ef4d78c9f1a9ada650dd2` |

## Owning admission boundary

`domain.schema/validate-event!` validates historical schema, event identity and
`shape.canonical/canonical-form`. The latter turns keyword namespace/name into
strings. That hash representation accepts the observed identifiers, but does not
establish readability of their printed original tokens. `infra.ledger` then
writes `(pr-str event)` directly, without candidate serialization admission.
`shape.edn/read-one` correctly rejects the token through its strict single-form
reader. The [official EDN specification](https://github.com/edn-format/edn#symbols)
applies the identifier first-character restrictions to the name after a namespace
separator; keyword rules inherit those restrictions.

Proposed prevention belongs to the existing pure Clio admission law/shape seam
and existing writer adapters. Reuse the canonical reader at serialization edges;
do not replace it. A pure lexical data law must remain effective even under a
historically permissive reader. Preserve the fixed canonical hash protocol and
all lawful existing preimages. Review schema-store and projection print boundaries
explicitly, splitting substantial additional scope when needed.

Foresight's `archaeology.infra/append-plan` is deliberately not a writer. Its
relation schema checks keyword type, which alone cannot prove portable printed
spelling. Future domain ID authoring is a Foresight concern; generic writer
prevention belongs here. The exact executable invocation that created September
artifacts is unobserved: actor/commit metadata alone cannot establish that it used
this API. No historic row has been rewritten or automatically reconciled.

## Admission state

The card is incoming and no board projection, validation or transition was run.
Review must settle scope/estimate and the covered adapters, then Rheos must
perform the lawful ready transition. No implementation or fixed-test claim is
included. Current personal-account native CodeRabbit evidence reports zero
included reviews remaining and an adjusted allowance of one per hour. Do not
send manual requests without fresh allowance/cooldown evidence and exact-head
deduplication. Native automatic review may remain pending; unavailable or quota
responses supply neither approval nor round credit. Auto-merge remains off.
