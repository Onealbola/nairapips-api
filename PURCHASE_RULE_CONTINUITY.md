# Purchase rule continuity

Issued legacy accounts retain the exact purchase snapshot through phase progression,
resets and paid funded renewal. Current catalogue edits and generic defaults cannot
substitute another drawdown, daily limit, phase target, journey or payout controls.
Package accounts continue to use their immutable package contract and stage rules.

`purchase_rule_continuity.py` binds legacy assignment before insertion. A request-local
paid-renewal context excludes only the exact predecessor from the duplicate check;
paid-event, owner, capital, stock and replay guards remain required. Finalization
validates a partially issued child's rules before monitoring or delivery.

`purchase_rule_continuity_guard.sql` adds an invoker trigger on account creation and
rule changes. It restores purchase-bound metadata, prevents purchase rebinding and
checks exact package contract identity and stage rules. Unchanged telemetry updates
are allowed for historical incomplete records; new unbound assignments require an
explicit manual-plan snapshot. The guard does not rewrite historical accounts.
The migration was applied through Supabase with the temporary-only verification in
`tests/purchase_rule_continuity_guard_check.sql` in the same transaction.

Reporting audits active accounts even when every rule marker was lost. Missing
purchase evidence requires review. A repaired daily/trailing account also requires
an exact fresh broker-state fingerprint before reporting verified enforcement.
No replacement account receives invented historical broker state or an automatic
breach based solely on a screenshot or a duplicated trade feed.

## Verification

- Exact one-phase, two-phase and instant journeys; static/trailing modes; custom
  daily/payout/withdrawal/reset terms; owner, capital and paid-event isolation.
- Actual assignment/finalization functions tested with intercepted database writes.
- Package reporting verifies immutable stage rules rather than generic purchase DD.
- Database temporary-copy checks verify insert, phase change, purchase detachment,
  cross-owner and capital guards without writing public financial records.
- Full pre-rollout run: 928 tests. Four existing failures reproduced on the unchanged
  production revision (two stale panel/package fixtures, an approval AST fixture
  missing a helper and a dashboard AST fixture missing requests). Seven local-socket
  errors were rerun with socket permission; all ten HTTP concurrency tests passed.

## Existing records

The identified funded replacement lost its purchase's daily-limit/withdrawal markers
and full phase journey. Restore only verified metadata against its exact owner and
purchase, record a private audit, and retain its account/breach history. A missing
historical broker journal remains an explicit review requirement. Nine active
unbound legacy accounts and one incomplete legacy purchase need individual review;
never guess their purchase links or silently apply a universal rule.
