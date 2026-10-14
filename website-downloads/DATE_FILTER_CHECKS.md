Date-filter integrity update

The displayed cards, Results by plan, Total row, paid amounts and purchase table
are now all derived from the same date-filtered purchase rows. Server-wide totals
cannot populate a selected period's cards. A response for another period is
rejected rather than shown. Dates persist immediately on input before debounce,
so background re-rendering cannot restore the previous date selection.

Tests include intentionally wrong all-time summary and plan totals with mixed
old/in-period/future rows. Each displayed count and money total matches the
selected cohort; per-plan totals sum to headlines; duplicate identities and
invalid amounts fail. Automatic input refresh and stale response protection pass.
Read-only live check for 10–13 October 2026: 10 sold, 7 ever breached, 1 recorded
payout recipient, amount recorded paid 38,488 NGN. This is a diagnostic example,
not confirmation of the user's unspecified selected four-day period.

The report remains a purchase cohort report: dates identify purchases sold in the
period, and outcomes follow those purchases up to today. It is not an event-only
cashflow report of payments or breaches occurring during the selected period.
No business records, payment actions or monitoring/assignment rules are changed.
