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

Confirmed root cause from the user's screenshots: date fields displayed
03/10/2026 and 12/10/2026, while REPORT PERIOD echoed 2026-03-10 to 2026-12-10.
The browser interpreted month/day/year: 10 March through 10 December. September
rows and all-time-looking totals were therefore within the request's actual
nine-month interval. Earlier filter checks missed this locale ambiguity.

Replacement date fields explicitly accept DD/MM/YYYY as text, validate Gregorian
calendar dates, and convert 03/10/2026 into 2026-10-03. Report period displays
3 October 2026 → 12 October 2026. Invalid/partial dates stop the request instead
of falling back to all time. Strict ISO entry also remains unambiguous. Tests
cover leap years, invalid dates, and the user's exact values. Real Chromium with
en-US locale sends the intended October interval and excludes September rows.
Read-only live calculation for 3–12 October: 32 bought, 22 ever breached,
6 paid recipients, amount recorded paid 392,744 NGN. No production writes.

Admin loading recovery V226: failed core requests no longer become empty arrays.
Initial missing data displays unavailable, not zero. A failed refresh keeps the
last successful snapshot and timestamp; overlapping refreshes share one request.
Fallback requests run at most two at a time. Action Centre refreshes also share
one in-flight request and retain old data on failure or invalid responses. The
backend separately reserves capacity for bootstrap and Action Centre, so normal
traffic and operations reads cannot consume the bootstrap's request lane. The
DD/MM/YYYY sales report, date-scope checks and totals row remain preserved.

Purchase approval V229: last failure remains visible in the Approve Purchase
modal instead of disappearing behind a transient toast. Blank MT5 choice is
labelled Auto-select a matching Phase account now. Same-purchase submit clicks
are guarded; no mutation is automatically repeated. Waiting-for-inventory cannot
be labelled assigned, and success uses the real server-returned MT5 rather than
a rejected browser candidate. A successful mutation with failed table refresh
retains its confirmed result. Browser auto age threshold follows the server's
max_age_days. Existing report/date and failed-load safeguards remain intact.
