# Account plan workspace

The landing and trader catalogues now use one NairaPips selector: model, account size and package, with a selected rules/price panel. Account rules and payout rules have separate tabs; percentage/currency controls convert only displayed targets and loss limits. Comparison shows the configured variants for the selected allocation. The default model prefers a currently open product; a trader's subsequent selections survive catalogue refreshes. Paused offers remain viewable with checkout disabled.

All prices, targets, drawdown, shares, reset terms and availability come from the existing public catalogue. The existing landing handoff and authenticated trader checkout receive the exact selected plan. No prices, saved limits, purchase switches or issued account terms are changed by this release.

Owner Package Studio now groups account navigation, package tabs, price/identity fields, account rules, payout rules, sales limits and a read-only live preview. The existing revision/CAS save, independent purchase switches, supported-rule gates, and owner-only authentication are retained. Standard/Premium settings remain accessible through the existing admin panel.

`account-plan-selector.js` is the common renderer. Hosted landing/trader scripts are served with this renderer prepended, so already-installed script links receive the redesign on refresh. New trader HTML also includes the renderer inline as a fallback. `tools/build_catalogue_delivery.py` aligns that fallback, the installer checksum and cPanel ZIPs.

Validation covers account/model/package switching, current prices and exact checkout identity, unit conversion without mutating contract percentages, rule tabs, comparison, keyboard navigation, live-update selection retention, owner pauses, read failures, mobile width, private settings edits/save/reload/conflicts, and a read-only owner preview. Existing purchase/assignment/cycle regressions also pass. No real customer financial transactions were submitted for testing.
