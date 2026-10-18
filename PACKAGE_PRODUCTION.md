# Customer packages and backend pricing

Primary interfaces: existing trader panel → Business → Build Your Package; existing admin panel → Business settings → Package prices & benefits. Shipping files are `website-downloads/trader_clean.html` and `website-downloads/admin_clean.html`, with scripts and styles embedded. Standalone `/build-package` and `/business-controls` remain available for diagnostics.

Customer choices request a read-only `/trader/package_price` calculation after a 220 ms debounce. All calculations use the saved backend configuration. The browser sends choices only. Disabled choices stay hidden once the owner saves configuration. Before the first tariff is configured, traders can practice with known choices and the display says Price coming soon. Unpriced choices cannot be purchased. Fixed NGN benefits and percentages of the capital base are supported; percentages are added together, fixed additions are then added, the result is rounded up to NGN100 and the saved minimum is applied. Existing percentage tariffs retain their behavior.

Before launch, traders can configure and save a non-financial draft on the same browser; both the payment quote and order routes reject new submissions. After launch and verified engine readiness, proceeding to payment creates a signed-in trader's ten-minute quote. The order submission accepts only that quote, its rules hash, a stable checkout key and payment evidence. Atomic database checkout records immutable contract terms. Browser prices, other traders' quotes, changed catalog versions and expired quotes cannot be submitted as new purchases. An uncertain response retains the same checkout key for reconciliation. Selection changes never create purchase or quote rows.

Package approval and exact account delivery use the package entitlement RPCs. Existing purchased contracts remain independent of subsequent tariff changes or paused sales. Legacy assignment is blocked for package purchases.

## Current release prerequisites

No production tariff was saved when checked on 18 October 2026. The owner will manage and change prices directly in the backend; no price list is required in chat. Prices and availability are editable at any time. Example rehearsal prices must not be published as a real tariff.

The production schema is installed. Customer sales remain closed until runtime verification passes for account lifecycle/renewal, payouts, reset allowances, and the running monitoring/DD fleet. These consumers have not been verified and are not marked ready. The release checkbox cannot substitute for implementing or deploying them. Verified fresh matching broker stock is also needed for actual delivery. No production purchase, approval, broker assignment or payout is executed by development or browser fixtures.

The new server flow and customer page do not install changes on cPanel or the Windows VPS. Deployment of the matching runtime consumers is still required before real package sales can open.

## Local validation

Never import or run production `app.py` during local tests; it starts financial workers against production bindings.

Use the existing API virtual environment with `python -m unittest discover -s tests -p 'test_package*.py' -q` and `python -m unittest discover -s tests -p 'test_production_controls.py' -q`. Browser fixtures run with `node tests/test_package_customer_browser.js`, `node tests/test_package_lab_browser.js`, and `node tests/test_owner_controls_browser.js`. All fixtures use loopback/in-memory storage and no real customer credentials or financial records.

## Engine integration progress

The API payout-share resolver and monitoring rule readers now have exact immutable package-contract bindings. A package cannot fall back to a current plan or another purchase. Package accounts are blocked from the legacy target writer and legacy assignment picker while the atomic lifecycle adapters are unfinished. The package DD candidate and durable watermark tests are in the monitoring repository; Windows deployment and verification remain outstanding. These changes do not constitute a completed engine launch.
