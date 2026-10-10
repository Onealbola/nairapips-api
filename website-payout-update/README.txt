Replace BOTH current website dashboard files with the included trader_clean.html
and admin_clean.html. These are based on the current owner uploads of 10 October
2026. Website HTML publication is separate from the Render backend deployment.

Traders can submit during delayed account updates. The last verified payout
amount and its update time stay visible. Requests display Received / Verifying
Balance rather than an update-related rejection.

Admin sees Awaiting Balance Verification and Verify & Approve. The backend must
verify a fresh exact-account balance before approval, and blocks payment of
unverified requests. Requests remain in the normal history and can be cancelled.
