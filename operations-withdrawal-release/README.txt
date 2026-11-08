WINDOWS VPS OPERATIONS UPDATE — OPTIONAL FUNDED PROFIT CAPS
No customer settings or prices are changed by installing these files.
Legacy accounts retain 15%. New accounts use their purchased per-plan cap;
disabled cap means no cap-based close. Evaluation targets and DD ownership stay
unchanged. Minimum payout and minimum profit thresholds are enforced by the API.

Extract to a staging directory on the VPS. CHECK_ONLY.cmd confirms the exact
running Operations engine path and reviewed original file hash. INSTALL_UPDATE.cmd
backs up and installs code only; it does not stop or start any process.
Restart only the existing Operations engine, using its existing command/window.
Keep the DD supervisor, four DD workers, roster watcher and breach relay running.
Verify legacy accounts and Operations history/target monitoring remain healthy.
Only after this installation and broker behaviour are verified, configure on the
main API: NP_PLAN_WITHDRAWAL_VERIFIED=NP_WITHDRAWAL_RULES_V1_20261108
This technical gate allows new custom/no-cap offerings; owner purchase switches
still control sales. Never deliberately breach a customer for testing.
