# Compact trader MT5 lists — 21 November 2026

MT5 Accounts and MT5 Vault now use compact tables rather than large card grids.
Desktop columns show the MT5 login/stage, status, capital, account metrics where
applicable, assignment date and actions. Phone layouts prioritize login/status,
equity (Accounts) or capital (Vault), with secondary data in expanded details.
Search, status counts/filters and 12-row pagination keep large histories usable.
Current accounts sort first; permanent history remains searchable and accessible.
Only one details row opens at a time and credentials are generated on expansion,
masked until explicitly revealed. Existing exact-account credential-copy, reset
payment and account-selection handlers remain in use. Breached rows do not expose
copyable passwords or launch actions, and historical records cannot launch MT5.
No financial/account/lifecycle state or broker rules are written by this module.

The existing render wrappers still run for recall/cache cleanup before the new
presentation reads SESSION. The hosted /public/trader-catalogue.js script appends
account-rows.js after the existing reporting and scope helpers, so already
installed cPanel pages receive this update on refresh. No cPanel/VPS installation
is required. The module has a release guard to prevent duplicate event listeners.

Browser validation exercises both full existing trader modules with 37 fixture
accounts, including recall exclusion; filters/search, pagination, missing/zero
metrics, password reveal and exact credential copy, reset action identity and
active selection; widths 320/360/390/768/1440/1920 and real mobile touch scrolling.
Existing panel-delivery tests and combined hosted-script syntax checks pass.
