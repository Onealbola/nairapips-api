# Trader access security

Purpose: staff review of possible links to restricted traders, never automatic bans or financial decisions.

## Setup

Run `migrations/20261027_trader_access_security.sql` in Supabase SQL Editor. It is idempotent and includes both the individual review and automatic review list functions. If the initial table/review setup was already installed, run only `migrations/20261027_trader_access_alerts.sql` to add the review list.

The backend requires its existing privileged Supabase API key. The private event table and RPCs explicitly deny PUBLIC, anon and authenticated database roles. Custom trader tokens cannot call these RPCs directly. Only the authenticated backend security routes expose evidence, restricted to super_admin, admin and admin_manager.

Upload/extract the security website package into `public_html`. Admin's Account Links button opens the review list, trader search and detailed history. Login and registration screens include a security-logging disclosure.

## Recording and provenance

Only successful `/register_trader` and `/login_trader` responses with a server-verified signed trader token are recorded. Client-supplied trader IDs, failed logins and admin preview tokens do not create observations. Existing sessions are not silently backfilled. Existing registration IP fields, where present on restricted profiles, are labelled historical/unverified proxy provenance.

Recorded fields: UUID, authenticated trader ID, registration/login event, observed IP, source method, timestamp and up to 512 printable User-Agent characters. No passwords, auth tokens, cookies, bank data or device fingerprints.

`SECURITY_TRUSTED_PROXY_CIDRS` controls trusted proxy hops. Default is loopback/private VPC ranges used by the Render reverse proxy. Inspect the actual deployment proxy chain if adding another public reverse proxy. Direct untrusted peers cannot supply an authoritative X-Forwarded-For header. Trusted proxy chains are evaluated right to left; invalid/private-only chains are not turned into shared public IP matches.

## Availability and retention

A bounded 128-event queue and at most one small logging worker per web process keep recording off the login critical path. Logging failure does not deny unrelated account access. Staff status reports missing setup, queue issues and unsuccessful writes; do not interpret missing history as proof of unrelated accounts. Restarting a process can lose events still in its in-memory queue. This is a review signal, not a complete authentication audit ledger.

Stored history is limited to the most recent 90 days in reviews. Old rows are deleted on logging activity at most once per six-hour cleanup interval. Review lists show up to 100 matching profiles; individual screens show up to 100 events and report total counts. Restriction status is checked from the current Black Book, so restored traders are not shown as currently banned.

IP and browser matches are not identity proof. Shared mobile networks, households, VPNs and user-controlled browser details can produce false positives. Staff must corroborate evidence with the existing verified account and payment review workflow. Financial and assignment controls are unchanged.
