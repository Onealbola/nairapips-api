# Staff access workspace

The owner selects a staff member, searches the grouped module checklist, checks
or unchecks modules, and saves explicitly. The catalogue includes all current
sidebar modules plus Black Book, Account Links and Email Traders. Staff
Management stays owner only. Role labels never grant owner privileges.

Enabled modules offer View only (reads), Work (add/edit/actions), or Full control
(including delete/remove). Underlying protected business actions retain their
existing requirements. Clearing every module is valid. Deactivating a staff
member blocks their existing session as well as subsequent sign-ins.

The server resolves each signed staff identity against its current protected
staff record on every request, with request-local reuse only. Stale token roles
cannot override the live role, status or permissions. Unknown staff operations
fail closed; known shared read feeds permit the modules that need those records
without sharing write grants. Public catalogue/announcement reads, authenticated
trader-scoped feeds and independent engine authentication remain separate.
Bootstrap aliases are filtered before compression, including counts and `data`.
Owner bootstrap authentication does not wait for a staff database lookup.

Staff CRUD is owner only, reads explicit safe fields and strips passwords from
all responses. Existing password storage is retained; password reset accepts the
existing `new_password` alias. Saves compare the original permission/status
snapshot, then use database compare-and-set on permissions/status/role. Conflicts
leave the draft available and instruct the owner to reload. Audit entries record
permission and status changes, never passwords. Owner accounts cannot be changed
or deleted through this workspace.

The browser refreshes its current permissions every 15 seconds and on focus.
Server denial applies on the next request regardless of browser refresh timing.
No staff records are changed just by deployment.

Build with `python tools/build_staff_access.py`. The dedicated
`/downloads/staff-access-update.zip` contains `admin_clean.html` and hashed
`admin-assets/` files; extract inside `public_html/admin`. No homepage or trader
files are in that archive. The hosted `/control-panel` already serves the rebuilt
standalone admin page. Existing cPanel pages require installing the archive once.

Validation uses isolated Flask fixtures and real Chromium fixtures (including
the exact compact ZIP) without production staff writes or broker operations.

Session recovery compatibility: the local recovery copy can outlive the signed
server token. Missing/expired/inactive sessions return HTTP 401 with the existing
admin client's recognized `Admin authentication is required` wording, so the
client clears its saved session and displays sign-in instead of leaving the
user stuck on an inventory form. A valid staff session with `mt5pool: edit`
can create phase/funded inventory without becoming an admin. View-only or
revoked access remains HTTP 403. Mutations are never automatically replayed.
