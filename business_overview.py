"""Read-only business overview: complete paged reads, explicit units and drilldowns."""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from time import monotonic
import gzip
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from flask import jsonify, request

RELEASE = 'business-overview-truth-2026-10-22'
TZ = ZoneInfo('Africa/Lagos')
OPEN = {'assigned_active', 'active', 'current_active', 'phase1_active', 'phase2_active', 'funded_active',
        'live', 'funded', 'approved_active', 'profit_protected', 'payout_pending', 'approved_payout_pending',
        'payment_processing', 'funded_profit_cap_reached'}
PROTECTED = {'profit_protected', 'payout_pending', 'approved_payout_pending', 'payment_processing', 'funded_profit_cap_reached'}
PENDING = {'pending', 'requested', 'submitted', 'pending_review', 'awaiting_review', 'under_review', 'review', 'awaiting_approval'}
APPROVED = {'approved', 'paid', 'completed', 'approved_active'}
TERMINAL = {'rejected', 'declined', 'cancelled', 'canceled', 'failed', 'expired'}


def stamp(value):
    try:
        d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def norm(value):
    return str(value or '').strip().lower()


def flag(value):
    return value is True or norm(value) in {'true', '1', 'yes'}


def amount(row, fields):
    # Explicit zero is preserved, not replaced with a larger original price.
    for field in fields:
        if row.get(field) is None or row[field] == '':
            continue
        try:
            value = Decimal(str(row[field]))
            return value if value.is_finite() and value >= 0 else None
        except (InvalidOperation, TypeError):
            return None
    return None


def build_overview(source, period='today'):
    if period not in {'today', '7d', '30d', 'all'}:
        raise ValueError('Choose Today, 7 days, 30 days or All records')
    required = {'customers', 'accounts', 'purchases', 'payouts', 'stock', 'tickets'}
    if not required <= set(source) or any(not isinstance(source[k], list) for k in required):
        raise ValueError('The complete business snapshot is unavailable')
    now = stamp(source.get('generated_at'))
    if now is None:
        raise ValueError('The snapshot has no valid database timestamp')
    day = now.astimezone(TZ).date()
    start = None if period == 'all' else day - timedelta(days={'today': 0, '7d': 6, '30d': 29}[period])
    def in_period(value):
        d = stamp(value)
        return bool(d and (start is None or start <= d.astimezone(TZ).date() <= day))
    people = {str(t['id']): t for t in source['customers'] if t.get('id')}
    records, groups, meta = {}, defaultdict(list), {}
    def add(key, row, table, label='', when=None, value=None, note=''):
        rid = table + ':' + str(row.get('id') or '')
        if not row.get('id'):
            raise ValueError('A source record is missing its identity')
        person_id = row.get('id') if table == 'customer' else row.get('trader_id')
        person = people.get(str(person_id or ''), {})
        if rid not in records:
            records[rid] = dict(id=str(row['id']), kind=table, trader_id=str(row.get('trader_id') or (row['id'] if table == 'customer' else '') or ''),
                customer=person.get('name') or person.get('email') or 'Unlinked customer', email=person.get('email') or '',
                reference=row.get('mt5_login') or row.get('plan_name') or row.get('subject') or str(row['id']),
                status=row.get('account_status') or row.get('payment_status') or row.get('status') or 'Registered',
                occurred_at=when or row.get('created_at'), observed_at=row.get('observed_at'), amount=float(value) if value is not None else None)
        if value is not None:
            records[rid]['amount'] = float(value)
        if when is not None:
            records[rid]['occurred_at'] = when
        if rid not in groups[key]:
            groups[key].append(rid)
        # Context belongs to the metric, so the same row can explain different facts.
        meta.setdefault(key, {})[rid] = note or label
    def issue(row, table, reason):
        rid = table + ':' + str(row['id'])
        prior = meta.get('record_checks', {}).get(rid, '')
        add('record_checks', row, table, note=reason)
        # Multiple checks remain one review record, never several urgent tasks.
        if reason not in prior:
            meta['record_checks'][rid] = prior + '; ' + reason
    def customer_ok(row):
        return str(row.get('trader_id') or '') in people
    for t in people.values():
        add('customers', t, 'customer', note='Registered profile; this does not imply a paid or active account')
    # Account history is kept separate. Only unarchived, nonterminal open statuses qualify.
    candidates = defaultdict(list)
    assigned_orders = set()
    for a in source['accounts']:
        if a.get('purchase_id') and customer_ok(a) and a.get('mt5_login'):
            assigned_orders.add((str(a['purchase_id']), str(a['trader_id'])))
        state = norm(a.get('account_status'))
        closed = bool(a.get('archived_at') or a.get('breached_at') or a.get('passed_at') or norm(a.get('phase_pass_status')) == 'passed')
        if state in OPEN and not closed and a.get('mt5_login'):
            if not customer_ok(a):
                issue(a, 'account', 'Open account has no matching customer profile')
                continue
            if not a.get('mt5_server'):
                issue(a, 'account', 'Broker server is missing; unique MT5 ownership cannot be confirmed')
                continue
            candidates[(str(a['mt5_login']).strip(), norm(a['mt5_server']))].append(a)
        else:
            add('account_history', a, 'account', note='Historical or non-open account row; not a current trading account')
    open_accounts = []
    for broker, rows in candidates.items():
        owners = {(str(a.get('trader_id')), str(a.get('purchase_id') or '')) for a in rows}
        if len(owners) > 1:
            for a in rows:
                issue(a, 'account', 'MT5 has conflicting open ownership or purchase links; excluded from confirmed open accounts')
            continue
        rows.sort(key=lambda a: stamp(a.get('updated_at') or a.get('created_at')) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        chosen = rows[0]
        if len(rows) > 1:
            for a in rows:
                issue(a, 'account', 'Repeated open rows for the same MT5; counted once using the latest record')
        open_accounts.append(chosen)
        add('open_accounts', chosen, 'account', note='One unique MT5 and server with an open assignment and no recorded closure/pass/breach')
        stage = norm(chosen.get('stage'))
        bucket = 'protected_accounts' if norm(chosen.get('account_status')) in PROTECTED else {'phase1': 'phase1_accounts', 'phase2': 'phase2_accounts', 'funded': 'funded_accounts'}.get(stage, 'other_open_accounts')
        add(bucket, chosen, 'account', note='Protected or awaiting payout; not assumed to be trading' if bucket == 'protected_accounts' else 'Open account assignment in this stage')
        age = (now - stamp(chosen.get('observed_at'))).total_seconds() if stamp(chosen.get('observed_at')) else None
        fresh = age is not None and -60 <= age <= 180
        add('telemetry_fresh' if fresh else 'telemetry_attention', chosen, 'account', when=chosen.get('observed_at'),
            note='Broker observation within 3 minutes' if fresh else 'No matching broker observation within 3 minutes; check the connection, not the customer balance')
    paid_customers = set()
    totals = {'approved_receipts': Decimal(0), 'paid_payouts': Decimal(0), 'approved_payouts': Decimal(0)}
    unknown_money = defaultdict(int)
    for p in source['purchases']:
        if flag(p.get('mark_as_test')):
            add('excluded_transactions', p, 'purchase', note='Test transaction; excluded from production reporting and work queues')
            continue
        if flag(p.get('excluded_from_revenue')):
            add('excluded_transactions', p, 'purchase', note='Excluded from recorded receipts; any real account delivery obligation remains in the work queue')
        state, raw = norm(p.get('payment_status') or p.get('status')), norm(p.get('status'))
        reset = flag(p.get('reset_order'))
        if state in APPROVED | PENDING and (p.get('rejected_at') or raw in TERMINAL):
            issue(p, 'purchase', 'Approval conflicts with rejected or terminal evidence; not included in receipt totals')
            continue
        if not customer_ok(p):
            issue(p, 'purchase', 'Payment record has no matching customer profile')
            continue
        value = amount(p, ['final_fee', 'fee', 'amount_due'])
        if state in PENDING:
            if p.get('has_proof'):
                add('reset_reviews' if reset else 'purchase_reviews', p, 'purchase', value=value,
                    note='Submitted payment proof awaiting a decision; not recognized as collected money')
            else:
                issue(p, 'purchase', 'Pending purchase has no payment proof; not counted as a submitted payment to review')
        if state in APPROVED:
            paid_customers.add(str(p['trader_id']))
            # A reset order is not a new challenge and must not inflate first-account work.
            if not reset and raw not in {'archived', 'completed', 'reset_consumed'} and (str(p['id']), str(p['trader_id'])) not in assigned_orders:
                if p.get('mt5_login') or p.get('trader_account_id') or p.get('assigned_mt5_id') or p.get('assigned_at'):
                    issue(p, 'purchase', 'Purchase has legacy assignment evidence but no exact linked account; reconcile the link, do not assign again blindly')
                else:
                    add('first_account_waiting', p, 'purchase', value=value, note='Approved purchase with no linked account or recorded assignment evidence; verify before assigning')
            date = p.get('approved_at') or p.get('created_at')
            if in_period(date) and not flag(p.get('excluded_from_revenue')):
                add('approved_receipts', p, 'purchase', value=value, when=date,
                    note=('Approved reset payment' if reset else 'Approved plan payment') + '; recorded approval, not independently reconciled bank cash')
                if value is None:
                    unknown_money['approved_receipts'] += 1
                    issue(p, 'purchase', 'Approved payment has no valid amount; excluded from the displayed monetary sum')
                else:
                    totals['approved_receipts'] += value
    for tid in paid_customers:
        add('paying_customers', people[tid], 'customer', note='At least one non-test, non-excluded approved payment record')
    for p in source['payouts']:
        if flag(p.get('mark_as_test')):
            add('excluded_transactions', p, 'payout', note='Test transaction; excluded from production reporting and work queues')
            continue
        if flag(p.get('excluded_from_revenue')):
            add('excluded_transactions', p, 'payout', note='Excluded from recorded financial reports; any unpaid production obligation remains in the queue')
        state = norm(p.get('status'))
        paid = bool(p.get('paid_at'))
        rejected = bool(p.get('rejected_at')) or state in TERMINAL
        if (paid and rejected) or (state == 'paid' and rejected):
            issue(p, 'payout', 'Paid and rejected/cancelled evidence conflict; excluded from payment and queue totals')
            continue
        value = amount(p, ['payout_amount', 'amount'])
        if paid:
            if in_period(p['paid_at']) and not flag(p.get('excluded_from_revenue')):
                add('paid_payouts', p, 'payout', value=value, when=p['paid_at'], note='Payout has a recorded paid timestamp; bank reconciliation is separate')
                if value is None:
                    unknown_money['paid_payouts'] += 1
                    issue(p, 'payout', 'Paid payout has no valid amount; excluded from the monetary sum')
                else:
                    totals['paid_payouts'] += value
            continue
        if rejected:
            continue
        if state == 'paid':
            issue(p, 'payout', 'Marked paid but missing the payment timestamp; not counted as a confirmed paid record')
            continue
        if not customer_ok(p):
            issue(p, 'payout', 'Outstanding payout has no matching customer profile')
            continue
        aid = str(p.get('trader_account_id') or '')
        source_account = next((a for a in source['accounts'] if str(a.get('id')) == aid), None)
        if not source_account or str(source_account.get('trader_id')) != str(p['trader_id']) or str(source_account.get('mt5_login')) != str(p.get('mt5_login')):
            issue(p, 'payout', 'Outstanding payout is not linked to the exact customer and source MT5; verify eligibility before payment')
        if state in PENDING:
            add('payout_reviews', p, 'payout', value=value, note='Submitted payout request awaiting review; not an approved debt')
        elif state == 'approved':
            add('approved_payouts', p, 'payout', value=value, note='Approved record has no paid timestamp; verify source and eligibility before payment')
            if value is None:
                unknown_money['approved_payouts'] += 1
                issue(p, 'payout', 'Approved payout has no valid amount; excluded from its monetary sum')
            else:
                totals['approved_payouts'] += value
    occupied = set(candidates)
    stock_seen = set()
    for m in source['stock']:
        broker = (str(m.get('mt5_login') or '').strip(), norm(m.get('mt5_server')))
        if norm(m.get('status')) in {'available', 'unused', 'free', 'new', 'ready', 'open'} and not m.get('assigned_trader_id') and not m.get('trader_account_id') and not m.get('archived_at'):
            if not all(broker) or broker in occupied:
                issue(m, 'stock', 'Available pool row has incomplete broker identity or is already open on a customer')
            elif broker not in stock_seen:
                stock_seen.add(broker)
                add('available_stock', m, 'stock', note=f"Unassigned {m.get('pool_class') or 'unclassified'} stock · account size {m.get('account_size')}; not necessarily suitable for every waiting purchase")
    for t in source['tickets']:
        if norm(t.get('status')) in {'open', 'pending', 'in_progress', 'in progress'} and not t.get('closed_at') and not flag(t.get('has_reply')):
            add('open_tickets', t, 'ticket', note='Open support conversation')
    queue_keys = ['purchase_reviews', 'reset_reviews', 'first_account_waiting', 'payout_reviews', 'approved_payouts', 'open_tickets']
    queue_ids = set(rid for key in queue_keys for rid in groups[key])
    groups['work_waiting'] = sorted(queue_ids)
    return dict(release=RELEASE, generated_at=source['generated_at'], period=period, period_start=str(start) if start else None,
        business_timezone='Africa/Lagos', telemetry_fresh_seconds=180, refresh_seconds=30,
        counts={key: len(ids) for key, ids in groups.items()}, amounts={key: float(v) for key, v in totals.items()},
        unknown_amounts=dict(unknown_money), groups=dict(groups), records=records, reasons=meta,
        read_started_at=source.get('read_started_at', source['generated_at']),
        oldest_waiting={k: min((records[r]['occurred_at'] for r in groups[k] if stamp(records[r]['occurred_at'])), key=stamp, default=None) for k in queue_keys},
        queue_keys=queue_keys, source_counts={k: len(source[k]) for k in required})


FIELDS = {
 'customers': ('traders', 'id,name,email,created_at,deleted_at'),
 'accounts': ('trader_accounts', 'id,trader_id,purchase_id,stage,account_status,mt5_login,mt5_server,archived_at,passed_at,breached_at,phase_pass_status,created_at,updated_at,monitoring_enabled'),
 'purchases': ('challenge_purchases', 'id,trader_id,plan_name,fee,final_fee,amount_due,payment_status,status,created_at,approved_at,rejected_at,payment_proof_url,admin_note,mark_as_test,excluded_from_revenue,mt5_login,trader_account_id,assigned_mt5_id,assigned_at'),
 'payouts': ('payouts', 'id,trader_id,trader_account_id,mt5_login,payout_amount,amount,status,created_at,requested_at,approved_at,paid_at,rejected_at,mark_as_test,excluded_from_revenue'),
 'stock': ('mt5_pool', 'id,mt5_login,mt5_server,account_size,pool_class,status,assigned_trader_id,trader_account_id,archived_at'),
 'tickets': ('support_tickets', 'id,trader_id,subject,status,priority,created_at,closed_at,replied_at,admin_reply'),
 'observations': ('np_live_account_state', 'trader_account_id,trader_id,mt5_login,observed_at'),
}


def read_sources(transport, url, key):
    started = datetime.now(timezone.utc).isoformat()
    deadline = monotonic() + 14
    headers = {'apikey': key, 'Authorization': 'Bearer ' + key}
    def read(item):
        name, (table, fields) = item
        pk = 'trader_account_id' if name == 'observations' else 'id'
        rows, last = [], None
        for _ in range(50):
            if monotonic() > deadline:
                raise TimeoutError('Complete read exceeded its deadline')
            params = {'select': fields, 'order': pk + '.asc', 'limit': '1000'}
            if last is not None:
                params[pk] = 'gt.' + last
            r = transport.get(url.rstrip('/') + '/rest/v1/' + table, headers=headers, params=params, timeout=(2, 4))
            r.raise_for_status()
            batch = r.json()
            if not isinstance(batch, list):
                raise ValueError('Invalid source data')
            if not batch:
                return name, rows
            current = str(batch[-1].get(pk) or '')
            if not current or (last is not None and current <= last):
                raise ValueError('Source paging did not advance')
            rows.extend(batch)
            last = current
        raise ValueError('Complete read exceeds its supported size')
    with ThreadPoolExecutor(max_workers=4) as pool:
        source = dict(pool.map(read, FIELDS.items()))
    observations = {str(r['trader_account_id']): r for r in source.pop('observations')}
    for a in source['accounts']:
        o = observations.get(str(a['id']))
        if o and str(o.get('trader_id')) in {str(a.get('trader_id')), str(a['id'])} and str(o.get('mt5_login')) == str(a.get('mt5_login')):
            a['observed_at'] = o.get('observed_at')
    import re
    for p in source['purchases']:
        p['has_proof'] = bool(str(p.pop('payment_proof_url', '') or '').strip())
        note = norm(p.pop('admin_note', ''))
        p['reset_order'] = 'np_reset_request:' in note or 'reset' in norm(p.get('status')) or bool(re.search(r'(^|[^a-z])reset([^a-z]|$)', norm(p.get('plan_name'))))
    for t in source['tickets']:
        t['has_reply'] = bool(t.get('replied_at') or str(t.pop('admin_reply', '') or '').strip())
    source.update(read_started_at=started, generated_at=datetime.now(timezone.utc).isoformat())
    return source


def register_business_overview(app, transport, url, key, require_admin):
    cached = [None, 0.0]
    lock = Lock()
    @app.get('/business_overview_version')
    def business_overview_version():
        return jsonify(success=True, release=RELEASE, refresh_seconds=30, read_only=True)

    @app.get('/admin/business_overview')
    def business_overview():
        _, error = require_admin()
        if error is not None:
            return error
        period = request.args.get('period', 'today')
        if period not in {'today', '7d', '30d', 'all'}:
            return jsonify(success=False, error='Invalid reporting period'), 400
        try:
            if not lock.acquire(timeout=0.2):
                raise TimeoutError('A full overview refresh is already running')
            try:
                if cached[0] is None or monotonic() - cached[1] > 5 or request.args.get('fresh') == '1':
                    source = read_sources(transport, url, key)
                    data = build_overview(source, period)
                    cached[:] = [source, monotonic()]
                else:
                    data = build_overview(cached[0], period)
            finally:
                lock.release()
            result = jsonify(success=True, data=data)
            result.headers['Cache-Control'] = 'no-store'
            if 'gzip' in request.headers.get('Accept-Encoding', '').lower() and len(result.get_data()) > 32000:
                result.set_data(gzip.compress(result.get_data(), compresslevel=4))
                result.headers['Content-Encoding'] = 'gzip'
                result.headers['Vary'] = 'Accept-Encoding'
            return result
        except Exception:
            return jsonify(success=False, error='The complete business snapshot could not be verified. No estimated or partial totals are shown.'), 503
