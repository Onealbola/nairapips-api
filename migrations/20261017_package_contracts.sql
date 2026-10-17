-- Additive new-product storage. Existing rows are not backfilled or reclassified.
-- Apply transactionally with a database owner; service/anon REST keys cannot DDL.
begin;
create table if not exists public.np_package_quotes (
 id uuid primary key, trader_id uuid not null references public.traders(id),
 catalog_revision integer not null check(catalog_revision>=0),
 document jsonb not null, rules_hash text not null check(rules_hash ~ '^[a-f0-9]{64}$'),
 price numeric(16,2) not null check(price>0),
 created_at timestamptz not null default clock_timestamp(), expires_at timestamptz not null,
 consumed_purchase_id uuid references public.challenge_purchases(id),
 check(expires_at>created_at)
);
create table if not exists public.np_package_contracts (
 id uuid primary key, quote_id uuid not null unique references public.np_package_quotes(id),
 purchase_id uuid not null unique references public.challenge_purchases(id),
 trader_id uuid not null references public.traders(id), checkout_key uuid not null,
 catalog_revision integer not null, rules_version text not null,
 rules_hash text not null check(rules_hash ~ '^[a-f0-9]{64}$'), document jsonb not null,
 created_at timestamptz not null default clock_timestamp(), unique(trader_id,checkout_key),
 check(document->>'currency'='NGN'), check(document->>'contract_id'=id::text),
 check(document->>'trader_id'=trader_id::text)
);
alter table public.challenge_purchases add column if not exists package_contract_id uuid;
alter table public.trader_accounts add column if not exists package_contract_id uuid;
alter table public.trader_accounts add column if not exists package_rules_hash text;
alter table public.payouts add column if not exists package_contract_id uuid;
do $$ begin
 if not exists(select 1 from pg_constraint where conname='np_purchase_package_contract_fk') then
  alter table public.challenge_purchases add constraint np_purchase_package_contract_fk
   foreign key(package_contract_id) references public.np_package_contracts(id) deferrable initially deferred;
 end if;
 if not exists(select 1 from pg_constraint where conname='np_account_package_contract_fk') then
  alter table public.trader_accounts add constraint np_account_package_contract_fk
   foreign key(package_contract_id) references public.np_package_contracts(id);
 end if;
 if not exists(select 1 from pg_constraint where conname='np_payout_package_contract_fk') then
  alter table public.payouts add constraint np_payout_package_contract_fk
   foreign key(package_contract_id) references public.np_package_contracts(id);
 end if;
end $$;
create table if not exists public.np_package_usage (
 contract_id uuid primary key references public.np_package_contracts(id),
 paid_cycles integer not null default 0 check(paid_cycles>=0),
 phase_resets_reserved integer not null default 0 check(phase_resets_reserved>=0),
 funded_resets_reserved integer not null default 0 check(funded_resets_reserved>=0),
 current_account_id uuid references public.trader_accounts(id),
 completed boolean not null default false, updated_at timestamptz not null default clock_timestamp()
);
create table if not exists public.np_package_entitlements (
 id uuid primary key default gen_random_uuid(), contract_id uuid not null references public.np_package_contracts(id),
 source_account_id uuid references public.trader_accounts(id),
 kind text not null check(kind in('first_account','phase_pass','phase_reset','funded_reset','payout_renewal')),
 target_stage text not null check(target_stage in('phase1','phase2','funded')),
 state text not null default 'reserved' check(state in('reserved','assigned')),
 assigned_account_id uuid unique references public.trader_accounts(id),
 created_at timestamptz not null default clock_timestamp(), assigned_at timestamptz,
 unique(contract_id,source_account_id,kind),
 check((kind='first_account')=(source_account_id is null)),
 check((state='assigned')=(assigned_account_id is not null))
);
create unique index if not exists np_package_one_first_entitlement
 on public.np_package_entitlements(contract_id) where kind='first_account';
create unique index if not exists np_package_one_successor_per_source
 on public.np_package_entitlements(source_account_id) where source_account_id is not null;
create table if not exists public.np_package_settlements (
 payout_id uuid primary key references public.payouts(id),
 contract_id uuid not null references public.np_package_contracts(id),
 account_id uuid not null unique references public.trader_accounts(id),
 amount numeric(16,2) not null check(amount>0), paid_at timestamptz not null,
 recorded_at timestamptz not null default clock_timestamp()
);
create or replace function public.np_package_immutable_contract()
returns trigger language plpgsql set search_path=public,pg_temp as $$
begin raise exception 'Purchased package contracts are immutable'; end $$;
drop trigger if exists np_package_contract_immutable on public.np_package_contracts;
create trigger np_package_contract_immutable before update or delete on public.np_package_contracts
 for each row execute function public.np_package_immutable_contract();

create or replace function public.np_package_checkout(
 p_trader_id uuid,p_quote_id uuid,p_checkout_key uuid,p_payment_proof text)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare q public.np_package_quotes%rowtype; c public.np_package_contracts%rowtype;
 t public.traders%rowtype; cfg jsonb; switches jsonb; contract_id uuid; doc jsonb; target2 numeric;
begin
 if p_checkout_key is null or length(trim(coalesce(p_payment_proof,'')))=0 then raise exception 'Checkout key and payment proof are required'; end if;
 select * into q from public.np_package_quotes where id=p_quote_id and trader_id=p_trader_id for update;
 if not found then raise exception 'Quote not found for this trader'; end if;
 select * into c from public.np_package_contracts where trader_id=p_trader_id and checkout_key=p_checkout_key;
 if found then
  if c.quote_id<>p_quote_id then raise exception 'Checkout key belongs to a different quote'; end if;
  return jsonb_build_object('purchase_id',c.purchase_id,'contract_id',c.id,'replayed',true);
 end if;
 if q.consumed_purchase_id is not null then
  raise exception 'Quote already consumed under a different checkout key';
 end if;
 if q.expires_at<=clock_timestamp() then raise exception 'Quote expired; refresh and confirm the current price'; end if;
 select value::jsonb into cfg from public.business_settings where key='future_package_configuration_v1';
 if cfg is null or coalesce((cfg->>'enabled')::boolean,false) is not true then raise exception 'Package checkout is not released'; end if;
 if (cfg->>'revision')::integer<>q.catalog_revision then raise exception 'Catalog changed; obtain and confirm a fresh quote'; end if;
 select value::jsonb into switches from public.business_settings where key='production_controls_v1';
 if switches is not null and (switches->'features'->>'challenge_sales')::boolean is distinct from true then raise exception 'New challenge purchases are paused'; end if;
 doc:=q.document; contract_id:=(doc->>'contract_id')::uuid;
 if doc->>'trader_id' is distinct from p_trader_id::text or doc->>'rules_version' is distinct from 'NP_PACKAGE_RULES_1' or doc->>'currency' is distinct from 'NGN'
  or (doc->>'catalog_revision')::integer<>q.catalog_revision or (doc->>'purchase_price')::numeric<>q.price then
  raise exception 'Quote contract identity or price mismatch';
 end if;
 select * into strict t from public.traders where id=p_trader_id;
 target2:=case when doc->'stages' ? 'phase2' then (doc->'stage_rules'->'phase2'->>'target_percent')::numeric else 0 end;
 insert into public.challenge_purchases(
  id,trader_id,trader_name,email,phone,plan_name,account_size,fee,original_fee,final_fee,amount_due,
  status,payment_status,payment_proof_url,challenge_journey,journey_source,
  product_category,phase1_target,phase2_target,max_drawdown,drawdown_type,payout_split,
  second_life_enabled,lives_total,package_contract_id,created_at,plan_snapshot_at)
 values(q.id,t.id,t.name,t.email,t.phone,'Build Your Package',(doc->>'capital')::numeric,q.price,q.price,q.price,q.price,
  'pending_review','pending',p_payment_proof,doc->'stages','immutable_package_contract',
  'premium',(doc->'stage_rules'->'phase1'->>'target_percent')::numeric,target2,
  (doc->'stage_rules'->'phase1'->>'drawdown_percent')::numeric,
  upper(doc->'stage_rules'->'phase1'->>'drawdown_type'),(doc->>'profit_share_percent')::numeric,
  false,1,contract_id,clock_timestamp(),clock_timestamp());
 insert into public.np_package_contracts(id,quote_id,purchase_id,trader_id,checkout_key,catalog_revision,rules_version,rules_hash,document)
 values(contract_id,q.id,q.id,p_trader_id,p_checkout_key,q.catalog_revision,doc->>'rules_version',q.rules_hash,doc);
 insert into public.np_package_usage(contract_id) values(contract_id);
 update public.np_package_quotes set consumed_purchase_id=q.id where id=q.id;
 return jsonb_build_object('purchase_id',q.id,'contract_id',contract_id,'replayed',false);
end $$;

create or replace function public.np_package_reserve_action(p_contract_id uuid,p_source_account_id uuid,p_kind text)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.np_package_contracts%rowtype; u public.np_package_usage%rowtype;
 a public.trader_accounts%rowtype; p public.challenge_purchases%rowtype;
 e public.np_package_entitlements%rowtype; next_stage text; allowance integer;
begin
 select * into strict c from public.np_package_contracts where id=p_contract_id;
 select * into strict u from public.np_package_usage where contract_id=c.id for update;
 select * into e from public.np_package_entitlements
  where contract_id=c.id and kind=p_kind and source_account_id is not distinct from p_source_account_id;
 if found then return to_jsonb(e)||jsonb_build_object('replayed',true); end if;
 if u.completed then raise exception 'Purchased package journey is complete'; end if;
 select * into strict p from public.challenge_purchases where id=c.purchase_id;
 if p.trader_id is distinct from c.trader_id or p.package_contract_id is distinct from c.id or coalesce(p.payment_status,'') not in('approved','paid')
  or p.approved_at is null or coalesce(p.status,'') not in('approved_active','approved','assigned','completed','approved_waiting_mt5') then
  raise exception 'Approved package purchase is required';
 end if;
 if p_kind='first_account' then
  if p_source_account_id is not null or u.current_account_id is not null then raise exception 'Initial account already assigned'; end if;
  if exists(select 1 from public.trader_accounts where package_contract_id=c.id) then raise exception 'Initial account already exists; reconcile its delivery'; end if;
  next_stage:='phase1';
 else
  if p_source_account_id is null or p_source_account_id is distinct from u.current_account_id then raise exception 'Action must reference the current exact account'; end if;
  select * into strict a from public.trader_accounts where id=p_source_account_id for update;
  if a.trader_id is distinct from c.trader_id or a.package_contract_id is distinct from c.id or a.package_rules_hash is distinct from c.rules_hash then raise exception 'Source account contract mismatch'; end if;
  if p_kind='phase_pass' then
   if a.breached_at is not null or a.passed_at is null or coalesce(a.phase_pass_status,'') not in('passed','phase1_passed','phase2_passed') then raise exception 'Recorded exact-account pass is required'; end if;
   if a.stage='phase1' then next_stage:=case when c.document->'stages' ? 'phase2' then 'phase2' else 'funded' end;
   elsif a.stage='phase2' and c.document->'stages' ? 'phase2' then next_stage:='funded';
   else raise exception 'This stage cannot pass to another evaluation stage'; end if;
  elsif p_kind in('phase_reset','funded_reset') then
   if a.breached_at is null or coalesce(a.account_status,'') not in('breached','breached_archived','closed','archived') then raise exception 'An exact terminal breached source is required'; end if;
   if p_kind='funded_reset' then
    if a.stage is distinct from 'funded' then raise exception 'Funded reset requires a funded account'; end if;
    allowance:=(c.document->>'funded_resets')::integer;
    if u.funded_resets_reserved>=allowance then raise exception 'No included funded resets remain'; end if;
    update public.np_package_usage set funded_resets_reserved=funded_resets_reserved+1 where contract_id=c.id;
   else
    if coalesce(a.stage,'') not in('phase1','phase2') then raise exception 'Phase reset requires an evaluation account'; end if;
    allowance:=(c.document->>'phase_resets')::integer;
    if u.phase_resets_reserved>=allowance then raise exception 'No included phase resets remain'; end if;
    update public.np_package_usage set phase_resets_reserved=phase_resets_reserved+1 where contract_id=c.id;
   end if;
   next_stage:=a.stage;
  else raise exception 'Unsupported package action'; end if;
 end if;
 -- One successor entitlement per account across every action kind.
 if p_source_account_id is not null and exists(select 1 from public.np_package_entitlements where source_account_id=p_source_account_id) then
  raise exception 'This source already has a successor entitlement';
 end if;
 insert into public.np_package_entitlements(contract_id,source_account_id,kind,target_stage)
 values(c.id,p_source_account_id,p_kind,next_stage) returning * into e;
 return to_jsonb(e)||jsonb_build_object('replayed',false);
end $$;

create or replace function public.np_package_record_paid(p_payout_id uuid)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare p public.payouts%rowtype; a public.trader_accounts%rowtype; c public.np_package_contracts%rowtype;
 u public.np_package_usage%rowtype; prior public.np_package_settlements%rowtype;
 expected numeric; gross numeric; cap numeric; cycle_limit integer; result jsonb; e uuid;
begin
 select * into strict p from public.payouts where id=p_payout_id for update;
 select * into strict a from public.trader_accounts where id=p.trader_account_id;
 select * into strict c from public.np_package_contracts where id=a.package_contract_id;
 select * into strict u from public.np_package_usage where contract_id=c.id for update;
 if p.trader_id is distinct from c.trader_id or a.trader_id is distinct from c.trader_id or p.package_contract_id is distinct from c.id
  or a.package_rules_hash is distinct from c.rules_hash or a.stage is distinct from 'funded' then raise exception 'Exact funded payout identity mismatch'; end if;
 if p.status is distinct from 'paid' or p.paid_at is null or p.verified_profit is null or p.verified_profit<=0 then raise exception 'A recorded paid payout and verified closed profit are required'; end if;
 select * into prior from public.np_package_settlements where payout_id=p.id;
 if found then
  if prior.amount<>p.amount or prior.account_id<>a.id then raise exception 'Paid settlement changed after recording'; end if;
  return to_jsonb(prior)||jsonb_build_object('replayed',true);
 end if;
 if u.completed or u.current_account_id is distinct from a.id then raise exception 'Payout is not for the current package funded account'; end if;
 gross:=p.verified_profit;
 if c.document->>'gross_profit_cap_percent' is not null then
  cap:=(c.document->>'capital')::numeric*(c.document->>'gross_profit_cap_percent')::numeric/100;
  gross:=least(gross,cap);
 end if;
 expected:=round(gross*(c.document->>'profit_share_percent')::numeric/100,2);
 if p.amount is distinct from expected then raise exception 'Paid amount does not match the purchased share and cap'; end if;
 cycle_limit:=(c.document->>'payout_cycles')::integer;
 if cycle_limit is not null and u.paid_cycles>=cycle_limit then raise exception 'Purchased paid cycles are exhausted'; end if;
 insert into public.np_package_settlements(payout_id,contract_id,account_id,amount,paid_at)
 values(p.id,c.id,a.id,p.amount,p.paid_at);
 update public.np_package_usage set paid_cycles=paid_cycles+1,
  completed=(cycle_limit is not null and paid_cycles+1>=cycle_limit),updated_at=clock_timestamp()
 where contract_id=c.id returning * into u;
 if not u.completed then
  if exists(select 1 from public.np_package_entitlements where source_account_id=a.id) then raise exception 'Paid source already has a successor entitlement'; end if;
  insert into public.np_package_entitlements(contract_id,source_account_id,kind,target_stage)
  values(c.id,a.id,'payout_renewal','funded') returning id into e;
 end if;
 return jsonb_build_object('payout_id',p.id,'contract_id',c.id,'paid_cycles',u.paid_cycles,'completed',u.completed,'renewal_entitlement_id',e,'replayed',false);
end $$;

-- Broker currency/unused-stock proof is separate from legacy pool records.
create table if not exists public.np_package_pool_verifications (
 pool_id uuid primary key references public.mt5_pool(id), mt5_login text not null,
 mt5_server text not null, currency text not null check(currency='NGN'),
 balance numeric(16,2) not null check(balance>0), equity numeric(16,2) not null check(equity>0),
 open_positions integer not null check(open_positions=0), trading_deals integer not null check(trading_deals=0),
 verified_at timestamptz not null, collector_id text not null
);
create table if not exists public.np_package_notifications (
 entitlement_id uuid primary key references public.np_package_entitlements(id),
 account_id uuid not null references public.trader_accounts(id),
 state text not null default 'pending' check(state in('pending','sent')),
 attempts integer not null default 0, created_at timestamptz not null default clock_timestamp(),
 sent_at timestamptz
);
alter table public.np_package_pool_verifications enable row level security;
alter table public.np_package_notifications enable row level security;
revoke all on public.np_package_pool_verifications,public.np_package_notifications from public,anon,authenticated,service_role;
grant select,insert,update on public.np_package_pool_verifications to service_role;
grant select on public.np_package_notifications to service_role;

create or replace function public.np_package_assign_entitlement(p_entitlement_id uuid)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare e public.np_package_entitlements%rowtype; c public.np_package_contracts%rowtype;
 u public.np_package_usage%rowtype; p public.mt5_pool%rowtype; t public.traders%rowtype;
 purchase public.challenge_purchases%rowtype; aid uuid; stage_rule jsonb;
begin
 select * into strict e from public.np_package_entitlements where id=p_entitlement_id for update;
 select * into strict c from public.np_package_contracts where id=e.contract_id;
 select * into strict u from public.np_package_usage where contract_id=c.id for update;
 if e.state='assigned' then
  return jsonb_build_object('account_id',e.assigned_account_id,'entitlement_id',e.id,'replayed',true,'waiting',false);
 end if;
 select * into strict purchase from public.challenge_purchases where id=c.purchase_id for update;
 if purchase.trader_id is distinct from c.trader_id or purchase.package_contract_id is distinct from c.id
  or coalesce(purchase.payment_status,'') not in('approved','paid') or purchase.approved_at is null
  or coalesce(purchase.status,'') not in('approved_active','approved','assigned','completed','approved_waiting_mt5') then
  raise exception 'Approved exact package purchase is required for assignment';
 end if;
 if u.completed or u.current_account_id is distinct from e.source_account_id then raise exception 'Entitlement does not belong to the current package head'; end if;
 if not (c.document->'stages' ? e.target_stage) then raise exception 'Target stage was not purchased'; end if;
 select * into strict t from public.traders where id=c.trader_id;
 select pool.* into p from public.mt5_pool pool
 join public.np_package_pool_verifications proof on proof.pool_id=pool.id
 where lower(pool.status)='available' and pool.trader_account_id is null and pool.assigned_trader_id is null
  and pool.account_size=(c.document->>'capital')::numeric
  and lower(pool.pool_class)=case when e.target_stage='funded' then 'funded' else 'phase' end
  and coalesce(pool.mt5_master_password,'')<>'' and coalesce(pool.mt5_investor_password,'')<>''
  and proof.mt5_login=pool.mt5_login::text and proof.mt5_server=pool.mt5_server
  and proof.currency='NGN' and proof.balance=(c.document->>'capital')::numeric
  and proof.equity=proof.balance and proof.open_positions=0 and proof.trading_deals=0
  and proof.verified_at between clock_timestamp()-interval '5 minutes' and clock_timestamp()+interval '30 seconds'
  and not exists(select 1 from public.trader_accounts old where old.mt5_login::text=pool.mt5_login::text)
 order by pool.created_at,pool.id for update of pool skip locked limit 1;
 if not found then
  return jsonb_build_object('entitlement_id',e.id,'waiting',true,'reason','No fresh NGN-verified unused MT5 matches the purchased stage and capital');
 end if;
 stage_rule:=c.document->'stage_rules'->e.target_stage; aid:=gen_random_uuid();
 insert into public.trader_accounts(id,trader_id,purchase_id,stage,account_status,account_size,start_balance,
  mt5_login,mt5_pool_id,mt5_server,mt5_master_password,mt5_investor_password,
  monitoring_enabled,dd_limit_percent,target_percent,challenge_journey,journey_source,
  package_contract_id,package_rules_hash,created_at,started_at,updated_at)
 values(aid,c.trader_id,c.purchase_id,e.target_stage,'assigned_active',(c.document->>'capital')::numeric,
  (c.document->>'capital')::numeric,p.mt5_login,p.id,p.mt5_server,p.mt5_master_password,p.mt5_investor_password,
  true,(stage_rule->>'drawdown_percent')::numeric,coalesce((stage_rule->>'target_percent')::numeric,0),
  c.document->'stages','immutable_package_contract',c.id,c.rules_hash,clock_timestamp(),clock_timestamp(),clock_timestamp());
 update public.mt5_pool set status='assigned',trader_account_id=aid,assigned_trader_id=c.trader_id,
  assigned_email=t.email,assigned_trader_name=t.name,assigned_at=clock_timestamp(),updated_at=clock_timestamp() where id=p.id;
 update public.challenge_purchases set trader_account_id=aid,mt5_login=p.mt5_login,assigned_mt5_id=p.id,
  mt5_server=p.mt5_server,assigned_at=clock_timestamp(),updated_at=clock_timestamp(),status='approved_active' where id=c.purchase_id;
 -- A separate existing funded journey must keep its own profile/current pointer.
 update public.traders set current_account_id=aid where id=c.trader_id
  and (current_account_id is null or current_account_id=e.source_account_id);
 if e.source_account_id is not null then
  update public.trader_accounts set account_status='archived_'||stage,monitoring_enabled=false,
   archived_at=clock_timestamp(),archive_reason=coalesce(archive_reason,'')||' [NP_PACKAGE_CONSUMED:'||e.id::text||'] successor='||aid::text,
   updated_at=clock_timestamp() where id=e.source_account_id and package_contract_id=c.id and trader_id=c.trader_id;
 end if;
 -- Same database transaction: an activation error rolls back account, pool and pointers.
 perform public.np_monitoring_activate(p_trader_account_id=>aid,p_predecessor_account_id=>e.source_account_id,
  p_reason=>'immutable package entitlement '||e.id::text);
 if not exists(select 1 from public.monitoring_registry where trader_account_id=aid and active=true and monitoring_state='LIVE') then
  raise exception 'Package monitoring activation was not confirmed';
 end if;
 update public.np_package_usage set current_account_id=aid,updated_at=clock_timestamp() where contract_id=c.id;
 update public.np_package_entitlements set state='assigned',assigned_account_id=aid,assigned_at=clock_timestamp() where id=e.id;
 insert into public.np_package_notifications(entitlement_id,account_id) values(e.id,aid);
 return jsonb_build_object('account_id',aid,'entitlement_id',e.id,'replayed',false,'waiting',false);
end $$;
revoke all on function public.np_package_assign_entitlement(uuid) from public,anon,authenticated;
grant execute on function public.np_package_assign_entitlement(uuid) to service_role;

create or replace function public.np_package_schema_version()
returns jsonb language sql stable security definer set search_path=public,pg_temp as $$
 select jsonb_build_object('schema_version','NP_PACKAGE_SCHEMA_1','ready',
  to_regclass('public.np_package_quotes') is not null and
  to_regclass('public.np_package_contracts') is not null and
  to_regclass('public.np_package_usage') is not null and
  to_regclass('public.np_package_entitlements') is not null and
  to_regclass('public.np_package_settlements') is not null and
  to_regclass('public.np_package_pool_verifications') is not null and
  to_regclass('public.np_package_notifications') is not null and
  to_regprocedure('public.np_package_checkout(uuid,uuid,uuid,text)') is not null and
  to_regprocedure('public.np_package_reserve_action(uuid,uuid,text)') is not null and
  to_regprocedure('public.np_package_record_paid(uuid)') is not null and
  to_regprocedure('public.np_package_assign_entitlement(uuid)') is not null)
$$;
revoke all on function public.np_package_schema_version() from public,anon,authenticated;
grant execute on function public.np_package_schema_version() to service_role;

-- Direct client writes are never allowed; all financial transitions use RPCs.
alter table public.np_package_quotes enable row level security;
alter table public.np_package_contracts enable row level security;
alter table public.np_package_usage enable row level security;
alter table public.np_package_entitlements enable row level security;
alter table public.np_package_settlements enable row level security;
revoke all on public.np_package_quotes,public.np_package_contracts,public.np_package_usage,public.np_package_entitlements,public.np_package_settlements from public,anon,authenticated,service_role;
grant select on public.np_package_quotes,public.np_package_contracts,public.np_package_usage,public.np_package_entitlements,public.np_package_settlements to service_role;
grant insert on public.np_package_quotes to service_role;
revoke all on function public.np_package_checkout(uuid,uuid,uuid,text),public.np_package_reserve_action(uuid,uuid,text),public.np_package_record_paid(uuid) from public,anon,authenticated;
grant execute on function public.np_package_checkout(uuid,uuid,uuid,text),public.np_package_reserve_action(uuid,uuid,text),public.np_package_record_paid(uuid) to service_role;
comment on table public.np_package_contracts is 'Immutable rules for new package purchases; legacy products and accounts remain unchanged.';
notify pgrst,'reload schema';
commit;
