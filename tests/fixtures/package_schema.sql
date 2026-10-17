create role anon; create role authenticated; create role service_role bypassrls;
create table public.traders(id uuid primary key, name text,email text,phone text,current_account_id uuid);
create table public.business_settings(key text primary key,value text,updated_at timestamptz,updated_by text);
create table public.challenge_purchases(
 id uuid primary key,trader_id uuid references public.traders(id),trader_name text,email text,phone text,
 plan_name text,account_size numeric,fee numeric,original_fee numeric,final_fee numeric,amount_due numeric,
 status text,payment_status text,payment_proof_url text,challenge_journey jsonb,journey_source text,
 product_category text,phase1_target numeric,phase2_target numeric,max_drawdown numeric,drawdown_type text,
 payout_split numeric,second_life_enabled boolean,lives_total integer,created_at timestamptz,
 plan_snapshot_at timestamptz,approved_at timestamptz);
create table public.trader_accounts(id uuid primary key,trader_id uuid,stage text,purchase_id uuid,
 account_status text,passed_at timestamptz,phase_pass_status text,breached_at timestamptz);
create table public.payouts(id uuid primary key,trader_account_id uuid,trader_id uuid,status text,
 paid_at timestamptz,verified_profit numeric,amount numeric);

alter table public.challenge_purchases add column trader_account_id uuid, add column mt5_login text,
 add column assigned_mt5_id uuid, add column mt5_server text, add column assigned_at timestamptz,add column updated_at timestamptz;
alter table public.trader_accounts add column account_size numeric,add column start_balance numeric,
 add column mt5_login text,add column mt5_pool_id uuid,add column mt5_server text,
 add column mt5_master_password text,add column mt5_investor_password text,
 add column monitoring_enabled boolean,add column dd_limit_percent numeric,add column target_percent numeric,
 add column challenge_journey jsonb,add column journey_source text,
 add column created_at timestamptz,add column started_at timestamptz,add column updated_at timestamptz,
 add column archived_at timestamptz,add column archive_reason text,
 add column current_balance numeric,add column current_equity numeric,add column last_sync_at timestamptz;
create table public.mt5_pool(id uuid primary key,account_size numeric,mt5_login text,mt5_server text,
 mt5_master_password text,mt5_investor_password text,pool_class text,status text,trader_account_id uuid,
 assigned_trader_id uuid,assigned_email text,assigned_trader_name text,assigned_at timestamptz,created_at timestamptz,updated_at timestamptz);
create table public.monitoring_registry(trader_account_id uuid primary key,active boolean,monitoring_state text);
create table public.test_monitoring_failure(enabled boolean);
insert into public.test_monitoring_failure values(false);
create function public.np_monitoring_activate(p_trader_account_id uuid,p_predecessor_account_id uuid default null,p_reason text default null)
returns jsonb language plpgsql as $$ begin
 if exists(select 1 from public.test_monitoring_failure where enabled=true) then raise exception 'Fixture registry failure'; end if;
 update public.monitoring_registry set active=false,monitoring_state='RETIRED' where trader_account_id=p_predecessor_account_id;
 insert into public.monitoring_registry values(p_trader_account_id,true,'LIVE');
 return jsonb_build_object('activated',true);
end $$;
