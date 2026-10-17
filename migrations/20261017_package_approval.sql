-- Authenticated admin approval enters the package entitlement ledger, never
-- the legacy plan assignment path. This does not publish the package product.
begin;
create table if not exists public.np_package_approval_events (
 purchase_id uuid primary key references public.challenge_purchases(id),
 contract_id uuid not null references public.np_package_contracts(id),
 actor text not null check(length(actor)>0),
 approved_at timestamptz not null default clock_timestamp()
);
alter table public.np_package_approval_events enable row level security;
revoke all on public.np_package_approval_events from public,anon,authenticated,service_role;
grant select on public.np_package_approval_events to service_role;

create or replace function public.np_package_approve_purchase(p_purchase_id uuid,p_actor text)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare p public.challenge_purchases%rowtype; c public.np_package_contracts%rowtype;
 u public.np_package_usage%rowtype; e jsonb; prior boolean;
begin
 if p_actor is null or length(trim(p_actor))=0 then raise exception 'Authenticated approving admin is required'; end if;
 select * into strict p from public.challenge_purchases where id=p_purchase_id for update;
 select * into strict c from public.np_package_contracts where id=p.package_contract_id;
 if c.purchase_id is distinct from p.id or c.trader_id is distinct from p.trader_id
  or p.fee is distinct from (c.document->>'purchase_price')::numeric
  or p.amount_due is distinct from (c.document->>'purchase_price')::numeric then
  raise exception 'Purchase identity or frozen package price does not match';
 end if;
 if coalesce(p.status,'') in('rejected','cancelled','canceled')
  or coalesce(p.payment_status,'') in('rejected','cancelled','canceled','refunded') then
  raise exception 'Rejected, cancelled or refunded purchases cannot be approved';
 end if;
 select exists(select 1 from public.np_package_approval_events where purchase_id=p.id) into prior;
 if not prior then
  if coalesce(p.payment_proof_url,'')='' then raise exception 'Payment proof is required for package approval'; end if;
  if coalesce(p.status,'') not in('pending','pending_review','approved_waiting_mt5','approved','approved_active','assigned','completed') then
   raise exception 'Purchase cannot be approved from this state';
  end if;
  update public.challenge_purchases set payment_status='approved',
   approved_at=coalesce(approved_at,clock_timestamp()),
   status=case when trader_account_id is null then 'approved_waiting_mt5' else status end,
   updated_at=clock_timestamp() where id=p.id;
  insert into public.np_package_approval_events(purchase_id,contract_id,actor)
   values(p.id,c.id,trim(p_actor));
 end if;
 select * into strict u from public.np_package_usage where contract_id=c.id;
 -- A retry following any later stage must return the current delivery, never
 -- manufacture another first account or overwrite its state.
 if u.current_account_id is not null then
  return jsonb_build_object('purchase_id',p.id,'contract_id',c.id,
   'account_id',u.current_account_id,'approved',true,'replayed',true);
 end if;
 e:=public.np_package_reserve_action(c.id,null,'first_account');
 return jsonb_build_object('purchase_id',p.id,'contract_id',c.id,
  'entitlement_id',e->>'id','approved',true,'replayed',prior);
end $$;
revoke all on function public.np_package_approve_purchase(uuid,text) from public,anon,authenticated;
grant execute on function public.np_package_approve_purchase(uuid,text) to service_role;
commit;
