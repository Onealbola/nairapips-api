/* Production overview. Counts and their records come from one backend snapshot. */
(function(){
'use strict';
const B={data:null,error:'',loading:false,period:'today',sequence:0,metric:null,query:'',page:1,identity:null};
const labels={customers:'Registered profiles',paying_customers:'Customers with approved payments',open_accounts:'Open MT5 assignments',phase1_accounts:'Phase 1 trading',phase2_accounts:'Phase 2 trading',funded_accounts:'Funded trading',protected_accounts:'Protected / awaiting payout',other_open_accounts:'Other open stages',available_stock:'Available MT5 stock',telemetry_fresh:'Fresh broker observations',telemetry_attention:'Broker updates to check',purchase_reviews:'Plan payments to review',reset_reviews:'Reset payments to review',first_account_waiting:'Approved purchases awaiting first MT5',payout_reviews:'Payout requests to review',approved_payouts:'Approved payouts not marked paid',open_tickets:'Open support tickets',work_waiting:'Work waiting now',approved_receipts:'Approved payments recorded',paid_payouts:'Payouts recorded paid',record_checks:'Records to reconcile',account_history:'Historical account rows',excluded_transactions:'Excluded / test transactions'};
const explanations={
 customers:'Profile records, not a count of paying or currently trading customers.',
 paying_customers:'Distinct customer profiles with at least one approved payment that is not flagged as test.',
 open_accounts:'Unique MT5 login and server. Archived, breached and passed records are excluded. Conflicting open ownership is held for reconciliation.',
 phase1_accounts:'Open Phase 1 assignments without recorded pass, breach or archive.',phase2_accounts:'Open Phase 2 assignments without recorded pass, breach or archive.',funded_accounts:'Open Funded assignments that are not protected or waiting for payout.',
 protected_accounts:'Open accounts marked profit protected, payout pending or processing. These are not assumed to be trading.',
 available_stock:'Unique unassigned pool accounts. Match the stage, size and server before assigning; this total does not guarantee suitable stock for every order.',
 telemetry_fresh:'Exact account, customer and MT5 observations no older than 3 minutes at the database snapshot time.',
 telemetry_attention:'Open assignments without a matching broker observation within 3 minutes. A connection check, not proof of a trading loss.',
 purchase_reviews:'Submitted plan payment proof still awaiting a decision. Pending proof is not collected revenue.',
 reset_reviews:'Submitted reset payment proof still awaiting a decision; kept separate from new plan sales.',
 first_account_waiting:'Approved plan purchases with no linked account or recorded assignment evidence. Exact purchase/customer links are used; check the record before assigning.',
 payout_reviews:'Outstanding payout requests awaiting review. Rejected, cancelled and already paid evidence takes precedence. Requests are not yet approved liabilities.',
 approved_payouts:'Approved payout records without a paid timestamp. Check the source account and eligibility before making payment. This queue is not filtered by the money reporting period.',
 open_tickets:'Support tickets still marked open, pending or in progress.',
 work_waiting:'Unique payment, purchase, payout and ticket records in the six queues below. A record is counted once here. Record checks and broker connection checks are separate.',
 approved_receipts:'Sum of saved final fees on approved, non-test, non-excluded plan and reset payments in the selected period. Uses approval date, or submission date when approval date is absent. This is recorded approval, not independently reconciled bank cash.',
 paid_payouts:'Payout amounts with a recorded paid timestamp in the selected period. Paid/rejected conflicts are held for reconciliation. This is not a bank statement.',
 record_checks:'Distinct source records with conflicting status, ownership, missing proof, amount or account links. These are checks to investigate, not automatically debts or customer actions. They can overlap operational queues.',
 account_history:'Historical or non-open database rows. Repeated rows for an old MT5 can exist; these are not additional current accounts or urgent tasks.',
 excluded_transactions:'Test rows are excluded from production. Revenue-excluded rows are left out of recorded financial totals, but any real unpaid or delivery obligation remains visible in the work queues.'
};
const e=x=>esc(String(x??''));
const money=n=>new Intl.NumberFormat('en-NG',{style:'currency',currency:'NGN',maximumFractionDigits:2}).format(Number(n));
const count=key=>B.data?.counts?.[key]??0;
const date=v=>v?new Date(v).toLocaleString('en-GB',{dateStyle:'medium',timeStyle:'medium'}):'No recorded date';
function card(key,sub='',cash=false){
 const value=cash?money(B.data.amounts[key]??0):Number(count(key)).toLocaleString('en-GB');
 const missing=B.data.unknown_amounts?.[key]||0;
 return `<button class="kpi" data-overview-metric="${key}" onclick="npOverviewRecords('${key}')" style="text-align:left;color:inherit;cursor:pointer;min-width:0;background:var(--bg-1,#090909)"><div class="kpi-label">${e(labels[key])}</div><div class="kpi-value" style="font-size:25px;overflow-wrap:anywhere">${e(value)}</div><div class="kpi-sub" style="line-height:1.55">${e(sub)}${B.data.oldest_waiting?.[key]?` · Oldest: ${e(date(B.data.oldest_waiting[key]))}`:''}${cash?` · ${count(key)} records`:''}${missing?` · ${missing} invalid amounts excluded`:''}</div><div class="text-xs" style="margin-top:8px;color:var(--gold)">View records →</div></button>`;
}
function ensureIdentity(){const identity=String(SESSION.token||'')+'|'+String(SESSION.admin?.id||'');if(B.identity!==identity){B.identity=identity;B.data=null;B.error='';B.loading=false;B.metric=null;B.sequence++;}}
function shell(){
 ensureIdentity();
 const stale=!!B.error;
 const d=B.data;
 const status=B.loading?'Refreshing complete snapshot…':d?(stale?'Refresh failed — previous snapshot retained':'Complete database read'):'Waiting for a verified database snapshot';
 const host=document.getElementById('content');if(!host)return;
 host.innerHTML=`<div class="vault mb-6"><div class="flex justify-between items-center gap-3 flex-wrap"><div><div class="badge">BUSINESS OVERVIEW</div><h1 style="font-size:28px;font-weight:900;margin:10px 0">Business operations</h1><p class="text-sm text-gray-400">Current work, open accounts and recorded payments. Every figure opens the records counted.</p></div><button class="btn btn-gold" id="business-overview-refresh" onclick="npRefreshBusinessOverview(true)" ${B.loading?'disabled':''}>↻ Refresh</button></div><p id="business-overview-status" role="status" style="margin-top:12px;color:${stale?'#fbbf24':'var(--t2,#aaa)'}">${e(status)}${d?' · Updated '+e(date(d.generated_at)):''} · Refreshes every 30 seconds while this page is visible</p>${d?`<p class="text-xs text-gray-400" style="margin-top:7px">Records read from ${e(date(d.read_started_at||d.generated_at))} to ${e(date(d.generated_at))}. Activity arriving during that read appears on the next refresh.</p>`:''}${B.error?`<p style="margin-top:8px;color:#fbbf24">${e(B.error)}. ${d?'The values below are from the last successful refresh.':'Totals are unavailable; no zero values are assumed.'}</p>`:''}</div>
 ${d?`<section class="card mb-6"><h2 style="font-size:20px;font-weight:900">Work waiting now</h2><p class="text-sm text-gray-400" style="margin:8px 0 15px">Payment, account delivery and support records still waiting · all dates. Older unresolved records remain visible; each card opens its exact list.</p><div class="grid md:grid-cols-3 gap-3">${card('purchase_reviews','Payment proof awaiting review')}${card('reset_reviews','Kept separate from new plan sales')}${card('first_account_waiting','No recorded first assignment')}${card('payout_reviews','Review before approving')}${card('approved_payouts','Recorded amount: '+money(d.amounts.approved_payouts??0))}${card('open_tickets','Customer conversations awaiting a response')}</div></section>
 <section class="card mb-6"><h2 style="font-size:20px;font-weight:900">Accounts open now</h2><p class="text-sm text-gray-400" style="margin:8px 0 15px">${count('open_accounts')} unique open MT5 assignments. A customer can own more than one account. Protected accounts are listed separately from trading accounts.</p><div class="grid md:grid-cols-4 gap-3">${card('phase1_accounts','Open Phase 1 MT5s')}${card('phase2_accounts','Open Phase 2 MT5s')}${card('funded_accounts','Open Funded trading MT5s')}${card('protected_accounts','Protected or awaiting payout')}</div>${count('other_open_accounts')?`<p><button class="btn btn-sm" onclick="npOverviewRecords('other_open_accounts')">${count('other_open_accounts')} other stages to review</button></p>`:''}<div class="grid md:grid-cols-3 gap-3" style="margin-top:12px">${card('available_stock','Unassigned; stage and size still need matching')}${card('telemetry_fresh','Broker observation within 3 minutes')}${card('telemetry_attention','Connection checks, separate from trading losses')}</div></section>
 <section class="card mb-6"><div class="flex justify-between items-center gap-3 flex-wrap"><h2 style="font-size:20px;font-weight:900">Recorded money</h2><select class="select" id="business-overview-period" ${B.loading?'disabled':''} aria-label="Money reporting period" onchange="npOverviewPeriod(this.value)">${[['today','Today'],['7d','Last 7 days'],['30d','Last 30 days'],['all','All recorded dates']].map(([v,l])=>`<option value="${v}" ${d.period===v?'selected':''}>${l}</option>`).join('')}</select></div><p class="text-sm text-gray-400" style="margin:8px 0 15px">Period applies only to the two totals below. Reporting dates: Africa/Lagos. Tests and rows excluded from revenue are left out.</p><div class="grid md:grid-cols-2 gap-3">${card('approved_receipts','Plan and reset payment approvals',true)}${card('paid_payouts','Recorded paid timestamp required',true)}</div><p class="text-sm text-gray-400" style="margin-top:12px">These are recorded transactions, not a bank balance or net profit. Costs and bank reconciliation are not included.</p></section>
 <section class="card mb-6"><h2 style="font-size:20px;font-weight:900">Customers and record checks</h2><div class="grid md:grid-cols-3 gap-3" style="margin-top:15px">${card('customers','Registered profiles; not all are paying customers')}${card('paying_customers','Distinct customers with approved payments')}${card('record_checks','Verify these records; do not treat them as debts')}</div><details style="margin-top:18px"><summary style="cursor:pointer;color:var(--gold)">History and exclusions</summary><p class="text-sm text-gray-400" style="margin:10px 0">Historical rows do not inflate current account counts or the work queue. Record checks can overlap a work queue, so do not add the two totals together.</p><div class="grid md:grid-cols-2 gap-3">${card('account_history','Database history rows, not current accounts')}${card('excluded_transactions','Test or explicitly excluded financial rows')}</div></details></section>
 <div id="business-overview-records"></div>`:`<div class="card" role="status">${B.loading?'Reading the complete customer, account, payment, payout, stock and support records…':'A verified overview is not available. Use Refresh to retry.'}</div>`}`;
 if(d&&B.metric)paintRecords();
}
window.npRefreshBusinessOverview=async function(manual=false){
 ensureIdentity();
 if(B.loading)return;
 if(!manual&&(SESSION.currentMod!=='overview'||document.hidden))return;
 const seq=++B.sequence;B.loading=true;if(manual||!B.data)shell();
 try{
  const r=await api('/admin/business_overview?period='+encodeURIComponent(B.period)+(manual?'&fresh=1':''));
  const d=r?.data;
  if(!r?.success||!d?.generated_at||!d?.counts||!d?.records||!d?.groups||d.period!==B.period||d.release!=='business-overview-truth-2026-10-22')throw Error(r?.error||'Complete overview was not returned');
  if(seq!==B.sequence)return;
  B.data=d;B.error='';
 }catch(error){if(seq===B.sequence)B.error=error.message||String(error);}
 finally{if(seq===B.sequence){B.loading=false;if(B.error&&B.data)B.period=B.data.period;if(SESSION.currentMod==='overview')shell();}}
};
window.npOverviewPeriod=function(value){if(B.loading)return;B.period=value;B.page=1;npRefreshBusinessOverview(true);};
window.npOverviewRecords=function(key){B.metric=key;B.query='';B.page=1;paintRecords();document.getElementById('business-overview-records')?.scrollIntoView({behavior:'smooth',block:'start'});};
window.npOverviewSearch=function(value){B.query=value;B.page=1;paintTable();};
window.npOverviewPage=function(delta){B.page+=delta;paintTable();};
function paintRecords(){
 const host=document.getElementById('business-overview-records');if(!host||!B.data)return;
 host.innerHTML=`<section class="card"><div class="flex justify-between items-center gap-3 flex-wrap"><h2 style="font-size:20px;font-weight:900">${e(labels[B.metric]||B.metric)}</h2><button class="btn btn-sm" onclick="npOverviewCloseRecords()">Close</button></div><p class="text-sm text-gray-400" style="margin:10px 0">${e(explanations[B.metric]||'')}</p><p class="text-xs text-gray-400">Snapshot: ${e(date(B.data.generated_at))} · Every row below contributed to this figure.</p><input class="input" aria-label="Search overview records" placeholder="Search customer, MT5, record ID or status" style="margin:12px 0" value="${e(B.query)}" oninput="npOverviewSearch(this.value)"><div id="business-overview-record-table"></div></section>`;
 paintTable();
}
window.npOverviewCloseRecords=function(){B.metric=null;const host=document.getElementById('business-overview-records');if(host)host.innerHTML='';};
function paintTable(){
 const host=document.getElementById('business-overview-record-table');if(!host||!B.data)return;
 const ids=B.data.groups[B.metric]||[];
 const q=B.query.trim().toLowerCase();
 const rows=ids.map(id=>({id,...B.data.records[id]})).filter(r=>!q||[r.customer,r.email,r.reference,r.status,r.id].join(' ').toLowerCase().includes(q));
 const pages=Math.max(1,Math.ceil(rows.length/25));B.page=Math.max(1,Math.min(B.page,pages));
 const visible=rows.slice((B.page-1)*25,B.page*25);
 host.innerHTML=`<p class="text-xs text-gray-400" style="margin-bottom:10px">${rows.length} matching records · ${ids.length} counted · page ${B.page} of ${pages}</p><div style="overflow-x:auto"><table class="compact"><thead><tr><th>Customer / record</th><th>Reference</th><th>Recorded state</th><th>Date</th><th>Amount</th><th>What this means</th><th></th></tr></thead><tbody>${visible.map(r=>`<tr><td>${e(r.customer)}<div class="text-xs text-gray-400">${e(r.kind)} · ${e(r.id)}</div></td><td>${e(r.reference)}</td><td>${e(r.status)}</td><td>${e(date(B.metric.startsWith('telemetry_')?r.observed_at:r.occurred_at))}</td><td>${r.amount==null?'—':e(money(r.amount))}</td><td style="min-width:220px">${e(B.data.reasons?.[B.metric]?.[r.kind+':'+r.id]||'Included in this snapshot')}</td><td>${r.trader_id?`<button class="btn btn-sm" data-customer-id="${e(r.trader_id)}" onclick="npOpenTrader360(this.dataset.customerId)">Customer records</button>`:''}</td></tr>`).join('')||'<tr><td colspan="7">No records in this snapshot.</td></tr>'}</tbody></table></div><div class="flex gap-3" style="margin-top:12px"><button class="btn btn-sm" ${B.page===1?'disabled':''} onclick="npOverviewPage(-1)">Previous</button><button class="btn btn-sm" ${B.page===pages?'disabled':''} onclick="npOverviewPage(1)">Next</button></div>`;
}
// This replaces the mixed client-side counters and the old alert/radar overview.
renderOverview=function(){shell();npRefreshBusinessOverview(false);};
window.refreshAdminOverview=()=>npRefreshBusinessOverview(true);
setInterval(()=>npRefreshBusinessOverview(false),30000);
document.addEventListener('visibilitychange',()=>{if(!document.hidden)npRefreshBusinessOverview(false);});
})();
