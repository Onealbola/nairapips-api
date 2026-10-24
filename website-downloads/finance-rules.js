/* FINANCE_RULES_START: common dates, amounts and exclusions for financial reports. */
function npFinanceFlag(v){return v===true||String(v).toLowerCase()==='true';}
function npFinanceExcluded(r){return npFinanceFlag(r.mark_as_test)||npFinanceFlag(r.excluded_from_revenue);}
function npFinanceStatus(r){return String(r.status||'').trim().toLowerCase();}
function npFinanceApproved(r){
 const s=npFinanceStatus(r),p=String(r.payment_status||'').trim().toLowerCase();
 return !npFinanceExcluded(r)&&!['rejected','declined','cancelled','canceled','refunded'].includes(s)&&!['rejected','refunded'].includes(p)&&(['approved','paid'].includes(p)||['approved','paid','completed','approved_active','assigned','approved_waiting_mt5'].includes(s));
}
function npFinancePaid(r){return !npFinanceExcluded(r)&&(npFinanceStatus(r)==='paid'||!!r.paid_at);}
function npFinanceNumber(r,keys){for(const k of keys){if(r[k]!==null&&r[k]!==undefined&&r[k]!==''){const n=Number(r[k]);if(!Number.isFinite(n)||n<0)throw Error('Invalid financial amount');return n;}}return 0;}
function npFinanceSum(rows,fn){return rows.reduce((sum,r)=>sum+Math.round(fn(r)*100),0)/100;}
function npFinanceSaleAmount(r){return npFinanceNumber(r,['final_fee','amount_paid','payment_amount','amount','challenge_fee','plan_fee','fee','amount_due','price']);}
function npFinancePayoutAmount(r){return npFinanceNumber(r,['amount','payout_amount','profit_share_amount','available_payout']);}
function npFinanceDate(v){if(!v)return null;const d=new Date(/^\d{4}-\d{2}-\d{2}$/.test(String(v))?v+'T00:00:00+01:00':v);return Number.isNaN(d.getTime())?null:d;}
function npFinanceSaleDate(r){for(const k of ['created_at','payment_submitted_at','payment_date','date','paid_at','payment_confirmed_at','approved_at']){const d=npFinanceDate(r[k]);if(d)return d;}return null;}
function npFinancePayoutDate(r){if(npFinancePaid(r))return npFinanceDate(r.paid_at);return npFinanceDate(npFinanceStatus(r)==='approved'?(r.approved_at||r.requested_at||r.created_at):(r.requested_at||r.created_at));}
function npFinanceDay(v){const d=npFinanceDate(v);return d?new Date(d.getTime()+3600000).toISOString().slice(0,10):'';}
function npFinanceBounds(range,from='',to='',now=new Date()){
 const n=now.getTime(),wat=new Date(n+3600000),day=Date.UTC(wat.getUTCFullYear(),wat.getUTCMonth(),wat.getUTCDate())-3600000;
 let lo=-Infinity,hi=n;
 if(range==='today')lo=day;
 else if(range==='yesterday'){lo=day-86400000;hi=day-1;}
 else if(range==='week')lo=day-((wat.getUTCDay()+6)%7)*86400000;
 else if(range==='month')lo=Date.UTC(wat.getUTCFullYear(),wat.getUTCMonth(),1)-3600000;
 else if(range==='year')lo=Date.UTC(wat.getUTCFullYear(),0,1)-3600000;
 else if(range==='7d'||range==='30d')lo=day-((range==='7d'?7:30)-1)*86400000;
 else if(range==='24h')lo=n-86400000;
 else if(range==='custom'){
  if(from)lo=new Date(from.includes('T')?from+'+01:00':from+'T00:00:00+01:00').getTime();
  if(to)hi=new Date(to.includes('T')?to+'+01:00':to+'T23:59:59.999+01:00').getTime()+(to.includes('T')?59999:0);
 }
 return {fromMs:lo,toMs:Math.min(hi,n)};
}
function npFinanceInRange(v,range,from='',to=''){const d=npFinanceDate(v);if(!d)return false;const b=npFinanceBounds(range,from,to);return d.getTime()>=b.fromMs&&d.getTime()<=b.toMs;}
function npFinanceRangeLabel(range,from='',to=''){
 if(range==='custom'&&!from&&!to)range='all';
 const names={all:'ALL TIME',today:'TODAY',yesterday:'YESTERDAY',week:'THIS WEEK',month:'THIS MONTH',year:'THIS YEAR','7d':'LAST 7 DAYS','30d':'LAST 30 DAYS','24h':'LAST 24 HOURS',custom:'CUSTOM'};
 const b=npFinanceBounds(range,from,to),fmt=n=>new Date(n).toLocaleDateString('en-GB',{timeZone:'Africa/Lagos',day:'numeric',month:'short',year:'numeric',...((range==='24h'||range==='custom'&&(from.includes('T')||to.includes('T')))?{hour:'2-digit',minute:'2-digit'}:{})});
 return (names[range]||range)+(Number.isFinite(b.fromMs)?' · '+fmt(b.fromMs)+' → '+fmt(b.toMs):'')+' · WAT';
}
function npFinanceSourceState(){const key=String(SESSION.token||'');if(!window.NP_FINANCE_SOURCE||NP_FINANCE_SOURCE.key!==key)window.NP_FINANCE_SOURCE={key,data:null,at:0,pending:null};return NP_FINANCE_SOURCE;}
function npFinancePurchases(){return npFinanceSourceState().data?.purchases||SESSION.purchases||[];}
function npFinancePayouts(){return npFinanceSourceState().data?.payouts||SESSION.payouts||[];}
async function npFinanceLoad(){
 const s=npFinanceSourceState();if(s.pending)return s.pending;
 s.pending=(async()=>{const r=await api('/admin/finance_source?nonce='+Date.now()+'-'+Math.random().toString(36).slice(2));if(npFinanceSourceState()!==s)throw Error('Session changed');if(!r.success||!Array.isArray(r.purchases)||!Array.isArray(r.payouts))throw Error(r.error||'Financial records unavailable');s.data=r;s.at=Date.now();})().finally(()=>s.pending=null);return s.pending;
}
/* FINANCE_RULES_END */
