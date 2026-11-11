/* Exact-account breach reporting; no trading or lifecycle writes. */
(function(){
'use strict';
if(window.NP_ACCOUNT_REPORTING_RELEASE)return;
window.NP_ACCOUNT_REPORTING_RELEASE='account-reporting-20261111';
const breached=a=>!!(a&&(a.recorded_breach||a.breached_at||String(a.account_status||'').toLowerCase().includes('breach')));
if(typeof admAccountLifecycle==='function'){
 const base=admAccountLifecycle;window.admAccountLifecycle=function(a){return breached(a)?'breached':base(a)};
 const authority=admAccountBreachAuthority;window.admAccountBreachAuthority=function(a,t){return breached(a)||authority(a,t)};
 window.admComputedAbsoluteDd=function(a){a=a||{};const start=Number(a.start_balance||a.account_size||0),values=[a.current_balance??a.balance,a.current_equity??a.equity].filter(x=>x!=null&&Number.isFinite(Number(x))).map(Number);return start>0&&values.length?Math.max(0,(start-Math.min(...values))/start*100):Math.max(0,Number(a.absolute_drawdown_percent||a.drawdown_percent||0))};
 window.admDdAbsolutePercent=function(a){return admComputedAbsoluteDd(a)};
 const riskCore=admRiskZone;window.admRiskZone=function(a,t){if(breached(a))return 'breached';if(a?.lifecycle_data_status&&a.lifecycle_data_status!=='verified')return 'unverified';return riskCore(a,t)};
}
// Also updates previously installed trader pages through their existing hosted script.
const truths=new Map();
if(typeof safeFetchJson==='function'){
 const fetchCore=safeFetchJson;
 window.safeFetchJson=async function(...args){const result=await fetchCore.apply(this,args);const x=result?.truth||result?.data?.truth;if(String(args[0]).includes('/account_truth_v116')&&x?.trader_account_id){truths.set(String(x.trader_account_id),x);setTimeout(paint,0)}return result};
}
function paint(){
 const card=document.getElementById('np-historic-truth-card');if(!card)return;
 const id=String(card.dataset.accountId||'');const x=truths.get(id);if(!x)return;
 const badge=card.querySelector('.np-smart-panel-head .pill'),verdict=card.querySelectorAll('.np-smart-card')[3]?.querySelector('strong');
 const label=x.recorded_breach?'BREACHED':x.ever_crossed_dd_limit?'OVERALL LIMIT CROSSED':x.monitoring_observation_present===false?'AWAITING BROKER UPDATE':'NO OVERALL CROSS RECORDED';
 if(verdict&&verdict.textContent!==label)verdict.textContent=label;
 if(badge){const value=x.recorded_breach?'BREACHED':x.ever_crossed_dd_limit?'OVERALL LIMIT CROSSED':x.monitoring_observation_present===false?'AWAITING BROKER UPDATE':'NO OVERALL CROSS RECORDED';if(badge.textContent!==value)badge.textContent=value;badge.className='pill '+(x.recorded_breach||x.ever_crossed_dd_limit?'pill-red':'pill-yellow')}
 const cr=card.querySelector('#ht-cross');if(cr&&x.recorded_breach){const value='Recorded breach: '+(x.breached_at?new Date(x.breached_at).toLocaleString('en-GB'):'time unavailable');if(cr.textContent!==value)cr.textContent=value}
}
setInterval(()=>{if(!document.hidden)paint()},2000);
})();
