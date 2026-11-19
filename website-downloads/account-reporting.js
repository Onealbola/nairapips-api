/* Exact-account breach reporting; no trading or lifecycle writes. */
(function(){
'use strict';
const release='account-reporting-20261119-recall-visibility-v6';
if(window.NP_ACCOUNT_REPORTING_RELEASE===release)return;
window.NP_ACCOUNT_REPORTING_RELEASE=release;
const corrected=a=>a?.reporting_status==='ASSIGNMENT_CORRECTED';
const breached=a=>!!(a&&(a.recorded_breach||a.breached_at||String(a.account_status||'').toLowerCase().includes('breach')));
if(typeof admAccountLifecycle==='function'){
 const base=admAccountLifecycle;window.admAccountLifecycle=function(a){return corrected(a)?'archived':breached(a)?'breached':base(a)};
 const authority=admAccountBreachAuthority;window.admAccountBreachAuthority=function(a,t){return corrected(a)?false:breached(a)||authority(a,t)};
 window.admComputedAbsoluteDd=function(a){a=a||{};const start=Number(a.start_balance||a.account_size||0),values=[a.current_balance??a.balance,a.current_equity??a.equity].filter(x=>x!=null&&Number.isFinite(Number(x))).map(Number);return start>0&&values.length?Math.max(0,(start-Math.min(...values))/start*100):Math.max(0,Number(a.absolute_drawdown_percent||a.drawdown_percent||0))};
 window.admDdAbsolutePercent=function(a){return admComputedAbsoluteDd(a)};
 const riskCore=admRiskZone;window.admRiskZone=function(a,t){if(corrected(a))return 'corrected';if(breached(a))return 'breached';if(a?.trading_rule_snapshot_status==='repair_required')return 'risk check pending';if(a?.lifecycle_data_status&&a.lifecycle_data_status!=='verified')return 'status check pending';if(admAccountLifecycle(a)==='live'&&['missing','unavailable'].includes(a?.live_data_status))return 'awaiting broker update';if(admAccountLifecycle(a)==='live'&&a?.live_data_status==='stale')return 'stale update';return riskCore(a,t)};
}
// All trader status helpers give an exact recorded breach priority over pass flags.
for(const name of ['npIsBreached','npIsPassed','npTargetAwaitingPassVerification','npDisplayStatus','npFriendlyAccountStatus']){
 const core=window[name];if(typeof core!=='function')continue;
 window[name]=function(a,...args){if(breached(a))return name==='npIsBreached'?true:name==='npIsPassed'||name==='npTargetAwaitingPassVerification'?false:'BREACHED';return core.call(this,a,...args)};
}
function pendingRisk(a){return !breached(a)&&!/(archived|passed|closed|disabled|reset)/i.test(String(a?.account_status||''))&&(a?.trading_rule_snapshot_status==='repair_required'||['missing','unavailable','stale'].includes(a?.live_data_status))}
function presentRisk(html,a){
 if(typeof html!=='string')return html;
 const template=document.createElement('template');template.innerHTML=html;
 // Internal rule investigations belong in admin, including notices injected by
 // an older installed inline patch. Preserve unrelated account/payout notices.
 for(const notice of template.content.querySelectorAll('[data-rule-check-notice],[role="alert"]')){
  if(notice.hasAttribute('data-rule-check-notice')||/saved rule metadata needs repair|Trading rule verification required|Account risk check in progress/.test(notice.textContent))notice.remove();
 }
 if(pendingRisk(a)){
  for(const pill of template.content.querySelectorAll('.np-smart-hero-status .pill,.badge-row .pill')){
   if(/^(SAFE|WARNING|CRITICAL|RULE REVIEW REQUIRED|RISK CHECK PENDING|AWAITING BROKER UPDATE|UPDATE DELAYED|UPDATING)$/i.test(pill.textContent.trim())){pill.textContent='Updating';pill.className='pill pill-gray'}
  }
 }
 return template.innerHTML;
}
if(typeof window.renderDashboard==='function'){
 const renderCore=window.renderDashboard;
 window.renderDashboard=function(...args){const html=renderCore.apply(this,args);const rows=typeof SESSION!=='undefined'?(SESSION.allAccounts||[]):[];const id=typeof SESSION!=='undefined'?String(SESSION.selectedAccountId||SESSION.data?.current_account?.id||''):'';const account=rows.find(a=>String(a.id||'')===id)||(typeof SESSION!=='undefined'?SESSION.data?.current_account:null);return presentRisk(html,account)};
}
if(typeof window.acctCard==='function'){
 const cardCore=window.acctCard;window.acctCard=function(a,...args){return presentRisk(cardCore.call(this,a,...args),a)};
}
// Recalled deliveries remain in the admin audit, never in a trader credential view.
const recalledIds=new Set();
function recalled(a){
 if(!a||typeof a!=='object')return false;
 const status=String(a.account_status||a.status||'').trim().toLowerCase();
 if(['recalled','wrong_assignment_recalled','recalled_wrong_assignment','admin_recalled'].includes(status))return true;
 if(status!=='archived')return false;
 if(a.recalled_delivery===true)return true;
 const text=[a.archive_reason,a.admin_note].join(' '),lower=text.toLowerCase();
 const id=String(a.id||'');
 const exact=(key)=>[...text.matchAll(new RegExp('\\['+key+':([^\\]]+)\\]','gi'))].some(m=>m[1]===id);
 if(lower.includes('[np_management_hold:active]')&&exact('NP_MH_ACCOUNT'))return true;
 if(lower.includes('[np_universal_recall:'))return exact('NP_RECALL_ORIGINAL_ACCOUNT');
 return ['wrong_assignment_recalled','recalled_wrong_assignment','admin_recall_wrong_assignment'].some(v=>lower.includes(v));
}
const accountLists=['accounts','all_accounts','active_accounts','trader_accounts','previous_accounts','archived_accounts','account_history','mt5_accounts','vault_accounts'];
const accountSingles=['account','current_account'];
function purgeRecalled(root){
 if(typeof SESSION==='undefined')return;
 const containers=[root,SESSION.data].filter(v=>v&&typeof v==='object'&&!Array.isArray(v));
 const rows=[...(SESSION.allAccounts||[]),...(SESSION.accounts||[])];
 for(const obj of containers){
  for(const key of accountLists)if(Array.isArray(obj[key]))rows.push(...obj[key]);
  if(Array.isArray(obj.data))rows.push(...obj.data);
  for(const key of accountSingles)if(obj[key])rows.push(obj[key]);
  for(const id of obj.recalled_excluded_account_ids||[])recalledIds.add(String(id));
 }
 for(const row of rows)if(recalled(row)&&row.id)recalledIds.add(String(row.id));
 if(!recalledIds.size)return;
 const keep=row=>!['id','trader_account_id','account_id','source_account_id','completed_account_id'].some(k=>recalledIds.has(String(row?.[k]||'')));
 for(const key of ['allAccounts','accounts','trades','snapshots','previousResetActions'])if(Array.isArray(SESSION[key]))SESSION[key]=SESSION[key].filter(keep);
 for(const obj of containers){
  for(const key of [...accountLists,'data','latest_trades','trades','snapshots','previous_reset_actions','timeline'])if(Array.isArray(obj[key]))obj[key]=obj[key].filter(keep);
  for(const key of accountSingles)if(obj[key]&&!keep(obj[key]))obj[key]=null;
  if(obj.trader&&recalledIds.has(String(obj.trader.current_account_id||''))){
   const current=obj.current_account||obj.account||{};obj.trader.current_account_id=current.id||null;
   for(const field of ['mt5_login','mt5_server','mt5_master_password','mt5_password','master_password','mt5_investor_password','investor_password'])if(field in obj.trader)obj.trader[field]=current[field]||null;
  }
 }
 for(const obj of [...containers,SESSION])for(const key of ['purchases','challenge_purchases']){
  if(Array.isArray(obj[key]))obj[key]=obj[key].map(p=>{
   if(!recalledIds.has(String(p?.trader_account_id||'')))return p;
   const clean={...p};for(const field of ['trader_account_id','mt5_login','current_mt5_login','mt5_server','current_mt5_server','mt5_master_password','mt5_password','master_password','mt5_investor_password','investor_password'])if(field in clean)clean[field]=null;
   return clean;
  });
 }
 const current=containers.map(obj=>obj.current_account||obj.account).find(row=>row&&keep(row));
 for(const key of ['selectedAccountId','lifecycleAccountId','adminFocusAccountId'])if(recalledIds.has(String(SESSION[key]||'')))SESSION[key]=current?.id||null;
}
for(const name of ['renderApp','renderAccounts','renderMT5Vault','showMT5Credentials','copyAllMT5Creds','npGlobalSelectableAccounts']){
 const core=window[name];if(typeof core==='function')window[name]=function(...args){purgeRecalled();return core.apply(this,args)};
}
// Also updates previously installed trader pages through their existing hosted script.
const truths=new Map();
if(typeof safeFetchJson==='function'){
 const fetchCore=safeFetchJson;
 window.safeFetchJson=async function(...args){const result=await fetchCore.apply(this,args);
 const root=result?.data&&!Array.isArray(result.data)?result.data:result;
 purgeRecalled(root);
 if(typeof SESSION!=='undefined'&&Array.isArray(root?.correction_excluded_account_ids)){
  const ids=new Set(root.correction_excluded_account_ids.map(String));
  const keep=r=>!['id','trader_account_id','account_id','source_account_id','completed_account_id'].some(k=>ids.has(String(r?.[k]||'')));
  for(const key of ['allAccounts','accounts','trades','snapshots','previousResetActions'])if(Array.isArray(SESSION[key]))SESSION[key]=SESSION[key].filter(keep);
  if(SESSION.data){
   for(const key of ['all_accounts','accounts','active_accounts'])if(Array.isArray(SESSION.data[key]))SESSION.data[key]=SESSION.data[key].filter(keep);
   if(ids.has(String(SESSION.data.current_account?.id||'')))SESSION.data.current_account=root.current_account||null;
  }
  for(const key of ['selectedAccountId','lifecycleAccountId','adminFocusAccountId'])if(ids.has(String(SESSION[key]||'')))SESSION[key]=root.current_account?.id||null;
 }
 const x=result?.truth||result?.data?.truth;if(String(args[0]).includes('/account_truth_v116')&&x?.trader_account_id){truths.set(String(x.trader_account_id),x);setTimeout(paint,0)}return result};
}
function paint(){
 const card=document.getElementById('np-historic-truth-card');if(!card)return;
 const id=String(card.dataset.accountId||'');const x=truths.get(id);if(!x)return;
 const badge=card.querySelector('.np-smart-panel-head .pill'),verdict=card.querySelectorAll('.np-smart-card')[3]?.querySelector('strong');
 const label=x.recorded_breach?'BREACHED':x.ever_crossed_dd_limit?'OVERALL LIMIT CROSSED':x.monitoring_observation_present===false?'Updating':'NO OVERALL CROSS RECORDED';
 if(verdict&&verdict.textContent!==label)verdict.textContent=label;
 if(badge){const value=x.recorded_breach?'BREACHED':x.ever_crossed_dd_limit?'OVERALL LIMIT CROSSED':x.monitoring_observation_present===false?'Updating':'NO OVERALL CROSS RECORDED';if(badge.textContent!==value)badge.textContent=value;badge.className='pill '+(x.recorded_breach||x.ever_crossed_dd_limit?'pill-red':'pill-yellow')}
 const cr=card.querySelector('#ht-cross');if(cr&&x.recorded_breach){const value='Recorded breach: '+(x.breached_at?new Date(x.breached_at).toLocaleString('en-GB'):'time unavailable');if(cr.textContent!==value)cr.textContent=value}
}
setInterval(()=>{if(!document.hidden)paint()},2000);
})();
