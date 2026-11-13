/* A purchased journey, including its archived breach, owns its recovery display. */
(function(){
 function anchor(){
  const d=SESSION.data||{},rows=[...(SESSION.allAccounts||[]),...(d.all_accounts||[]),...(d.accounts||[]),...(d.current_account?[d.current_account]:[])];
  const id=SESSION.adminFocusAccountId||(SESSION.userSelectedAccountId?SESSION.selectedAccountId:null)||d.lifecycle_authority?.focus_account_id||d.trader?.current_account_id||d.current_account?.id||SESSION.selectedAccountId;
  return rows.find(a=>String(a.id)===String(id))||null;
 }
 function belongs(o,a){
  if(!o||!a)return !a;
  const pid=o.purchase_id||o.parent_purchase_id||o.focus_purchase_id||o.purchase?.id;
  if(pid)return String(pid)===String(a.purchase_id||a.challenge_purchase_id);
  return String(o.source_account_id||o.account_id||o.source?.id||'')===String(a.id);
 }
 const bootstrap=npBootstrapLifecycle;
 npBootstrapLifecycle=function(){const a=anchor(),lc=SESSION.data?.lifecycle_authority;if(a&&lc?.journey==='second_life'&&!belongs(lc,a))return null;return bootstrap();};
 const second=npActiveSecondLifeContext;
 npActiveSecondLifeContext=function(){
  const a=anchor();if(!a)return second();
  const d=SESSION.data||{}, saved=[d.purchases,d.challenge_purchases,SESSION.purchases];
  const pid=String(a.purchase_id||a.challenge_purchase_id||'');
  try{
   if(Array.isArray(saved[0]))d.purchases=saved[0].filter(p=>String(p.id)===pid);
   if(Array.isArray(saved[1]))d.challenge_purchases=saved[1].filter(p=>String(p.id)===pid);
   if(Array.isArray(saved[2]))SESSION.purchases=saved[2].filter(p=>String(p.id)===pid);
   return second();
  }finally{d.purchases=saved[0];d.challenge_purchases=saved[1];SESSION.purchases=saved[2];}
 };
 const primary=npPrimaryResetOpportunity;
 npPrimaryResetOpportunity=function(){const a=anchor();if(!a)return primary();return npResetOpportunityForSource(a.id)||null;};
 const terminal=npJourneyTerminalNoActive;
 npJourneyTerminalNoActive=function(){const a=anchor(),action=a?npPreviousResetActionForAccount(a.id):null;if(action&&action.eligible&&['PAYMENT_REQUIRED','PAYMENT_UNDER_REVIEW','PAYMENT_APPROVED_WAITING_MT5'].includes(action.code))return null;return terminal();};
 const synchronize=npSynchronizeGlobalLifecycleDisplay;
 npSynchronizeGlobalLifecycleDisplay=function(){const a=anchor();if(a&&npIsBreached(a))return;return synchronize();};
})();
