/* Payout targets apply to the selected account, including pending verification. */
(function(){
 function blocked(){
  const a=npResolvePayoutAccount()||{},q=SESSION.data?.payout_quotes?.[String(a.id||'')]||a.payout_quote;
  if(!q||String(q.trader_account_id||'')!==String(a.id||''))return null;
  if(q.payout_schedule_met===false)return q.payout_schedule_reason||'Your payout schedule is not due yet.';
  if(q.next_payout_at&&Date.parse(q.next_payout_at)>Date.now())return q.payout_schedule_reason||'Your payout schedule is not due yet.';
  const t=q.withdrawal_rules;
  if(!t?.minimum_profit_enabled)return null;
  const age=q.observed_at?(Date.now()-Date.parse(q.observed_at))/1000:0;
  const fresh=q.verified===true&&Number.isFinite(age)&&age<=180&&age>=-60;
  const met=fresh&&Number(q.start_balance)>0&&Number(q.verified_profit)>=Number(q.start_balance)*Number(t.minimum_profit_percent)/100;
  return met?null:(q.payout_target_reason||(!fresh?'Your profit target must be verified against a fresh account update before payout.':`This plan requires ${t.minimum_profit_percent}% verified profit before payout.`));
 }
 const available=npPayoutAvailable;
 npPayoutAvailable=function(){const result=available(),reason=blocked();return reason?Object.assign({},result,{available:0,verifying:false,reason}):result;};
 const eligibility=npEffectivePayoutEligibility;
 npEffectivePayoutEligibility=function(){const result=eligibility(),reason=blocked();return reason?Object.assign({},result,{eligible:false,reason}):result;};
 const validate=validatePayoutForm;
 validatePayoutForm=function(){const reason=blocked();if(reason){const button=document.getElementById('po-submit'),warning=document.getElementById('po-warning');if(button)button.disabled=true;if(warning)warning.textContent=reason;return false;}return validate();};
})();
