/* Describe the recorded cause, never infer it from a clamped usage percentage. */
window.npAccountBreachReason=function(a){
 a=a||{};const reason=[a.archive_reason,a.breach_reason,a.breach_source].join(' ');
 if(/DAILY_DRAWDOWN|daily.loss|daily.drawdown/i.test(reason)){
  let daily=Number(a.daily_drawdown??a.daily_drawdown_percent);
  try{const marker='|np_risk_controls_v1:',part=String(a.journey_source||'').split(marker)[1]?.split('|')[0];if(part){const raw=part.replace(/-/g,'+').replace(/_/g,'/');daily=Number(JSON.parse(atob(raw+'='.repeat((4-raw.length%4)%4))).daily_drawdown)}}catch{}
  return 'Daily drawdown limit reached'+(daily>0?' ('+daily+'% rule)':'');
 }
 if(/overall|total.drawdown|static.drawdown|trailing.drawdown/i.test(reason))return 'Overall drawdown limit reached';
 if(a.mt5_access_disabled)return 'MT5 access disabled — view Intelligence';
 return 'Recorded breach — view Intelligence';
};
