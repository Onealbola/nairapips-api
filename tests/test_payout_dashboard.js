// Run against the corrected trader HTML: node tests/test_payout_dashboard.js FILE
const fs=require('node:fs');
const vm=require('node:vm');
const assert=require('node:assert/strict');
const html=fs.readFileSync(process.argv[2],'utf8');
function extract(name,endName){return html.slice(html.indexOf('function '+name+'('),html.indexOf(endName,html.indexOf('function '+name+'(')));}
const account={id:'exact',mt5_login:'123',account_status:'assigned_active',start_balance:1000};
const quote={trader_account_id:'exact',verified:false,last_verified_available:true,start_balance:1000,current_equity:1200,verified_profit:200,payout_split:60,available_payout:120,observed_at:new Date(Date.now()-240000).toISOString()};
const fields={'po-amount':{value:'150'},'po-method':{value:'bank'},'po-bank':{value:'bank'},'po-acct':{value:'123'},'po-name':{value:'name'},'po-warning':{},'po-submit':{}};
const ctx={SESSION:{currentTab:'payouts',data:{payout_quotes:{exact:quote}}},document:{getElementById:id=>fields[id]},
npResolvePayoutAccount:()=>account,npIsFundedPayoutAccount:()=>true,npIsActivePayoutAccount:()=>true,npPayoutLedgerReady:()=>true,
npPayoutAccountStatusBlob:()=>account.account_status,npV124IsExactAderetiFundedPayoutAccount:()=>false,npOpenPayoutRequest:()=>null,
npAwaitingFreshFundedMt5AfterPaid:()=>false,npRejectedPayoutHold:()=>null};
vm.createContext(ctx);
for(const [name,end] of [['npEffectivePayoutEligibility','function npPayoutAvailable'],['npPayoutAvailable','function npPayoutAwaitingVerification'],['npPayoutAwaitingVerification','function npNormalizedPayoutStatus'],['validatePayoutForm','async function cancelPendingPayout']]){
 vm.runInContext(extract(name,end),ctx);
}
assert.equal(ctx.npPayoutAvailable().available,120,'last verified amount stays visible');
assert.equal(ctx.npPayoutAvailable().verifying,true);
assert.equal(ctx.npEffectivePayoutEligibility().eligible,true,'delayed update does not close requests');
assert.equal(ctx.validatePayoutForm(),true,'requested amount awaits verification, not stale maximum');
assert.equal(fields['po-submit'].disabled,false);
fields['po-bank'].value='';assert.equal(ctx.validatePayoutForm(),false,'payment details still required');fields['po-bank'].value='bank';
quote.verified=true;quote.observed_at=new Date().toISOString();
assert.equal(ctx.validatePayoutForm(),false,'fresh verified maximum still applies');
fields['po-amount'].value='100';assert.equal(ctx.validatePayoutForm(),true);
assert.equal(ctx.npPayoutAwaitingVerification({status:'pending',admin_note:'[NP_PAYOUT_VERIFY_PENDING]'}),true);
assert.equal(ctx.npPayoutAwaitingVerification({status:'approved',admin_note:''}),false);
console.log('Trader quote display, delayed submission, form validation and pending-label checks passed');
