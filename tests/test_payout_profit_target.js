const vm=require('vm'),fs=require('fs'),assert=require('assert');
const a={id:'exact'},q={trader_account_id:'exact',verified:true,start_balance:1000000,verified_profit:49999.99,withdrawal_rules:{minimum_profit_enabled:true,minimum_profit_percent:5},observed_at:new Date().toISOString()};
const button={disabled:false},warning={textContent:''};
const ctx={document:{getElementById:id=>id==='po-submit'?button:warning},SESSION:{data:{payout_quotes:{exact:q}}},npResolvePayoutAccount:()=>a,npPayoutAvailable:()=>({available:30000,verifying:true}),npEffectivePayoutEligibility:()=>({eligible:true}),validatePayoutForm:()=>true};
vm.createContext(ctx);vm.runInContext(fs.readFileSync('website-downloads/payout-profit-target.js','utf8'),ctx);
assert.equal(ctx.npEffectivePayoutEligibility().eligible,false);assert.equal(ctx.npPayoutAvailable().available,0);assert.equal(ctx.validatePayoutForm(),false);assert.equal(button.disabled,true);assert.ok(warning.textContent.includes('5%'));
q.verified_profit=50000;assert.equal(ctx.npEffectivePayoutEligibility().eligible,true);assert.equal(ctx.validatePayoutForm(),true);
q.observed_at=new Date(Date.now()-181000).toISOString();assert.equal(ctx.npEffectivePayoutEligibility().eligible,false);
q.withdrawal_rules.minimum_profit_enabled=false;assert.equal(ctx.npEffectivePayoutEligibility().eligible,true);
console.log('Target blocks below threshold and stale quotes; exact threshold and plans without targets retain eligibility.');

q.withdrawal_rules.minimum_profit_enabled=false;q.payout_schedule_met=false;q.payout_schedule_reason='Your biweekly payout opens later.';
assert.equal(ctx.npEffectivePayoutEligibility().eligible,false);assert.equal(ctx.validatePayoutForm(),false);assert.equal(warning.textContent,q.payout_schedule_reason);
q.payout_schedule_met=true;assert.equal(ctx.npEffectivePayoutEligibility().eligible,true);
