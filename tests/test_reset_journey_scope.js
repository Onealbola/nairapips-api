const vm=require('vm'),fs=require('fs'),assert=require('assert');
const a={id:'new',purchase_id:'newpurchase',account_status:'breached_archived'};
const old={id:'oldpurchase',second_life_used:true};
const ctx={SESSION:{allAccounts:[a],selectedAccountId:'new',data:{trader:{current_account_id:'new'},lifecycle_authority:{journey:'second_life',purchase_id:'oldpurchase'},purchases:[old,{id:'newpurchase'}]}},npBootstrapLifecycle(){throw Error('Unrelated lifecycle must not mutate selection');},npActiveSecondLifeContext(){return ctx.SESSION.data.purchases.find(p=>p.second_life_used)||null;},npPrimaryResetOpportunity(){return {source_account_id:'old'};},npResetOpportunityForSource(id){return id==='new'?{source_account_id:id,price:6000}:null;},npSynchronizeGlobalLifecycleDisplay(){throw Error('Unrelated waiting history must not override breach');},npJourneyTerminalNoActive:()=>({old:true}),npPreviousResetActionForAccount:id=>id==='new'?{eligible:true,code:'PAYMENT_REQUIRED'}:null,npIsBreached:()=>true};
vm.createContext(ctx);vm.runInContext(fs.readFileSync('website-downloads/reset-journey-scope.js','utf8'),ctx);
assert.equal(ctx.npBootstrapLifecycle(),null);assert.equal(ctx.npActiveSecondLifeContext(),null);assert.equal(ctx.SESSION.data.purchases[0],old);assert.equal(ctx.npPrimaryResetOpportunity().price,6000);ctx.npSynchronizeGlobalLifecycleDisplay();assert.equal(ctx.SESSION.selectedAccountId,'new');
console.log('Purchased reset remains available; unrelated Second Life cannot take over archived breach.');

assert.equal(ctx.npJourneyTerminalNoActive(),null);
