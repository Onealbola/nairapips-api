const fs=require('fs'),assert=require('assert'),vm=require('vm');
const html=fs.readFileSync(__dirname+'/../website-downloads/trader_clean.html','utf8');
const start=html.indexOf('function npExplicitAttachedReplacementAccounts(){');
const end=html.indexOf('\nfunction npExplicitPayoutRenewalWaitingOpportunity',start);
function run(child){
 const source={id:'old',trader_id:'owner',replacement_account_id:child.id};
 const rows=[source,child];
 const ctx={SESSION:{data:{trader:{id:'owner'},all_accounts:rows},allAccounts:[]},npMergeAccountRows:(...g)=>g.flat(),npUniqueFundedSuccessorForWaitingV140:()=>null};
 vm.createContext(ctx);vm.runInContext(html.slice(start,end),ctx);
 return ctx.npExplicitAttachedReplacementAccounts();
}
const child={id:'child',trader_id:'owner',mt5_login:'123',stage:'funded',account_status:'assigned_active'};
assert.equal(run(child).length,1);
for(const status of ['archived','archived_funded','completed','breached_archived','closed'])assert.equal(run({...child,account_status:status}).length,0,status);
assert.equal(run({...child,archived_at:'2026-10-01'}).length,0);
assert.equal(run({...child,trader_id:'other'}).length,0);
console.log('Passed: archived replacements and other owners cannot become live; genuine live replacement retained.');
