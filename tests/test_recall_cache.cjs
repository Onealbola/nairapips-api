const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
(async()=>{
 const stale={id:'held',account_status:'assigned_active',mt5_login:'477761875'},fresh={id:'new',account_status:'assigned_active',mt5_login:'valid'};
 let response={accounts:[fresh],recalled_excluded_account_ids:['held'],current_account:fresh};
 const c={document:{getElementById:()=>null},setInterval:()=>{},setTimeout:()=>{},SESSION:{allAccounts:[stale,fresh],accounts:[stale,fresh],selectedAccountId:'held',trades:[{trader_account_id:'held'}],data:{all_accounts:[stale,fresh],current_account:stale,trader:{current_account_id:'held',mt5_master_password:'revoked'}}},safeFetchJson:async()=>response,renderAccounts:()=>c.SESSION.allAccounts};
 c.window=c;vm.createContext(c);vm.runInContext(fs.readFileSync('website-downloads/account-reporting.js','utf8'),c);
 await c.safeFetchJson('/trader_accounts');assert.deepEqual(c.SESSION.allAccounts,[fresh]);assert.equal(c.SESSION.selectedAccountId,'new');assert.equal(c.SESSION.data.trader.mt5_master_password,null);
 // An older request completing after recall cannot restore the cached delivery.
 response={accounts:[stale,fresh]};await c.safeFetchJson('/trader_accounts');assert.deepEqual(response.accounts,[fresh]);
 c.SESSION.allAccounts=[stale,fresh];assert.deepEqual(c.renderAccounts(),[fresh]);
 // A valid replacement referencing the recall is never hidden.
 const replacement={id:'replacement',account_status:'assigned_active',admin_note:'[NP_UNIVERSAL_RECALL:v] [NP_RECALL_ORIGINAL_ACCOUNT:held]'};
 c.SESSION.allAccounts=[replacement];assert.deepEqual(c.renderAccounts(),[replacement]);
 console.log('PASS: stale response, cache, selection and credential cleanup; fresh replacement preserved.');
})().catch(e=>{console.error(e);process.exitCode=1});
