const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/trader_clean.html','utf8');
const start=html.indexOf('function npTerminalRecoveryForAccount('),end=html.indexOf('\nfunction renderDashboard',start);
const ctx={npIsBreached:a=>String(a.account_status||'').includes('breach')};vm.createContext(ctx);vm.runInContext(html.slice(start,end),ctx);
const op={kind:'terminal',sourceId:'old'};
assert.equal(ctx.npTerminalRecoveryForAccount(op,{id:'new',account_status:'assigned_active',mt5_login:'477522430'}),false);
assert.equal(ctx.npTerminalRecoveryForAccount(op,{id:'old',account_status:'assigned_active'}),false);
assert.equal(ctx.npTerminalRecoveryForAccount(op,{id:'old',account_status:'breached',breached_at:'yesterday'}),true);
assert.equal(ctx.npTerminalRecoveryForAccount({kind:'terminal'},{id:'new',account_status:'assigned_active'}),false);
assert(html.includes('${npTerminalRecoveryForAccount(dashboardRecovery,acct)'));
assert(html.includes('const terminalNoActive=npJourneyTerminalNoActive();'));
for(const block of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(block[1].trim())new vm.Script(block[1]);
console.log('Terminal warning follows exact account; active replacements remain active; terminal-history guard preserved; all scripts parse.');

(async()=>{
 const elements={};for(const id of ['np-historic-truth-card','ht-low','ht-high','ht-cross','ht-live'])elements[id]={textContent:''};
 const badge={textContent:'WITHIN LIMIT'};elements['np-historic-truth-card'].querySelectorAll=()=>[];elements['np-historic-truth-card'].querySelector=()=>badge;
 const context={SESSION:{data:{trader:{id:'owner'}}},API:'https://fixture.invalid',Date,encodeURIComponent,htMoney:String,htWhen:v=>v||'Timestamp not retained',document:{getElementById:id=>elements[id]},safeFetchJson:async()=>({truth:{monitoring_observation_present:false,last_monitored_at:null}})};
 const a=html.indexOf('async function htLoad(a)'),b=html.indexOf('\nif(typeof window.renderDashboard',a);
 vm.createContext(context);vm.runInContext(html.slice(a,b),context);await context.htLoad({id:'new',trader_id:'owner'});
 assert.equal(elements['ht-live'].textContent,'Awaiting first broker update');assert.equal(badge.textContent,'AWAITING BROKER UPDATE');assert.equal(elements['ht-low'].textContent,'No broker observation recorded');
 console.log('No-observation response shows pending broker update instead of a fresh/healthy monitoring claim.');
})().catch(e=>{console.error(e);process.exitCode=1});
