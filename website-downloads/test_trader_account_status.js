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
 let selected='old',card;const pending=[];
 function makeCard(id){
  const elements={};for(const id of ['ht-low','ht-high','ht-cross','ht-live'])elements[id]={textContent:''};
  const badge={textContent:'CHECKING'},values=[{textContent:'initial'},{textContent:'initial'},{textContent:'initial'}];
  return {dataset:{accountId:id},elements,badge,values,querySelectorAll:()=>values.map(v=>({querySelector:()=>v})),querySelector:s=>s.startsWith('#')?elements[s.slice(1)]:badge};
 }
 const context={SESSION:{data:{trader:{id:'owner'}}},API:'https://fixture.invalid',Date,encodeURIComponent,htAcct:()=>({id:selected}),htMoney:String,htWhen:v=>v||'Timestamp not retained',document:{getElementById:id=>id==='np-historic-truth-card'?card:null},safeFetchJson:url=>new Promise((resolve,reject)=>pending.push({url,resolve,reject}))};
 const a=html.indexOf('let htRequestVersion=0;'),b=html.indexOf('\nif(typeof window.renderDashboard',a);
 vm.createContext(context);vm.runInContext(html.slice(a,b),context);
 const truth=(id,extra={})=>({truth:{trader_account_id:id,trader_id:'owner',mt5_login:id+'-login',lowest_equity:500,highest_equity:550,max_historic_dd_percent:0,last_monitored_at:'fresh-'+id,monitoring_observation_present:true,...extra}});
 const account=id=>({id,trader_id:'owner',mt5_login:id+'-login'});
 card=makeCard('old');const oldWork=context.htLoad(account('old'));const oldRequest=pending.shift();
 selected='new';card=makeCard('new');const newCard=card;const newWork=context.htLoad(account('new'));const newRequest=pending.shift();
 assert(newRequest.url.includes('mt5_login=new-login'));
 newRequest.resolve(truth('new'));await newWork;assert.equal(newCard.elements['ht-live'].textContent,'fresh-new');
 oldRequest.resolve(truth('old',{highest_equity:999}));await oldWork;assert.equal(newCard.values[1].textContent,'550');assert.equal(newCard.elements['ht-live'].textContent,'fresh-new');
 const mismatched=context.htLoad(account('new'));pending.shift().resolve(truth('old',{highest_equity:999}));await mismatched;assert.equal(newCard.values[1].textContent,'550');assert.equal(newCard.elements['ht-live'].textContent,'Exact account history unavailable');
 const waiting=context.htLoad(account('new'));pending.shift().resolve(truth('new',{monitoring_observation_present:false,last_monitored_at:null}));await waiting;
 assert.equal(newCard.elements['ht-live'].textContent,'Awaiting first broker update');assert.equal(newCard.badge.textContent,'AWAITING BROKER UPDATE');assert.equal(newCard.elements['ht-low'].textContent,'No broker observation recorded');
 const first=context.htLoad(account('new'));const firstRequest=pending.shift();const second=context.htLoad(account('new'));const secondRequest=pending.shift();secondRequest.resolve(truth('new',{highest_equity:600}));await second;firstRequest.resolve(truth('new',{highest_equity:999}));await first;assert.equal(newCard.values[1].textContent,'600');
 const errorWork=context.htLoad(account('new'));const errorRequest=pending.shift();selected='third';card=makeCard('third');errorRequest.reject(Error('timeout'));await errorWork;assert.equal(card.elements['ht-live'].textContent,'');
 console.log('Exact account/login/owner validation, delayed responses, same-account request races, stale failures and missing-observation displays verified.');
})().catch(e=>{console.error(e);process.exitCode=1});
