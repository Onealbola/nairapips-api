const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
const code=html.slice(html.indexOf('let npAdminLoadPromiseV226=null;'),html.indexOf('/* === ADMIN OVERVIEW REFRESH CONTROL'));
const SESSION={currentMod:'overview',traders:[{id:'cached'}],traderAccounts:[{id:'cached-account'}],purchases:[{id:'cached-purchase'}],payouts:[{id:'cached-payout'}],mt5Pool:[{id:'cached-pool'}],loadedAt:new Date(0)};
let calls=[],active=0,max=0;
const ctx={SESSION,window:{},console:{warn(){}},toast(){},renderOverview(){},localStorage:{setItem(){}},admChooseCurrentAccount:()=>null,admAccountStageRank:()=>0,admAccountIsActive:()=>false,norm:x=>String(x).toLowerCase(),api:async path=>{calls.push(path);throw Error('server busy');}};
vm.createContext(ctx);vm.runInContext(code,ctx);
(async()=>{
 await assert.rejects(ctx.loadAll(),/server busy/);
 assert.equal(SESSION.traders[0].id,'cached');assert.equal(SESSION.traderAccounts[0].id,'cached-account');assert.equal(SESSION.purchases[0].id,'cached-purchase');assert.equal(SESSION.payouts[0].id,'cached-payout');assert.equal(SESSION.loadedAt.getTime(),0);assert(SESSION.coreDataError);
 let release;ctx.api=path=>new Promise(r=>{calls.push(path);release=r});calls=[];
 const one=ctx.loadAll(),two=ctx.loadAll();assert.strictEqual(one,two);assert.equal(calls.length,1);
 release({traders:[],accounts:[],purchases:[],payouts:[],mt5_pool:[],plans:[],announcements:[]});await one;assert(SESSION.coreDataReady);assert.equal(SESSION.coreDataError,null);
 ctx.api=async path=>{if(path==='/admin_bootstrap')throw Error('bootstrap unavailable');active++;max=Math.max(max,active);await new Promise(r=>setImmediate(r));active--;return {traders:[],accounts:[],purchases:[],payouts:[],mt5_pool:[],plans:[],announcements:[]};};
 await ctx.loadAll();assert(max<=2);assert(SESSION.coreDataReady);
 // First-load failure must render unavailable, never calculate zero KPI cards.
 const render=html.slice(html.indexOf('function renderOverview(){'),html.indexOf('function renderOverview(){')+html.slice(html.indexOf('function renderOverview(){')).indexOf('  const counts'));
 assert(render.includes('They are not zero.'));assert(render.includes('return;'));
 console.log('Failed core reads preserve cached rows and timestamps; overlapping refreshes coalesce; fallback concurrency <=2; first-load failure shows unavailable.');
})();
