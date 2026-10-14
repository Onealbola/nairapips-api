const fs=require('fs'),vm=require('vm'),assert=require('assert');
const h=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
const start=h.indexOf('window.adminOpenIntelligence=async function');
const code=h.slice(start,h.indexOf('\n};',start)+3);
let timer,dialog,calls=[],resolve;
const ctx={window:{},API_URL:'https://main-fixture.invalid',SESSION:{token:'fixture-token'},AbortController,encodeURIComponent,Number,Date,String,Math,
 setTimeout:fn=>{timer=fn;return 1},clearTimeout:()=>{},esc:s=>String(s).replaceAll('<','&lt;'),money:s=>String(s),pct:s=>String(s),toast:()=>{},npMonitoringApiUrl:()=> 'https://fixture.invalid',
 modal:()=>{const card={innerHTML:'Loading'};dialog={isConnected:true,card,querySelector:()=>card};return dialog},
 fetch:(url,options)=>{calls.push({url,options});return new Promise(r=>resolve=r)}};
vm.createContext(ctx);vm.runInContext(code,ctx);
(async()=>{
 const work=ctx.window.adminOpenIntelligence('trader','account');assert(dialog);assert.equal(dialog.card.innerHTML,'Loading');assert(calls[0].url.startsWith('https://main-fixture.invalid/admin/breach_intelligence?'));assert(calls[0].url.includes('account_id=account'));assert.equal(calls[0].options.headers.Authorization,'Bearer fixture-token');
 resolve({ok:true,json:async()=>({success:true,data:{account:{mt5_login:'123',account_size:1000},rule:{},timeline:[]}})});await work;assert(dialog.card.innerHTML.includes('MT5 123'));
 ctx.fetch=async()=>({ok:false,status:409,json:async()=>({success:false,error:'Trader/account ownership mismatch'})});await ctx.window.adminOpenIntelligence('trader','account');assert(dialog.card.innerHTML.includes('ownership mismatch'));assert(dialog.card.innerHTML.includes('Retry'));assert(!dialog.card.innerHTML.includes('BREACH CONFIRMED'));
 ctx.fetch=(url,options)=>new Promise((r,reject)=>options.signal.addEventListener('abort',()=>reject(Object.assign(Error('timeout'),{name:'AbortError'}))));const hung=ctx.window.adminOpenIntelligence('trader','account');timer();await hung;assert(dialog.card.innerHTML.includes('25 seconds'));
 ctx.fetch=()=>new Promise(r=>resolve=r);const closed=ctx.window.adminOpenIntelligence('trader','account');const old=dialog;old.isConnected=false;resolve({ok:true,json:async()=>({data:{}})});await closed;assert.equal(old.card.innerHTML,'Loading');
 console.log('Intelligence opens immediately; exact account, authorization, timeout, errors and dismissed dialogs verified.');
})();
