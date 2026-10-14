const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(m[1].trim())new vm.Script(m[1]);
const code=html.slice(html.indexOf('let npSalesReportSequence=0;'),html.indexOf('function renderPurchases(skipLiveRefresh=false){'));
const fields={'np-sales-from':{value:'2026-10-13'},'np-sales-to':{value:'2026-10-13'},'np-sales-result':{isConnected:true},content:{}};
let timer;const calls=[];
const payload=(from,to)=>{
 const sold=from==='2026-10-13'?2:4;
 const outcomes={awaiting_account:0,no_recorded_result:0,passed_not_funded:0,failed_before_funded:sold,funded_no_payout:0,funded_breached_no_payout:0,paid_no_funded_breach:0,paid_and_funded_breached:0,needs_review:0};
 return {success:true,from,to,summary:{sold,breached:sold,failed_before_funded:sold,not_funded_yet:0,funded:0,needs_review:0,collected_payout:0,paid_payout_count:0,payout_amount:0,outcomes},plans:[],rows:Array.from({length:sold},(_,i)=>({purchase_id:'purchase-'+i,sale_date:from,plan:'Legend',breached:true,passed:false,funded:false,funded_breached:false,collected_payout:false,paid_then_breached_closed:false,assigned:true,paid_payout_count:0,payout_amount:0,outcome:'failed_before_funded',outcome_label:'Breached before funded'}))};
};
const ctx={window:{},document:{getElementById:id=>fields[id]},URLSearchParams,money:v=>'₦'+v,setTimeout:fn=>(timer=fn,1),clearTimeout:()=>{timer=null},api:async path=>{calls.push(path);const q=new URLSearchParams(path.split('?')[1]);return payload(q.get('from'),q.get('to'));}};
vm.createContext(ctx);vm.runInContext(code,ctx);
(async()=>{
 await ctx.loadSalesOutcomes();assert(fields['np-sales-result'].innerHTML.includes('13 October 2026 → 13 October 2026'));assert(fields['np-sales-result'].innerHTML.includes('Total bought: 2'));
 fields['np-sales-from'].value=fields['np-sales-to'].value='2026-10-12';ctx.npSalesDatesChanged();assert.equal(fields['np-sales-result'].textContent,'Dates changed. Updating results…');assert.equal(ctx.window.npSalesFrom,'2026-10-12');assert(timer);timer();await new Promise(r=>setImmediate(r));
 assert(calls[1].includes('from=2026-10-12'));assert(fields['np-sales-result'].innerHTML.includes('Total bought: 4'));assert(fields['np-sales-result'].innerHTML.includes('12 October 2026 → 12 October 2026'));
 let release;ctx.api=()=>new Promise(r=>{release=r});const old=ctx.loadSalesOutcomes();
 fields['np-sales-from'].value=fields['np-sales-to'].value='2026-10-13';ctx.npSalesDatesChanged();ctx.api=async()=>payload('2026-10-13','2026-10-13');await ctx.loadSalesOutcomes();
 release(payload('2026-10-12','2026-10-12'));await old;assert(fields['np-sales-result'].innerHTML.includes('Total bought: 2'));
 ctx.api=async()=>payload('','');await ctx.loadSalesOutcomes();assert(fields['np-sales-result'].textContent.includes('different date period'));
 fields['np-sales-from'].value='2026-10-15';await ctx.loadSalesOutcomes();assert(fields['np-sales-result'].textContent.includes('start date'));
 assert(html.includes('id="np-sales-from" type="text" inputmode="numeric" placeholder="DD/MM/YYYY" maxlength="10" oninput="npSalesDatesChanged()" onchange="npSalesDatesChanged()"'));assert(html.includes('id="np-sales-to" type="text" inputmode="numeric" placeholder="DD/MM/YYYY" maxlength="10" oninput="npSalesDatesChanged()" onchange="npSalesDatesChanged()"'));
 console.log('Inline scripts parse; date changes automatically refresh correct totals; stale and mismatched periods blocked; invalid dates rejected.');
})();
