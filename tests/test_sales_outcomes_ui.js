const fs=require('fs'),vm=require('vm'),assert=require('assert');
const s=fs.readFileSync(process.argv[2] || '/workspace/payout-dashboard-fix/admin_sales_outcomes.html','utf8');
for(const m of s.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(m[1].trim())new vm.Script(m[1]);
const js=s.slice(s.indexOf('let npSalesReportSequence=0;'),s.indexOf('function renderPurchases(skipLiveRefresh=false){'));
const box={isConnected:true},content={},fields={'np-sales-from':{value:'2026-10-01'},'np-sales-to':{value:'2026-10-14'},'np-sales-result':box,content};
const ctx={window:{},document:{getElementById:id=>fields[id]},URLSearchParams,money:x=>'₦'+x,api:async()=>({success:true,summary:{sold:1,breached:1,passed:1,funded:1,breached_rate:100,passed_rate:100,funded_rate:100,awaiting_assignment:0,no_milestone:0},plans:[{plan:'<script>evil</script>',sold:1,breached:1,passed:1,funded:1}],rows:[]})};
vm.createContext(ctx);vm.runInContext(js,ctx);
(async()=>{await ctx.loadSalesOutcomes();assert(box.innerHTML.includes('COUNTS')||box.innerHTML.includes('ACCOUNTS SOLD'));assert(!box.innerHTML.includes('<script>evil'));ctx.api=async()=>{throw Error('Unavailable')};await ctx.loadSalesOutcomes();assert.equal(box.textContent,'Unavailable');console.log('All inline scripts parse; report rendering, escaping and failure checks passed');})();
