const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(m[1].trim())new vm.Script(m[1]);
const code=html.slice(html.indexOf('let npSalesReportSequence=0;'),html.indexOf('function renderPurchases(skipLiveRefresh=false){'));
const ctx={};vm.createContext(ctx);vm.runInContext(code,ctx);
const row=(id,date,plan,paid=false)=>({purchase_id:id,sale_date:date,plan,assigned:true,breached:!paid,passed:paid,funded:paid,funded_breached:false,collected_payout:paid,paid_payout_count:paid?1:0,payout_amount:paid?100.25:0,outcome:paid?'paid_no_funded_breach':'failed_before_funded',outcome_label:'test'});
const rows=[row('old','2026-09-01','Old',true),row('1','2026-10-10','Legend'),row('2','2026-10-11','Legend',true),row('3','2026-10-13','Ruby',true),row('future','2026-10-14','Future',true)];
// Deliberately wrong global cards/plan totals must never reach the dated report.
const report={summary:{sold:999,breached:999,payout_amount:999999},plans:[{plan:'Wrong',sold:999}],rows};
let r=ctx.npSalesScopedReportV224(report,'2026-10-10','2026-10-13');
assert.equal(r.summary.sold,3);assert.equal(r.summary.breached,1);assert.equal(r.summary.collected_payout,2);assert.equal(r.summary.payout_amount,200.5);assert.equal(r.rows.length,3);assert.equal(r.plans.length,2);
for(const key of ['sold','breached','funded','collected_payout','paid_payout_count','payout_amount'])assert.equal(r.plans.reduce((total,p)=>total+p[key],0),r.summary[key]);
assert.equal(ctx.npSalesScopedReportV224(report,'2026-10-11','2026-10-11').summary.sold,1);
assert.equal(ctx.npSalesScopedReportV224(report,'2030-01-01','2030-01-04').summary.sold,0);
assert.equal(ctx.npSalesScopedReportV224(report,'','').summary.sold,5);
assert.throws(()=>ctx.npSalesScopedReportV224({...report,rows:[rows[0],rows[0]]},'',''),/Duplicate/);
assert.throws(()=>ctx.npSalesScopedReportV224({...report,rows:[{...rows[0],payout_amount:'NaN'}]},'',''),/Invalid/);
console.log('Every headline, plan total, money total and purchase row comes from the same selected dates; all-time contamination rejected.');

assert.equal(ctx.npSalesDateISOv225('03/10/2026'),'2026-10-03');
assert.equal(ctx.npSalesDateISOv225('12/10/2026'),'2026-10-12');
assert.equal(ctx.npSalesDateLabelv225('2026-10-03'),'3 October 2026');
for(const bad of ['31/02/2026','29/02/2026','03/13/2026','3/10/2026','03/10'])assert.throws(()=>ctx.npSalesDateISOv225(bad));
assert.equal(ctx.npSalesDateISOv225('29/02/2024'),'2024-02-29');
console.log('DD/MM/YYYY parsing verified: 03/10/2026 is 3 October, never 10 March.');
