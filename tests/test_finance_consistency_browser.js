const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
const html=fs.readFileSync('website-downloads/admin_clean.html','utf8');
const section=(a,b)=>html.slice(html.indexOf(a),html.indexOf(b,html.indexOf(a)));
const code=fs.readFileSync('website-downloads/finance-rules.js','utf8')+'\n'+section('function approvedPurchase(p){','function purchaseHasRealAssignment')+'\n'+section('function purchaseFee(p){','function purchaseLinkedAccountDate')+'\n'+section('function purchaseSalesDate(p){','function purchaseCreatedDate')+'\n'+section('function purchaseInRange(ts,range){','function npNetTradingResult')+'\n'+section('function npPayoutExactRecord(p){','window.npPayoutIntelSetRange=')+'\n'+section('function npRevenueNumber(row, keys){','async function revenueModule()');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
 for(const timezoneId of ['Africa/Lagos','America/Los_Angeles','Asia/Tokyo']){
  const page=await browser.newPage({timezoneId});await page.clock.install({time:new Date('2026-10-24T06:30:00Z')});
  await page.evaluate(()=>{
   window.SESSION={token:'fixture',purchases:[],payouts:[],traders:[],traderAccounts:[]};window.productionMode=()=> 'live';window.revenueLaunchDateValue=()=> '2026-09-01';
   window.PURCHASE_FILTER_RANGE='month';window.PURCHASE_CUSTOM_FROM='';window.PURCHASE_CUSTOM_TO='';window.NP_PAYOUT_INTEL={range:'month',sort:'exposure',q:''};
  });await page.addScriptTag({content:code});
  await page.evaluate(()=>{
   const purchases=[{id:'old',status:'approved',created_at:'2026-05-01',approved_at:'2026-10-24',fee:999},{id:'wat-start',status:'approved',created_at:'2026-09-30T23:01:00Z',fee:100,final_fee:80},{id:'before',status:'approved',created_at:'2026-09-30T22:59:00Z',fee:50},{id:'late-approval',status:'approved',created_at:'2026-10-20',approved_at:'2026-11-01',fee:20},{id:'excluded',status:'approved',created_at:'2026-10-20',fee:1000,excluded_from_revenue:true}];
   const payouts=[{id:'wat-start',status:'paid',paid_at:'2026-09-30T23:01:00Z',created_at:'2026-09-20',amount:10,trader_id:'t'},{id:'before',status:'paid',paid_at:'2026-09-30T22:59:00Z',amount:5,trader_id:'t'},{id:'date-proof',status:'cancelled',paid_at:'2026-10-20',amount:20,trader_id:'t'},{id:'missing-date',status:'paid',created_at:'2026-10-20',amount:500,trader_id:'t'},{id:'pending',status:'pending',created_at:'2026-10-20',amount:100,trader_id:'t'},{id:'test',status:'paid',paid_at:'2026-10-20',amount:500,mark_as_test:true,trader_id:'t'}];
   npFinanceSourceState().data={purchases,payouts};
  });
  const result=await page.evaluate(()=>({purchaseSales:purchaseSalesTotal('month'),purchasePayouts:npPaidPayoutTotalInRange('month'),intel:npPayoutIntelBuild([]).paidTotal,revenue:npRevenueLocalSummaries({from_date:'2026-10-01',to_date:'2026-10-24'}).summary,month:npFinanceRangeLabel('month'),last30:npFinanceRangeLabel('30d')}));
  assert.equal(result.purchaseSales,100);assert.equal(result.purchasePayouts,30);assert.equal(result.intel,30);assert.equal(result.revenue.range_sales,100);assert.equal(result.revenue.range_payouts,30);assert.equal(result.revenue.gross_revenue,1149);assert(result.month.includes('1 Oct'));assert(result.last30.includes('25 Sept'));
  for(const range of ['all','today','week','7d','30d','month']){
   const x=await page.evaluate(range=>{NP_PAYOUT_INTEL.range=range;const b=npFinanceBounds(range);const rev=npRevenueLocalSummaries({from_date:Number.isFinite(b.fromMs)?npFinanceDay(new Date(b.fromMs)):'',to_date:npFinanceDay(new Date(b.toMs))}).summary;return {a:npPaidPayoutTotalInRange(range),b:npPayoutIntelBuild([]).paidTotal,c:rev.range_payouts,s:purchaseSalesTotal(range),rs:rev.range_sales};},range);
   assert.equal(x.a,x.b,range);assert.equal(x.a,x.c,range);assert.equal(x.s,x.rs,range);
  }
  await page.close();
 }console.log('PASS: Purchases/Payouts/Revenue reconcile for identical periods across three browser timezones, transaction vs approval dates, WAT midnight, paid-date evidence, test exclusions, full snapshot and all-time history.');
 }finally{await browser.close()}})().catch(e=>{console.error(e);process.exit(1)});
