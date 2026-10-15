const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
const begin=html.indexOf('window.NP_PAYOUT_INTEL =');
const end=html.indexOf('\nfunction npStopPayoutActionEvent',begin);
let output='';const content={set innerHTML(v){output=v},get innerHTML(){return output}};
const ctx={window:{},SESSION:{payouts:[],traders:[{id:'a',name:'Ada',email:'ada@test'},{id:'b',name:'Bola'}],traderAccounts:[{id:'old',trader_id:'a',mt5_login:'OLD-100',status:'archived'},{id:'current',trader_id:'a',mt5_login:'CURRENT-999'},{id:'other',trader_id:'b',mt5_login:'OTHER'}],_rejectedPayoutNotesCleanupRequested:true},Date,console,setInterval:()=>1,setTimeout:fn=>{fn();return 1},clearTimeout:()=>{},api:()=>Promise.reject(Error('No network in test')),esc:v=>String(v??''),money:v=>'N'+v,kpi:(a,b)=>`<kpi>${a}:${b}</kpi>`,tag:v=>v,toast:()=>{},npAdminPayoutDateTime:v=>String(v||'—'),npAdminPayoutPending:p=>p.status==='pending',npAdminPayoutEffectiveStatus:p=>p.status,rangeOf:(xs,f)=>xs.reduce((r,x)=>(r[f(x)]=(r[f(x)]||0)+1,r),{}),document:{getElementById:id=>id==='content'?content:null}};
vm.createContext(ctx);vm.runInContext(html.slice(begin,end),ctx);
const st=ctx.window.NP_PAYOUT_INTEL;st.range='custom';st.from='2026-10-14T10:00';st.to='2026-10-14T10:00';
const fixture=[
{id:'1',trader_id:'a',trader_account_id:'old',status:'paid',amount:100,requested_at:'2026-10-01T00:00:00Z',paid_at:'2026-10-14T09:00:00Z'},
{id:'2',trader_id:'a',mt5_login:'OLD-200',status:'paid',amount:200,paid_at:'2026-10-14T09:00:59.999Z'},
{id:'3',trader_id:'a',status:'paid',amount:300,paid_at:'2026-10-14T09:01:00Z'},
{id:'4',trader_id:'b',status:'paid',amount:400,paid_at:'2026-10-14T09:00:30Z'},
{id:'5',trader_id:'a',status:'pending',amount:50,requested_at:'2026-10-14T09:00:30Z'},
{id:'6',trader_id:'a',status:'cancelled',amount:999,requested_at:'2026-10-14T09:00:30Z'},
{id:'7',trader_id:'a',status:'approved',amount:70,requested_at:'2026-10-01T00:00:00Z',approved_at:'2026-10-14T09:00:30Z'},
{id:'8',trader_id:'a',status:'paid',amount:999,paid_at:'2026-10-14T08:59:59.999Z'}];
ctx.SESSION.payouts=fixture;
assert.equal(ctx.npPayoutFiltered(fixture).length,6);
st.q='ada';let intel=ctx.npPayoutIntelBuild(fixture);assert.equal(intel.paidTotal,300);assert.equal(intel.pendingTotal,50);assert.equal(intel.approvedTotal,70);assert.equal(intel.exposureTotal,420);assert.equal(intel.rows.length,1);
ctx.renderPayouts();assert(output.includes('5 matching payout(s)'));assert(output.includes('Paid:2'));assert(output.includes('Pending:1'));assert(output.includes('Rejected / Cancelled:1'));assert(output.includes('OLD-100'));assert(output.includes('OLD-200'));assert(!output.includes('CURRENT-999'));assert(!output.includes('Bola'));assert(output.includes('Paid MT5 account history'));
st.q='old-100';assert.equal(ctx.npPayoutIntelBuild(fixture).paidTotal,100);assert.equal(ctx.npPayoutFiltered(fixture).length,1);
assert.equal(ctx.npPayoutExactRecord({trader_id:'a',account_id:'other'}).mt5_login,undefined);
assert.equal(ctx.npPayoutExactRecord({trader_id:'a'}).mt5_login,undefined);
st.range='all';st.q='';ctx.SESSION.payouts=Array.from({length:25},(_,i)=>({id:String(i),trader_id:'a',mt5_login:'HIST-'+i,status:'paid',amount:10,paid_at:'2026-10-14T09:00:00Z'}));
ctx.renderPayouts();assert(output.includes('PAID OUT · ALL TIME:N250'));assert(output.includes('1–10 of 25'));assert.equal((output.match(/Retry Funded Renewal/g)||[]).length,10);
ctx.window.npPayoutGoPage('requests',3);assert(output.includes('21–25 of 25'));assert.equal((output.match(/Retry Funded Renewal/g)||[]).length,5);assert(output.includes('PAID OUT · ALL TIME:N250'));
ctx.window.npPayoutIntelSetSearch('HIST-24');assert.equal(st.pages.requests,1);assert(output.includes('1 matching payout(s)'));assert(output.includes('PAID OUT · ALL TIME:N10'));
ctx.window.npPayoutSetPageSize(20);assert.equal(st.pageSize,20);
st.q='nomatch';ctx.renderPayouts();assert(output.includes('0 matching payout(s)'));assert(output.includes('PAID OUT · ALL TIME:N0'));
// All scripts must parse, including embedded modal templates.
for(const block of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(block[1].trim())new vm.Script(block[1]);
console.log('Payout scope verified: WAT minute boundaries, paid/approval/request dates, search totals, exact archived MT5, ownership, all cards/tables, pagination and full-script syntax.');
