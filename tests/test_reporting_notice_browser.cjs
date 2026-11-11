const {chromium}=require('playwright');const fs=require('fs');const assert=require('assert');
(async()=>{const browser=await chromium.launch({headless:true,executablePath:'/usr/bin/chromium',args:['--no-sandbox']});const page=await browser.newPage();
await page.setContent('<html><body></body></html>');
await page.evaluate(()=>{
 window.NP_ACCOUNT_REPORTING_RELEASE='account-reporting-20261111';
 window.SESSION={selectedAccountId:'a',allAccounts:[{id:'a',rule_snapshot_status:'repair_required',trading_rule_snapshot_status:'repair_required'}],data:{}};
 window.renderDashboard=()=>'<section role="alert">Your payout is processing</section><section role="alert">Trading rule verification required: saved rule metadata needs repair</section><section class="np-smart-hero-status"><span class="pill pill-green">SAFE</span></section>';
 window.npIsBreached=()=>false;
 window.acctCard=a=>'<div class="badge-row"><span class="pill pill-yellow">PHASE1</span><span class="pill pill-green">'+(npIsBreached(a)?'BREACHED':'SAFE')+'</span></div>';
});
await page.addScriptTag({path:'website-downloads/account-reporting.js'});
let html=await page.evaluate(()=>renderDashboard());assert(html.includes('Updating'));assert(!html.includes('RISK CHECK PENDING'));assert(!html.includes('>SAFE<'));assert(!html.includes('Account risk check in progress'));assert(!html.includes('data-rule-check-notice'));assert(!html.includes('metadata'));assert(!html.includes('Trading rule verification required'));assert(html.includes('Your payout is processing'));assert(!html.includes('Please contact support'));
html=await page.evaluate(()=>acctCard(SESSION.allAccounts[0]));assert(html.includes('Updating'));assert(!html.includes('RISK CHECK PENDING'));assert(!html.includes('>SAFE<'));
html=await page.evaluate(()=>{SESSION.allAccounts[0].trading_rule_snapshot_status='verified';return renderDashboard()});assert(html.includes('>SAFE<'));assert(!html.includes('risk check in progress'));
html=await page.evaluate(()=>{SESSION.allAccounts[0].live_data_status='missing';return renderDashboard()});assert(html.includes('Updating'));assert(!html.includes('AWAITING BROKER UPDATE'));assert(!html.includes('>SAFE<'));
html=await page.evaluate(()=>{SESSION.allAccounts[0].trading_rule_snapshot_status='repair_required';SESSION.allAccounts[0].breached_at='2026-11-11';window.renderDashboard=()=>'<div>BREACHED</div>';return acctCard(SESSION.allAccounts[0])});assert(!html.includes('RISK CHECK PENDING'));assert(html.includes('BREACHED'));
console.log('Browser checks passed: internal trader banners removed, neutral Updating status, older patch upgrade, unrelated alerts retained and breach priority preserved.');await browser.close();})().catch(e=>{console.error(e);process.exit(1)});
