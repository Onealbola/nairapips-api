const {chromium}=require('playwright'),fs=require('fs'),assert=require('assert');
(async()=>{const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});try{
const page=await browser.newPage();await page.setContent('<main id="root"></main>');await page.addScriptTag({content:fs.readFileSync('website-downloads/account-plan-selector.js','utf8')});
for(const width of [320,390,1440]){
 await page.setViewportSize({width,height:900});
 await page.evaluate(()=>{const product={id:'future-trailing',family:'standard',name:'Future trailing',purchases_open:false,plan:{id:'future',name:'Future trailing',account_size:1000000,fee:10000,max_drawdown:10,drawdown_type:'TRAILING',trailing_formula:'highest_closed_balance_percent',phase1_target:10,phase2_target:0,payout_split:70,payout_cycles:3}};document.querySelector('#root').innerHTML=NPPlanSelector.render([product],{key:'future-'+innerWidth,context:'trader'})});
 const text=await page.locator('.nps-summary').innerText();assert(text.includes('Highest closed balance'));assert(text.includes('Peak closed balance × 90%'));assert(text.includes('Does not raise the breach line'));assert(await page.locator('.nps-buy').isDisabled());
 assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),`overflow at ${width}`);
}
console.log('PASS: prospective trailing formula and paused checkout display correctly at 320, 390 and 1440 pixels.');
}finally{await browser.close()}})().catch(err=>{console.error(err);process.exit(1)});
