const assert=require('assert'),vm=require('vm'),fs=require('fs');
(async()=>{
 let sent;
 const ctx={URL,location:{href:'https://nairapips.com/dashboard/'},window:{fetch:async(input,opts)=>{sent=opts;return {ok:false,json:async()=>({success:false,error:'[Errno 11] Resource temporarily unavailable'})}}}};
 vm.createContext(ctx);vm.runInContext(fs.readFileSync('website-downloads/public-messages.js','utf8'),ctx);
 const opts={headers:{Authorization:'Bearer fixture'}};const r=await ctx.window.fetch('https://nairapips-api.onrender.com/np2_referral/activity',opts);
 assert.equal((await r.json()).error,'This service is temporarily unavailable. Please try again shortly.');assert.equal(sent,opts);
 assert.equal(ctx.window.npPublicMessage('Maximum trader payout is 37500'),'Maximum trader payout is 37500');
 for(const f of ['trader_login.html','trader_clean.html','referral.html']){
  const html=fs.readFileSync('website-downloads/'+f,'utf8');for(const m of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g))new vm.Script(m[1]);
  assert(!/location\.(?:href|replace)\s*(?:=|\()\s*['"]https:\/\/nairapips-api\.onrender/.test(html));
 }
 console.log('Public messages preserve auth and business errors; dashboard navigation stays on current domain');
})();
