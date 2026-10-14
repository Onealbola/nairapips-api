const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');const start=html.indexOf('window.showCreateMT5 = function');const code=html.slice(start,html.indexOf('window.showBulkMT5',start));
const elements={};for(const [key,value] of Object.entries({login:'123456',server:'fixture',plan:'fixture',size:'750000',pool:'phase',master:'private-fixture-master',inv:'private-fixture-view',note:''}))elements['cm-'+key]={value};
elements['cm-add-btn']={textContent:'＋ Add',style:{},disabled:false};elements['cm-error']={textContent:'',style:{}};
let removed=0,calls=0;const messages=[];
const ctx={window:{},SESSION:{mt5Pool:[]},document:{getElementById:id=>elements[id],querySelector:()=>({remove:()=>removed++})},setTimeout:()=>1,clearTimeout:()=>{},toast:(m,type)=>messages.push({m,type}),npShowResetModalSafe:()=>{},api:async()=>{calls++;return{success:true,data:{mt5:[{id:'new',mt5_login:'123456'}],automation_retry:{scheduled:true}}}},audit:async()=>{},renderMT5Pool:()=>{},reload:()=>{}};
vm.createContext(ctx);vm.runInContext(code,ctx);
(async()=>{
 await ctx.window.createMT5();assert.equal(ctx.SESSION.mt5Pool[0].id,'new');assert.equal(removed,1);assert(!elements['cm-add-btn'].disabled);
 ctx.api=async()=>{throw Error('fixture server refusal')};await ctx.window.createMT5();assert(elements['cm-error'].textContent.includes('fixture server refusal'));assert.equal(elements['cm-error'].style.display,'block');assert.equal(removed,1);
 ctx.api=async()=>({success:true,data:{mt5:[]}});await ctx.window.createMT5();assert(elements['cm-error'].textContent.includes('could not be confirmed'));assert.equal(removed,1);
 let release;calls=0;ctx.api=()=>{calls++;return new Promise(r=>release=r)};const first=ctx.window.createMT5();await ctx.window.createMT5();assert.equal(calls,1);release({data:{mt5:[{id:'new',mt5_login:'123456'}]}});await first;
 console.log('Pool create confirms the exact saved row, preserves inline errors and prevents duplicate submissions.');
})();
