const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/admin_clean.html','utf8');
const code=html.slice(html.indexOf('function npLoadOperations(force=false){'),html.indexOf('function npOpsEsc('));
const cached={summary:{action_required:9},actions:[{id:'known'}]};const OPS={data:cached,loading:false};
const ctx={OPS,Date,api:async()=>{throw Error('server busy')}};vm.createContext(ctx);vm.runInContext(code,ctx);
(async()=>{
 await assert.rejects(ctx.npLoadOperations(true),/server busy/);assert.strictEqual(OPS.data,cached);
 let release,calls=0;ctx.api=()=>{calls++;return new Promise(r=>{release=r})};
 const one=ctx.npLoadOperations(true),two=ctx.npLoadOperations(true);assert.strictEqual(one,two);assert.equal(calls,1);
 release({summary:{action_required:1},actions:[]});await one;assert.equal(OPS.data.summary.action_required,1);
 ctx.api=async()=>({});await assert.rejects(ctx.npLoadOperations(true),/Complete Action Centre/);assert.equal(OPS.data.summary.action_required,1);
 console.log('Operations requests coalesce; failed or incomplete replies preserve previous valid data.');
})();
