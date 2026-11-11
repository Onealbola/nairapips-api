const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(process.argv[2]||'website-downloads/admin_clean.html','utf8');
const start=source.indexOf('const NP_ADMIN_READ_QUEUE=[];');
const end=source.indexOf('async function sb(',start);
const context={Promise,Map,AbortController,setTimeout,clearTimeout,SESSION:{token:'test'},ADMIN_INFLIGHT:new Map(),API_URL:'https://example.test',npClearAdminSession(){throw Error('unexpected auth reset')}};
let active=0,max=0,calls=0,releases=[];
context.fetch=async(url,opts)=>{
 calls++;active++;max=Math.max(max,active);
 await new Promise(r=>releases.push(r));active--;
 return {ok:true,status:200,text:async()=>JSON.stringify({success:true,url})};
};
vm.createContext(context);vm.runInContext(source.slice(start,end),context);
const tick=()=>new Promise(r=>setImmediate(r));
(async()=>{
 const reads=Array.from({length:7},(_,i)=>context.api('/module'+i));
 const duplicate=context.api('/module0');
 await tick();assert.equal(calls,2);assert.equal(max,2);
 // Mutations remain immediate and are never queued behind slow reads.
 const write=context.api('/write','POST',{value:1});
 await tick();assert.equal(calls,3);
 while(releases.length || active || calls<8){
  releases.splice(0).forEach(r=>r());await tick();
 }
 const result=await Promise.all([...reads,duplicate,write]);
 assert.equal(calls,8);assert.equal(result.length,9);assert.equal(context.ADMIN_INFLIGHT.size,0);
 // Rejected reads release capacity for the next module.
 context.fetch=async()=>{throw new Error('application failure')};
 await Promise.all(Array.from({length:4},(_,i)=>context.api('/failure'+i).catch(e=>e)));
 await tick();assert.equal(context.ADMIN_INFLIGHT.size,0);
 console.log('Admin global read queue: concurrency, deduplication, writes and failure recovery passed');
})().catch(e=>{console.error(e);process.exitCode=1});
