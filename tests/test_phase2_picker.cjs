const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
(async()=>{for(const file of ['admin_clean.html','admin_clean_cpanel.html']){
 const html=fs.readFileSync('website-downloads/'+file,'utf8'),a=html.indexOf('window.smartAssignPhase ='),b=html.indexOf('\n};',a)+4;
 const source={id:'source',trader_id:'owner',account_size:1500000,account_status:'archived_phase1',mt5_login:'old'};
 const rows=[{id:'fresh',pool_class:'phase',account_size:1500000,status:'available'},{id:'wrong-pool',pool_class:'funded',account_size:1500000,status:'available'},{id:'wrong-size',pool_class:'phase',account_size:500000,status:'available'}];
 const c={window:{},SESSION:{traders:[{id:'owner',account_size:500000}]},String,Number,npRefreshFullMT5Pool:async()=>rows,npTraderAllExactAccounts:()=>[source],npMT5ExpectedPoolForStage:s=>s==='funded'?'funded':'phase',npMT5PoolClass:m=>m.pool_class,npMT5RowAvailable:m=>m.status==='available',npMT5PickerSort:r=>r,npMT5PickerOptions:(r,stage,size)=>{c.options={rows:r,stage,size};return '<option>fresh</option>';},npShowResetModalSafe:h=>{c.modal=h;},toast:m=>{c.error=m;},esc:String,money:String,api:()=>{throw Error('No mutation before an explicit selection')}};
 vm.createContext(c);vm.runInContext(html.slice(a,b),c);await c.window.smartAssignPhase('queue','owner','phase2','ref','source');
 assert.equal(c.error,undefined);assert.equal(c.options.stage,'phase2');assert.equal(c.options.size,1500000);assert.deepEqual(c.options.rows.map(r=>r.id),['fresh']);assert(c.modal.includes('PHASE2'));assert(c.modal.includes('PHASE vault'));
 c.modal=null;await c.window.smartAssignPhase('queue','owner','phase2','ref','different-source');assert.equal(c.modal,null);assert(c.error.includes('exact source account'));
}console.log('PASS: Phase 2 opens exact-source phase-vault review in both Admin pages, rejects wrong sizes/vaults and never mutates before confirmation.');})().catch(e=>{console.error(e);process.exitCode=1});
