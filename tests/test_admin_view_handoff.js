const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(process.argv[2],'utf8');
const begin=html.indexOf('function npRenderAdminShell(){');
const end=html.indexOf("window.addEventListener('message'",begin);
assert(begin>=0&&end>begin);
function check(stored){
 const intervals=[];let paints=0,acks=0;const records=new Map();
 const payload={type:'NAIRAPIPS_ADMIN_VIEW_BOOTSTRAP',trader_id:'exact',auth_token:'signed',bootstrap:{trader:{id:'exact'}}};
 if(stored)records.set('np_admin_view_handoff_test',JSON.stringify(payload));
 const context={SESSION:{},URLSearchParams,console,document:{title:'Trader'},btoa:s=>Buffer.from(s).toString('base64'),unescape,encodeURIComponent,
 localStorage:{getItem:k=>records.get(k),removeItem:k=>records.delete(k)},
 setInterval:f=>(intervals.push(f),intervals.length),clearInterval:()=>{},setTimeout:()=>{},
 npRenderAdminOpeningScreen:()=>{},npAdminShellTimeoutGuard:()=>{},
 npHydrateAdminBootstrapPayload:(data,id,token)=>data.trader.id===id&&token==='signed',renderApp:()=>paints++};
 context.window={location:{search:'?admin_shell=1&handoff=test',origin:'https://site.example',pathname:'/dashboard/trader_clean.html'},history:{replaceState:()=>{}},opener:{postMessage:p=>{if(p.type==='NAIRAPIPS_ADMIN_VIEW_OPENED')acks++;}}};
 vm.createContext(context);vm.runInContext(html.slice(begin,end),context);context.npRenderAdminShell();
 if(!stored){records.set('np_admin_view_handoff_test',JSON.stringify(payload));intervals[0]();}
 assert.equal(paints,1);assert.equal(acks,1);assert.equal(context.SESSION.__adminViewStarting,false);assert.equal(records.size,0);
 context.window.__npHydrateAdminShellPayload(payload);assert.equal(paints,1);
}
check(true);check(false);console.log('Immediate and delayed Admin View handoffs render once and acknowledge securely.');
