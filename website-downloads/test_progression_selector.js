const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/trader_clean.html','utf8');
function source(name){const start=html.indexOf('function '+name+'(');assert(start>=0,name);const end=html.indexOf('\nfunction ',start+1);return html.slice(start,end<0?html.length:end);}
const ctx={SESSION:{allAccounts:[],data:{trader:{}}},
 npStageRaw:a=>a.stage,npIsBreached:a=>String(a.account_status).includes('breach'),
 npIsResetArchive:a=>String(a.account_status).includes('reset'),
 npIsPassed:a=>String(a.account_status).includes('phase1_passed')||String(a.account_status).includes('archived_phase1')||Number(a.profit_percent)>=10,
 npIsOverviewSelectable:a=>a.account_status!=='archived',
 npFixDuplicateStageSameLogin:a=>a,npHappyExactDeadDisplayRow:()=>false,
 npTraderWaitingForFreshFundedMt5:()=>false,npExplicitResetWaitingAccounts:()=>[],
 npExactResetActionAccounts:()=>[],npExplicitAttachedReplacementAccounts:()=>[],
 npV121ConfirmedValidDashboardMt5:()=>false,npIsTerminalHistory:a=>['closed','archived','breached'].includes(a.account_status),
 mt5Bucket:a=>a.stage==='funded'?'funded':'active',npSortOverviewAccounts:a=>a};
vm.createContext(ctx);
for(const name of ['npProgressionSourceConsumedForever','npUnresolvedPhaseSelectorAccount','npTargetAwaitingPassVerification','npGlobalSelectableAccounts'])vm.runInContext(source(name),ctx);
const phase={id:'phase',purchase_id:'new-purchase',stage:'phase1',mt5_login:'477500736',account_status:'assigned_active',profit_percent:12.43013,created_at:'2026-10-12'};
const funded={id:'funded',purchase_id:'old-purchase',stage:'funded',mt5_login:'477389747',account_status:'profit_protected',created_at:'2026-10-02'};
ctx.SESSION.allAccounts=[phase,funded];
assert.deepEqual(Array.from(ctx.npGlobalSelectableAccounts(),a=>a.id).sort(),['funded','phase']);
assert(ctx.npTargetAwaitingPassVerification(phase));
const passed={...phase,account_status:'archived_phase1',phase_pass_status:'phase1_passed'};
ctx.SESSION.allAccounts=[passed,funded];assert(ctx.npUnresolvedPhaseSelectorAccount(passed));assert(!ctx.npTargetAwaitingPassVerification(passed));
for(const status of ['archived','closed','completed','disabled','breached','archived_reset','recalled'])assert(!ctx.npUnresolvedPhaseSelectorAccount({...phase,account_status:status}),status);
for(const extra of [{successor_account_id:'child'},{replacement_account_id:'child'},{archive_reason:'[NP_CONSUMED:PHASE_PASS:phase]'}])assert(!ctx.npUnresolvedPhaseSelectorAccount({...phase,...extra}));
const child={id:'child',purchase_id:phase.purchase_id,stage:'funded',mt5_login:'next',created_at:'2026-10-16'};
ctx.SESSION.allAccounts=[phase,funded,child];assert(!ctx.npUnresolvedPhaseSelectorAccount(phase));
ctx.SESSION.allAccounts=[phase,funded,{...child,purchase_id:'independent-purchase'}];assert(ctx.npUnresolvedPhaseSelectorAccount(phase));
assert(!ctx.npUnresolvedPhaseSelectorAccount({...phase,profit_percent:9.99}));
assert(!ctx.npUnresolvedPhaseSelectorAccount({...phase,stage:'funded'}));
console.log('Unresolved target-reaching Phase1 remains selectable beside independent funded account; completed/breached/consumed sources stay excluded.');
