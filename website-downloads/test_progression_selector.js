const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(__dirname+'/trader_clean.html','utf8');
function source(name){const start=html.indexOf('function '+name+'(');assert(start>=0,name);const end=html.indexOf('\nfunction ',start+1);return html.slice(start,end<0?html.length:end);}
const ctx={SESSION:{allAccounts:[],data:{trader:{}}},
 npStageRaw:a=>a.stage,npIsBreached:a=>String(a.account_status).includes('breach'),
 npIsResetArchive:a=>String(a.account_status).includes('reset'),
 npIsWaitingAccount:()=>false,npIsPassedArchive:a=>/archived_phase[12]|phase[12]_passed/.test(a.account_status),npProfitPct:a=>Number(a.profit_percent||0),npTargetFor:a=>Number(a.target_percent||10),
 npIsOverviewSelectable:a=>a.account_status!=='archived',
 npFixDuplicateStageSameLogin:a=>a,npHappyExactDeadDisplayRow:()=>false,
 npTraderWaitingForFreshFundedMt5:()=>false,npExplicitResetWaitingAccounts:()=>[],
 npExactResetActionAccounts:()=>[],npExplicitAttachedReplacementAccounts:()=>[],
 npV121ConfirmedValidDashboardMt5:()=>false,npIsTerminalHistory:a=>['closed','archived','breached'].includes(a.account_status),
 mt5Bucket:a=>a.stage==='funded'?'funded':'active',npSortOverviewAccounts:a=>a};
vm.createContext(ctx);
for(const name of ['npIsPassed','npHasReachedPhaseTarget','npProgressionSourceConsumedForever','npUnresolvedPhaseSelectorAccount','npTargetAwaitingPassVerification','npGlobalSelectableAccounts'])vm.runInContext(source(name),ctx);
const phase={id:'phase',purchase_id:'new-purchase',stage:'phase1',mt5_login:'477500736',account_status:'assigned_active',profit_percent:12.43013,created_at:'2026-10-12'};
const funded={id:'funded',purchase_id:'old-purchase',stage:'funded',mt5_login:'477389747',account_status:'profit_protected',created_at:'2026-10-02'};
ctx.SESSION.allAccounts=[phase,funded];
assert.deepEqual(Array.from(ctx.npGlobalSelectableAccounts(),a=>a.id).sort(),['funded','phase']);
assert(ctx.npTargetAwaitingPassVerification(phase));assert(!ctx.npIsPassed(phase));assert(ctx.npHasReachedPhaseTarget(phase));
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

const old={id:'old-phase',purchase_id:'old-purchase',stage:'phase1',mt5_login:'477299291',account_status:'archived_phase1',profit_percent:30,updated_at:'2026-10-16',archive_reason:'PHASE1 PASSED | [NP_CONSUMED:PHASE_PASS:old-phase] replacement_account_id=funded'};
ctx.SESSION.allAccounts=[phase,funded,old];assert(ctx.npProgressionSourceConsumedForever(old));assert(!ctx.npUnresolvedPhaseSelectorAccount(old));assert.deepEqual(Array.from(ctx.npGlobalSelectableAccounts(),a=>a.id).sort(),['funded','phase']);
const decorated={...old,archive_reason:null};assert(ctx.npProgressionSourceConsumedForever(decorated));
vm.runInContext(source('npNextWaitingLabel'),ctx);assert.equal(ctx.npNextWaitingLabel(phase),'TARGET REACHED · VERIFICATION PENDING');assert.equal(ctx.npNextWaitingLabel(old),'COMPLETED');
console.log('Numeric target never becomes recorded pass; exact completion receipts exclude old sources even after timestamps change or display decoration strips receipt fields.');
ctx.npIsResetWaiting=()=>false;
for(const name of ['npDisplayStatus','npFriendlyAccountStatus'])vm.runInContext(source(name),ctx);
ctx.SESSION.allAccounts=[phase,funded,old];
assert.equal(ctx.npDisplayStatus(phase),'TARGET REACHED · VERIFICATION PENDING');
assert.equal(ctx.npFriendlyAccountStatus(phase),'TARGET REACHED · VERIFICATION PENDING');
assert.equal(ctx.npIsPassed(phase),false);
assert.equal(ctx.npIsPassed(passed),true);
console.log('Account status, friendly status and progression label consistently distinguish target reached from recorded pass.');
