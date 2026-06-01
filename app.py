<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NairaPips Master Command Center</title>
<script src="https://cdn.tailwindcss.com"></script>

<style>
body{background:#050505;color:white;font-family:Arial,sans-serif}
.gold{color:#d4af37}.bg-gold{background:#d4af37;color:#000}
.card{background:#111;border:1px solid rgba(212,175,55,.18)}
.card2{background:#090909;border:1px solid rgba(255,255,255,.08)}
.vault{background:linear-gradient(135deg,#141414,#050505);border:1px solid rgba(212,175,55,.35);box-shadow:0 0 35px rgba(212,175,55,.08)}
.deep{background:#080808}
.sidebar-btn{width:100%;text-align:left;padding:12px 14px;border-radius:14px;color:#ccc;font-weight:600}
.sidebar-btn:hover,.sidebar-btn.active{background:#d4af37;color:#000;font-weight:bold}
input,select,textarea{background:#000;border:1px solid #333;color:white;border-radius:12px;padding:12px;width:100%}
.btn{padding:10px 15px;border-radius:12px;font-weight:bold}
.btn-gold{background:#d4af37;color:#000}.btn-red{background:#991b1b;color:white}.btn-dark{background:#000;border:1px solid #444;color:white}.btn-green{background:#166534;color:white}
.badge{padding:5px 10px;border-radius:999px;background:#1f1f1f;border:1px solid rgba(212,175,55,.25);color:#d4af37;font-size:12px;font-weight:bold}
table{width:100%;min-width:1250px;border-collapse:collapse}
th,td{padding:13px;border-bottom:1px solid rgba(255,255,255,.08);text-align:left;vertical-align:top}
th{color:#d4af37}.tableWrap{overflow:auto}
.loader{width:38px;height:38px;border:4px solid #333;border-top-color:#d4af37;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* Payout Command Center overflow fixes */
.kpi-amount{
  font-size:clamp(22px,2.2vw,36px);
  line-height:1.05;
  white-space:normal;
  overflow-wrap:anywhere;
  word-break:break-word;
  max-width:100%;
}
.payout-amount-main{
  font-size:clamp(28px,3vw,48px);
  line-height:1.02;
  white-space:normal;
  overflow-wrap:anywhere;
  word-break:break-word;
  max-width:100%;
}
.payout-field{
  min-width:0;
  overflow:hidden;
}
.payout-field-value{
  font-size:clamp(16px,1.35vw,22px);
  line-height:1.18;
  white-space:normal;
  overflow-wrap:anywhere;
  word-break:break-word;
  max-width:100%;
}
.payout-date-value{
  font-size:clamp(14px,1.15vw,18px);
  line-height:1.22;
  white-space:normal;
  overflow-wrap:anywhere;
}
.payout-bank-grid{
  grid-template-columns:repeat(auto-fit,minmax(190px,1fr));
}
.payout-date-grid{
  grid-template-columns:repeat(auto-fit,minmax(185px,1fr));
}
@media(max-width:900px){
  .payout-amount-main{font-size:34px}
  .kpi-amount{font-size:26px}
}


.monitor-meter{height:12px;border-radius:999px;background:#111;overflow:hidden;border:1px solid rgba(255,255,255,.08)}
.monitor-safe{background:linear-gradient(90deg,#22c55e,#84cc16)}
.monitor-warning{background:linear-gradient(90deg,#eab308,#f97316)}
.monitor-danger{background:linear-gradient(90deg,#f97316,#ef4444)}
.monitor-breached{background:linear-gradient(90deg,#991b1b,#ef4444)}
.monitor-text{font-size:clamp(18px,1.8vw,32px);line-height:1.05;overflow-wrap:anywhere;word-break:break-word}
.monitor-card{min-width:0;overflow:hidden}


/* Monitoring Intelligence UI */
.timeline-dot{width:14px;height:14px;border-radius:999px;background:#eab308;box-shadow:0 0 18px rgba(234,179,8,.45);flex:0 0 auto}
.timeline-line{width:2px;background:rgba(234,179,8,.25);margin-left:6px}
.evidence-grid{grid-template-columns:repeat(auto-fit,minmax(210px,1fr))}
.evidence-value{font-size:clamp(20px,2vw,34px);line-height:1.05;overflow-wrap:anywhere;word-break:break-word}
.zone-safe{color:#22c55e}.zone-warning{color:#eab308}.zone-danger{color:#fb923c}.zone-critical,.zone-breached{color:#ef4444}


/* NairaPips premium plan + marketing CRM refinements */
.plan-hero{background:radial-gradient(circle at top left,rgba(212,175,55,.18),transparent 35%),linear-gradient(135deg,#151515,#050505);border:1px solid rgba(212,175,55,.38);box-shadow:0 0 42px rgba(212,175,55,.08)}
.plan-card{background:linear-gradient(160deg,#141414,#060606);border:1px solid rgba(212,175,55,.32);box-shadow:0 18px 45px rgba(0,0,0,.35);position:relative;overflow:hidden}
.plan-card:before{content:"";position:absolute;inset:0;background:radial-gradient(circle at 20% 0%,rgba(212,175,55,.14),transparent 28%);pointer-events:none}
.plan-price{font-size:clamp(32px,4vw,64px);line-height:1;font-weight:900;color:#d4af37;overflow-wrap:anywhere}
.plan-size{font-size:clamp(24px,2.5vw,44px);line-height:1.05;font-weight:900}
.plan-pill{padding:7px 11px;border-radius:999px;background:rgba(212,175,55,.1);border:1px solid rgba(212,175,55,.28);color:#f5d76e;font-size:12px;font-weight:900;letter-spacing:.04em}
.crm-toolbar{position:sticky;top:0;z-index:10;background:rgba(5,5,5,.88);backdrop-filter:blur(12px);border:1px solid rgba(212,175,55,.22)}
.crm-contact-card{background:linear-gradient(145deg,#111,#050505);border:1px solid rgba(212,175,55,.20);transition:.2s ease}
.crm-contact-card:hover{border-color:rgba(212,175,55,.55);transform:translateY(-1px)}
.crm-check{width:auto;accent-color:#d4af37}


/* MT5 Pool Vault beauty + password visibility fixes */
.mt5-vault-shell{display:grid;grid-template-columns:minmax(360px,430px) minmax(0,1fr);gap:24px;align-items:start}
.mt5-panel-title{font-size:clamp(24px,2.4vw,34px);line-height:1.05}
.mt5-password-box{background:linear-gradient(135deg,rgba(212,175,55,.08),rgba(255,255,255,.025));border:1px solid rgba(212,175,55,.26);border-radius:22px;padding:16px;overflow:hidden}
.mt5-password-row{display:grid;grid-template-columns:minmax(0,1fr);gap:10px;margin-bottom:14px}
.mt5-password-actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
.mt5-password-actions .btn,.mt5-mini-actions .btn{padding:10px 12px;font-size:13px;white-space:nowrap;text-align:center}
.mt5-password-input{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;letter-spacing:.4px;font-size:15px;min-height:48px;overflow:visible;text-overflow:clip}
.mt5-mini-actions{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
.mt5-card{background:linear-gradient(135deg,#101010,#050505);border:1px solid rgba(212,175,55,.18);box-shadow:0 16px 40px rgba(0,0,0,.28)}
.mt5-secret-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.mt5-secret{background:#060606;border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:14px;min-width:0;overflow:hidden}
.mt5-secret-value{display:block;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:13px;line-height:1.3;color:#f5f5f5;white-space:normal;overflow-wrap:anywhere;word-break:break-word;margin-top:6px}
.mt5-info-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.mt5-stock-card{position:relative;overflow:hidden}
.mt5-stock-card:before{content:"";position:absolute;inset:0;background:radial-gradient(circle at top right,rgba(212,175,55,.13),transparent 35%);pointer-events:none}

.mt5-assigned-card{position:relative;overflow:hidden;background:linear-gradient(135deg,#101010,#050505);border:1px solid rgba(212,175,55,.22);box-shadow:0 18px 45px rgba(0,0,0,.32)}
.mt5-assigned-card:before{content:"";position:absolute;inset:0;background:radial-gradient(circle at top right,rgba(212,175,55,.12),transparent 38%);pointer-events:none}
.mt5-assigned-card>*{position:relative;z-index:1}
.mt5-assigned-head{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:16px;align-items:start}
.mt5-assigned-login{font-size:clamp(22px,2vw,32px);line-height:1.05;overflow-wrap:anywhere;word-break:break-word}
.mt5-assigned-size{text-align:right;min-width:145px}
.mt5-assigned-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(185px,1fr));gap:12px}
.mt5-assigned-field{background:#060606;border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:14px;min-width:0;overflow:hidden}
.mt5-assigned-field b,.mt5-assigned-value{display:block;line-height:1.28;white-space:normal;overflow-wrap:anywhere;word-break:break-word;max-width:100%}
.mt5-assigned-note{background:rgba(255,255,255,.025);border:1px solid rgba(212,175,55,.12);border-radius:18px;padding:14px;overflow-wrap:anywhere;word-break:break-word}
.mt5-assigned-actions{display:flex;flex-wrap:wrap;gap:10px;margin-top:14px}
@media(max-width:640px){.mt5-assigned-head{grid-template-columns:1fr}.mt5-assigned-size{text-align:left;min-width:0}.mt5-assigned-actions .btn{width:100%;text-align:center}}
@media(max-width:1100px){.mt5-vault-shell{grid-template-columns:1fr}.mt5-password-actions,.mt5-mini-actions{grid-template-columns:1fr 1fr}.mt5-mini-actions button:last-child{grid-column:1/-1}}
@media(max-width:640px){.mt5-secret-grid{grid-template-columns:1fr}.mt5-password-actions,.mt5-mini-actions{grid-template-columns:1fr}}



/* MT5 bulk creation from Challenge Plan - isolated add-on */
.mt5-plan-factory{background:radial-gradient(circle at top left,rgba(212,175,55,.18),transparent 32%),linear-gradient(135deg,#121212,#050505);border:1px solid rgba(212,175,55,.34);box-shadow:0 20px 55px rgba(0,0,0,.34),0 0 36px rgba(212,175,55,.06)}
.mt5-plan-factory-grid{display:grid;grid-template-columns:minmax(320px,420px) minmax(0,1fr);gap:20px;align-items:start}
.mt5-plan-preview{background:#070707;border:1px solid rgba(212,175,55,.20);border-radius:22px;padding:16px;min-height:150px}
.mt5-plan-preview-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(135px,1fr));gap:10px;margin-top:14px}
.mt5-plan-preview-item{background:#040404;border:1px solid rgba(255,255,255,.08);border-radius:15px;padding:12px;min-width:0;overflow:hidden}
.mt5-bulk-box{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;min-height:190px;line-height:1.5;resize:vertical}
.mt5-factory-actions{display:flex;flex-wrap:wrap;gap:10px}
.mt5-factory-help{background:rgba(212,175,55,.06);border:1px solid rgba(212,175,55,.16);border-radius:18px;padding:14px;color:#aaa}
@media(max-width:1100px){.mt5-plan-factory-grid{grid-template-columns:1fr}}

/* Referral Marketing Module - safe visual patch */
.referral-bar{background:linear-gradient(135deg,rgba(212,175,55,.16),rgba(12,12,12,.98));border:1px solid rgba(212,175,55,.32);box-shadow:0 14px 40px rgba(0,0,0,.28),0 0 35px rgba(212,175,55,.08)}
.referral-chip{display:inline-flex;align-items:center;gap:7px;padding:8px 12px;border-radius:999px;background:#070707;border:1px solid rgba(212,175,55,.24);color:#d4af37;font-size:12px;font-weight:800;white-space:nowrap}
.referral-hero{background:radial-gradient(circle at top left,rgba(212,175,55,.22),transparent 34%),linear-gradient(135deg,#15120a,#050505 65%);border:1px solid rgba(212,175,55,.38);box-shadow:0 0 45px rgba(212,175,55,.1)}
.referral-grid{grid-template-columns:repeat(auto-fit,minmax(230px,1fr))}
.referral-input-row{display:grid;grid-template-columns:1fr auto;gap:10px}
.referral-link-box{background:#050505;border:1px solid rgba(212,175,55,.25);border-radius:18px;padding:14px;overflow-wrap:anywhere;word-break:break-word;color:#f5e6ae}
.referral-plan-card{background:linear-gradient(180deg,#101010,#070707);border:1px solid rgba(255,255,255,.08);box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
@media(max-width:760px){.referral-input-row{grid-template-columns:1fr}.referral-bar .btn{width:100%}}


/* Staff RBAC Command Center */
.permission-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px}
.staff-permission-card{background:rgba(255,255,255,.035);border:1px solid rgba(212,175,55,.18);border-radius:22px;padding:16px}
.staff-switch{display:flex;align-items:center;gap:8px;font-size:13px;color:#ddd;margin-top:8px}
.staff-switch input{width:auto;accent-color:#d4af37}
.staff-pill{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;border:1px solid rgba(212,175,55,.25);background:#111;color:#d4af37;font-size:12px;font-weight:800}
.staff-danger-zone{border:1px solid rgba(239,68,68,.35);background:rgba(127,29,29,.12)}


/* Compact MT5 Reset Button */
.btn-mt5-reset{background:#d4af37;color:#000;margin-top:8px;display:block;width:100%;text-align:center}


/* Lead follow-up system */
.lead-hot{border:1px solid rgba(239,68,68,.45);background:linear-gradient(135deg,rgba(239,68,68,.13),#080808)}
.lead-warm{border:1px solid rgba(212,175,55,.35);background:linear-gradient(135deg,rgba(212,175,55,.10),#080808)}
.lead-action-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}



/* Production safety controls - black-gold preserved */
.production-toggle{display:inline-grid;grid-template-columns:1fr 1fr;gap:4px;background:#050505;border:1px solid rgba(212,175,55,.25);border-radius:16px;padding:4px}
.production-toggle button{border-radius:12px;padding:9px 12px;font-weight:900;color:#aaa}
.production-toggle button.active{background:#d4af37;color:#000}
.production-warning{border:1px solid rgba(239,68,68,.35);background:linear-gradient(135deg,rgba(127,29,29,.18),rgba(5,5,5,.96))}
.test-badge{border-color:rgba(239,68,68,.35);color:#fb7185;background:rgba(127,29,29,.14)}

</style>
</head>

<body>

<div id="loginScreen" class="min-h-screen flex items-center justify-center p-6">
  <div class="vault p-8 rounded-3xl max-w-md w-full">
    <h1 class="text-4xl font-black mb-2">Naira<span class="gold">Pips</span></h1>
    <p class="text-gray-400 mb-6">Master Command Center</p>
    <input id="adminUser" placeholder="Username" class="mb-4">
    <input id="adminPass" type="password" placeholder="Password" class="mb-6">
    <button onclick="adminLogin()" class="btn btn-gold w-full">Enter Command Center</button>
    <p id="loginError" class="text-red-400 mt-4 hidden">Invalid login</p>
  </div>
</div>

<div id="app" class="hidden min-h-screen">
<div class="grid lg:grid-cols-[315px_1fr]">

<aside class="deep border-r border-yellow-900/30 min-h-screen p-5">
  <h1 class="text-3xl font-black mb-2">Naira<span class="gold">Pips</span></h1>
  <p class="text-gray-500 text-sm mb-8">Capital Operating System</p>

  <div class="space-y-2">
    <button class="sidebar-btn active" data-module="overview" onclick="setModule('overview',this)">Dashboard Overview</button>
    <button class="sidebar-btn" data-module="payments" onclick="setModule('payments',this)">Payments</button>
    <button class="sidebar-btn" data-module="traders" onclick="setModule('traders',this)">Users / Traders</button>
    <button class="sidebar-btn" data-module="addTrader" onclick="setModule('addTrader',this)">Add Trader</button>
    <button class="sidebar-btn" data-module="bulkTrader" onclick="setModule('bulkTrader',this)">Bulk Add Traders</button>
    <button class="sidebar-btn" data-module="timeline" onclick="setModule('timeline',this)">Timeline Intelligence</button>
    <button class="sidebar-btn" data-module="plans" onclick="setModule('plans',this)">Challenge Plans</button>
    <button class="sidebar-btn" data-module="purchases" onclick="setModule('purchases',this)">Challenge Purchases</button>
    <button class="sidebar-btn" data-module="mt5pool" onclick="setModule('mt5pool',this)">MT5 Pool Vault</button>
    <button class="sidebar-btn" data-module="payouts" onclick="setModule('payouts',this)">Payouts</button>
    <button class="sidebar-btn" data-module="funded" onclick="setModule('funded',this)">Funded Traders</button>
    <button class="sidebar-btn" data-module="revenue" onclick="setModule('revenue',this)">Revenue</button>
    <button class="sidebar-btn" data-module="database" onclick="setModule('database',this)">Users Database</button>
    <button class="sidebar-btn" data-module="leads" onclick="setModule('leads',this)">Lead Follow-Up</button>
    <button class="sidebar-btn" data-module="monitoring" onclick="setModule('monitoring',this)">MT5 Monitoring</button>
    <button class="sidebar-btn" data-module="trades" onclick="setModule('trades',this)">Trader Trades</button>
    <button class="sidebar-btn" data-module="support" onclick="setModule('support',this)">Support Tickets</button>
    <button class="sidebar-btn" data-module="referrals" onclick="setModule('referrals',this)">Referrals</button>
    <button class="sidebar-btn" data-module="competitions" onclick="setModule('competitions',this)">Competitions</button>
    <button class="sidebar-btn" data-module="announcements" onclick="setModule('announcements',this)">Announcements</button>
    <button class="sidebar-btn" data-module="staff" onclick="setModule('staff',this)">Staff</button>
    <button class="sidebar-btn" data-module="accounts" onclick="setModule('accounts',this)">Account Management</button>
  </div>
</aside>

<main class="p-6 lg:p-8">
  <div class="flex flex-wrap justify-between items-center gap-4 mb-8">
    <div>
      <h2 id="pageTitle" class="text-4xl font-black">Dashboard Overview</h2>
      <p class="text-gray-400">Black-gold capital command center for NairaPips operations.</p>
    </div>

    <div class="flex gap-3">
      <input id="searchBox" oninput="render()" placeholder="Search trader / status / plan..." class="max-w-xs">
      <a href="#" onclick="this.href=nairaPipsWhatsAppUrl()" target="_blank" rel="noopener" class="btn btn-dark">Chat with NairaPips</a>
      <button onclick="loadData()" class="btn btn-gold">Refresh</button>
    </div>
  </div>

  <div id="globalReferralBar" class="mb-6"></div>
  <div id="content"></div>
</main>

</div>
</div>

<script>
const API_URL = "https://nairapips-api.onrender.com";
const WHATSAPP_NUMBER = "2348184035363";
const WHATSAPP_MESSAGE = "Hello NairaPips, I need help from the admin dashboard.";

function normalizeNairaPhoneDigits(phone){
  let d = String(phone || "").replace(/[^0-9]/g, "");
  if(!d) return "";
  if(d.startsWith("00")) d = d.slice(2);
  if(d.startsWith("234") && d.length >= 13) return d;
  if(d.startsWith("0") && d.length >= 11) return "234" + d.slice(1);
  if(d.length === 10 && /^[789]/.test(d)) return "234" + d;
  return d;
}

function makeWhatsAppUrl(phone, message){
  const n = normalizeNairaPhoneDigits(phone || WHATSAPP_NUMBER);
  const msg = encodeURIComponent(message || WHATSAPP_MESSAGE);
  return n ? `https://wa.me/${n}?text=${msg}` : "";
}

function nairaPipsWhatsAppUrl(){
  return makeWhatsAppUrl(WHATSAPP_NUMBER, WHATSAPP_MESSAGE);
}

let traders = [];
let payouts = [];
let tickets = [];
let announcements = [];
let plans = [];
let purchases = [];
let mt5pool = [];
let traderTrades = [];
let marketingDeletedIdCache = [];
let referralSettingsCache = {};
let businessSettingsCache = {};
let staffMembers = [];
let auditLogs = [];
let currentAdmin = {username:"superadmin", role:"super_admin", name:"Super Admin", permissions:"all"};
let staffPermissionDraft = {};
let staffCreatePermissionDraft = {};
let currentModule = "overview";
let isLoading = false;

async function adminLogin(){
  const u = document.getElementById("adminUser").value.trim();
  const p = document.getElementById("adminPass").value;

  if(u === "admin" && p === "nairapips123"){
    currentAdmin = {username:"admin", name:"Super Admin", role:"super_admin", permissions:"all"};
    document.getElementById("loginScreen").classList.add("hidden");
    document.getElementById("app").classList.remove("hidden");
    loadData();
    return;
  }

  try{
    const data = await postJSON(`${API_URL}/staff_login`, {username:u, password:p});
    currentAdmin = data.staff || data.data || {username:u, role:"support", permissions:{}};
    document.getElementById("loginScreen").classList.add("hidden");
    document.getElementById("app").classList.remove("hidden");
    loadData();
  }catch(e){
    document.getElementById("loginError").innerText = "Invalid login or staff account not active";
    document.getElementById("loginError").classList.remove("hidden");
  }
}

function money(n){return "₦" + Number(n||0).toLocaleString()}
function pct(n){return Number(n||0).toFixed(2)+"%"}
function q(){return (document.getElementById("searchBox")?.value||"").toLowerCase()}

const EXNESS_SERVERS = [
  "Exness-MT5Trial1","Exness-MT5Trial2","Exness-MT5Trial3","Exness-MT5Trial4","Exness-MT5Trial5",
  "Exness-MT5Trial6","Exness-MT5Trial7","Exness-MT5Trial8","Exness-MT5Trial9","Exness-MT5Trial10",
  "Exness-MT5Trial11","Exness-MT5Trial12","Exness-MT5Trial13","Exness-MT5Trial14","Exness-MT5Trial15",
  "Exness-MT5Trial16","Exness-MT5Trial17","Exness-MT5Trial18","Exness-MT5Trial19","Exness-MT5Trial20"
];

function serverOptions(selected=""){
  const current = String(selected || "").trim();
  const servers = [...EXNESS_SERVERS];
  if(current && !servers.includes(current)) servers.unshift(current);
  return `<option value="">Select Exness server</option>` + servers.map(s=>`<option value="${s}" ${s===current ? "selected" : ""}>${s}</option>`).join("");
}

function getPlanServer(p){
  return p?.mt5_server || p?.default_server || p?.server || "";
}

function formatPlanServer(p){
  return getPlanServer(p) || "Choose in MT5 Pool";
}

function formatPlanLabel(p){
  const srv = getPlanServer(p);
  return `${p.name || "Challenge Plan"} • ${money(p.account_size)}${srv ? " • " + srv : ""}`;
}

function formatServerValue(value){
  return String(value || "").trim();
}

function formatServerOption(value){
  return escapeHtml ? escapeHtml(value) : String(value || "");
}

function formatDate(value){
  if(!value) return "Not available yet";
  const d = new Date(value);
  if(isNaN(d.getTime())) return "Not available yet";
  return d.toLocaleString("en-GB",{day:"2-digit",month:"short",year:"numeric",hour:"2-digit",minute:"2-digit"});
}

function renderLoading(msg="Loading data..."){
  document.getElementById("content").innerHTML = `
  <div class="vault p-10 rounded-3xl flex items-center gap-5">
    <div class="loader"></div>
    <div>
      <h3 class="text-2xl font-black gold">${msg}</h3>
      <p class="text-gray-400">Loading live data from Render and Supabase.</p>
    </div>
  </div>`;
}

async function getJSON(url){
  try{
    const res = await fetch(url);
    const data = await res.json();
    return Array.isArray(data) ? data : [];
  }catch(e){ return []; }
}

async function getObject(url, fallback={}){
  try{
    const res = await fetch(url);
    const data = await res.json();
    return data && typeof data === "object" && !Array.isArray(data) ? data : fallback;
  }catch(e){ return fallback; }
}

async function postJSON(url, payload={}){
  const res = await fetch(url,{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify(payload)
  });
  const data = await res.json().catch(()=>({success:false,error:"Invalid server response"}));
  if(!res.ok || data.success === false){
    throw new Error(data.error || `Request failed: ${res.status}`);
  }
  return data;
}

async function loadData(){
  isLoading = true;
  renderLoading("Opening Master Command Center...");

  try{
    const [
      traderData,payoutData,ticketData,announcementData,planData,purchaseData,mt5Data,traderTradesData,marketingDeletedData,referralSettingsData,businessSettingsData,staffData,auditData
    ] = await Promise.all([
      getJSON(`${API_URL}/traders`),
      getJSON(`${API_URL}/payouts`),
      getJSON(`${API_URL}/support_tickets`),
      getJSON(`${API_URL}/announcements`),
      getJSON(`${API_URL}/challenge_plans`),
      getJSON(`${API_URL}/challenge_purchases`),
      getJSON(`${API_URL}/mt5_pool`),
      getJSON(`${API_URL}/trader_trades`),
      getJSON(`${API_URL}/marketing_deleted_contacts`),
      getObject(`${API_URL}/referral_settings`, {}),
      getObject(`${API_URL}/business_settings`, {}),
      getJSON(`${API_URL}/staff_members`),
      getJSON(`${API_URL}/audit_logs`)
    ]);

    traders = traderData;
    payouts = payoutData;
    tickets = ticketData;
    announcements = announcementData;
    plans = planData;
    purchases = purchaseData;
    mt5pool = mt5Data;
    traderTrades = traderTradesData;
    marketingDeletedIdCache = marketingDeletedData.map(x => String(x.contact_id || x.id || x));
    referralSettingsCache = referralSettingsData.data || referralSettingsData || {};
    businessSettingsCache = businessSettingsData.data || businessSettingsData || {};
    applyBusinessSettingsFromServer();
    staffMembers = staffData || [];
    auditLogs = auditData || [];

  }catch(e){
    document.getElementById("content").innerHTML = `
    <div class="card p-8 rounded-3xl">
      <h3 class="text-2xl font-black text-red-400">Could not load command center</h3>
      <p class="text-gray-400">Check API or wait for Render to wake up.</p>
      <button onclick="loadData()" class="btn btn-gold mt-5">Try Again</button>
    </div>`;
    isLoading = false;
    return;
  }

  isLoading = false;
  applyStaffPermissions();
  render();
}

function setModule(module,btn){
  if(!canViewModule(module)){
    alert("Access denied. This staff account cannot open this module.");
    return;
  }
  currentModule = module;
  document.querySelectorAll(".sidebar-btn").forEach(b=>b.classList.remove("active"));
  if(btn) btn.classList.add("active");
  document.getElementById("pageTitle").innerText = btn ? btn.innerText : module;
  render();
}


const STAFF_MODULES = [
  ["overview","Dashboard Overview"], ["payments","Payments"], ["traders","Users / Traders"],
  ["addTrader","Add Trader"], ["bulkTrader","Bulk Add Traders"], ["timeline","Timeline Intelligence"],
  ["plans","Challenge Plans"], ["purchases","Challenge Purchases"], ["mt5pool","MT5 Pool Vault"],
  ["payouts","Payouts"], ["funded","Funded Traders"], ["revenue","Revenue"],
  ["database","Users Database"], ["leads","Lead Follow-Up"], ["monitoring","MT5 Monitoring"], ["trades","Trader Trades"],
  ["support","Support Tickets"], ["referrals","Referrals"], ["competitions","Competitions"],
  ["announcements","Announcements"], ["staff","Staff"], ["accounts","Account Management"]
];

const STAFF_ACTIONS = ["view","create","edit","delete","approve","export","reveal_passwords"];

function roleTemplate(role){
  const all = {};
  const grant = (mods, actions=["view"]) => mods.forEach(m => all[m] = Object.fromEntries(actions.map(a=>[a,true])));
  if(role === "super_admin"){
    STAFF_MODULES.forEach(([m]) => all[m] = Object.fromEntries(STAFF_ACTIONS.map(a=>[a,true])));
  }else if(role === "admin_manager"){
    grant(["overview","payments","traders","addTrader","bulkTrader","purchases","mt5pool","funded","support","announcements","accounts"],["view","create","edit","approve"]);
    grant(["payouts","monitoring","trades","revenue"],["view"]);
  }else if(role === "finance"){
    grant(["overview","payments","payouts","purchases","revenue"],["view","approve","edit"]);
    grant(["traders"],["view"]);
  }else if(role === "support"){
    grant(["overview","traders","support","announcements","database","leads"],["view","create","edit"]);
  }else if(role === "marketing"){
    grant(["overview","database","leads","referrals","announcements","revenue"],["view","create","edit","export"]);
  }
  return all;
}

function emptyPermissions(){
  const perms = {};
  STAFF_MODULES.forEach(([m])=>{
    perms[m] = {};
    STAFF_ACTIONS.forEach(a=>perms[m][a]=false);
  });
  return perms;
}

function fullPermissions(){
  const perms = {};
  STAFF_MODULES.forEach(([m])=>{
    perms[m] = {};
    STAFF_ACTIONS.forEach(a=>perms[m][a]=true);
  });
  return perms;
}

function normalizePermissions(p, role="support"){
  if(p === "all") return "all";
  if(!p || typeof p !== "object" || Array.isArray(p)) return role === "super_admin" ? fullPermissions() : emptyPermissions();
  return p;
}

function canViewModule(module){
  if(!currentAdmin || currentAdmin.role === "super_admin" || currentAdmin.permissions === "all") return true;
  const perms = normalizePermissions(currentAdmin.permissions, currentAdmin.role);
  return !!(perms[module] && perms[module].view);
}

function canDo(module, action="view"){
  if(!currentAdmin || currentAdmin.role === "super_admin" || currentAdmin.permissions === "all") return true;
  const perms = normalizePermissions(currentAdmin.permissions, currentAdmin.role);
  return !!(perms[module] && perms[module][action]);
}

function applyStaffPermissions(){
  document.querySelectorAll(".sidebar-btn[data-module]").forEach(btn=>{
    const module = btn.getAttribute("data-module");
    btn.style.display = canViewModule(module) ? "block" : "none";
  });
  if(!canViewModule(currentModule)){
    const first = document.querySelector(".sidebar-btn[data-module]:not([style*='display: none'])");
    currentModule = first ? first.getAttribute("data-module") : "overview";
  }
}

function guarded(module, action, fn){
  if(!canDo(module, action)){
    alert(`Access denied. You do not have ${action} permission for this module.`);
    return;
  }
  return fn();
}

function render(){
  if(isLoading) return;
  if(document.getElementById("globalReferralBar")){
    document.getElementById("globalReferralBar").innerHTML = referralGlobalBar();
  }

  if(currentModule==="overview") return overview();
  if(currentModule==="payments") return paymentsModule();
  if(currentModule==="traders") return tradersModule();
  if(currentModule==="addTrader") return addTraderModule();
  if(currentModule==="bulkTrader") return bulkTraderModule();
  if(currentModule==="timeline") return timelineModule();
  if(currentModule==="plans") return plansModule();
  if(currentModule==="purchases") return purchasesModule();
  if(currentModule==="mt5pool") return mt5PoolModule();
  if(currentModule==="monitoring") return monitoringModule();
  if(currentModule==="payouts") return payoutsModule();
  if(currentModule==="revenue") return revenueModule();
  if(currentModule==="database") return databaseModule();
  if(currentModule==="leads") return leadsModule();
  if(currentModule==="trades") return traderTradesModule();
  if(currentModule==="funded") return fundedModule();
  if(currentModule==="support") return supportModule();
  if(currentModule==="referrals") return referralsModule();
  if(currentModule==="competitions") return placeholder("Competitions","Trading contests and leaderboard campaigns will connect later.");
  if(currentModule==="announcements") return announcementsModule();
  if(currentModule==="staff") return staffModule();
  if(currentModule==="accounts") return accountsModule();
}

function stat(label,value,extra=""){
  return `<div class="vault p-5 rounded-2xl">
    <p class="text-gray-400 text-sm">${label}</p>
    <h3 class="text-3xl font-black gold">${value}</h3>
    <p class="text-gray-500 text-xs mt-1">${extra}</p>
  </div>`;
}

function empty(msg){
  return `<div class="card p-8 rounded-3xl text-center text-gray-400">${msg}</div>`;
}

function filteredTraders(){
  const s = q();
  return traders.filter(t =>
    (t.name||"").toLowerCase().includes(s) ||
    (t.email||"").toLowerCase().includes(s) ||
    (t.phone||"").toLowerCase().includes(s) ||
    (t.mt5_login||"").toLowerCase().includes(s) ||
    (t.account_reference||"").toLowerCase().includes(s)
  );
}

function tradeStatusBadge(status){
  const s = String(status||"open").toLowerCase();
  const cls = s === "open" ? "text-green-400" : s === "closed" ? "text-gray-400" : "text-yellow-400";
  return `<span class="badge ${cls}">${s.toUpperCase()}</span>`;
}

function tradeProfitClass(v){
  const n = Number(v||0);
  return n >= 0 ? "text-green-400" : "text-red-400";
}

function tradeTypeBadge(v){
  const t = String(v||"-").toUpperCase();
  const cls = t === "BUY" ? "text-green-400" : t === "SELL" ? "text-red-400" : "text-gray-300";
  return `<span class="badge ${cls}">${t}</span>`;
}

async function traderTradesModule(){
  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">LIVE MT5 TRADE SURVEILLANCE</span>
        <h3 class="text-4xl font-black gold mt-3">Trader Trades</h3>
        <p class="text-gray-400 mt-2">Live open positions synced from the MT5 rotation engine into NairaPips command center.</p>
      </div>
      <div class="flex gap-3 items-start">
        <button onclick="traderTradesModule()" class="btn btn-gold">Refresh Trades</button>
        <button onclick="loadData()" class="btn btn-dark">Reload All</button>
      </div>
    </div>
  </div>
  <div class="vault p-10 rounded-3xl flex items-center gap-5">
    <div class="loader"></div>
    <div>
      <h3 class="text-2xl font-black gold">Loading live trades...</h3>
      <p class="text-gray-400">Reading /trader_trades from NairaPips API.</p>
    </div>
  </div>`;

  try{
    traderTrades = await getJSON(`${API_URL}/trader_trades`);
  }catch(e){
    traderTrades = [];
  }

  const s = q();
  const list = traderTrades.filter(t =>
    (t.trader_name||"").toLowerCase().includes(s) ||
    (t.email||"").toLowerCase().includes(s) ||
    String(t.mt5_login||"").toLowerCase().includes(s) ||
    (t.symbol||"").toLowerCase().includes(s) ||
    (t.trade_type||"").toLowerCase().includes(s) ||
    (t.status||"").toLowerCase().includes(s)
  );

  const open = traderTrades.filter(t=>String(t.status||"open").toLowerCase()==="open").length;
  const closed = traderTrades.filter(t=>String(t.status||"").toLowerCase()==="closed").length;
  const totalProfit = traderTrades.reduce((a,t)=>a+Number(t.profit||0),0);
  const totalLots = traderTrades.reduce((a,t)=>a+Number(t.volume||0),0);
  const uniqueTraders = new Set(traderTrades.map(t=>String(t.mt5_login||"")).filter(Boolean)).size;

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">LIVE MT5 TRADE SURVEILLANCE</span>
        <h3 class="text-4xl font-black gold mt-3">Trader Trades</h3>
        <p class="text-gray-400 mt-2">Open trades, symbols, lot sizes, floating P/L and trade activity synced from MT5.</p>
      </div>
      <button onclick="traderTradesModule()" class="btn btn-gold">Refresh Trades</button>
    </div>
  </div>

  <div class="grid md:grid-cols-5 gap-4 mb-8">
    ${stat("Total Synced Trades",traderTrades.length,"Latest trade records")}
    ${stat("Open Trades",open,"Currently running")}
    ${stat("Closed Records",closed,"History records")}
    ${stat("Total Lots",Number(totalLots||0).toFixed(2),"Exposure size")}
    ${stat("Floating P/L",money(totalProfit),"Live trade profit")}
  </div>

  <div class="vault p-6 rounded-3xl">
    <div class="flex flex-wrap justify-between items-center gap-4 mb-5">
      <div>
        <h3 class="text-3xl font-black gold">Live Trade Feed</h3>
        <p class="text-gray-400">${uniqueTraders} MT5 login(s) represented. Auto-refresh this module every time you click Refresh Trades.</p>
      </div>
      <span class="badge">${formatDate(new Date().toISOString())}</span>
    </div>

    <div class="tableWrap">
      <table>
        <thead>
          <tr>
            <th>Status</th>
            <th>Trader</th>
            <th>Email</th>
            <th>MT5 Login</th>
            <th>Symbol</th>
            <th>Type</th>
            <th>Lot</th>
            <th>Open Price</th>
            <th>Current Price</th>
            <th>SL</th>
            <th>TP</th>
            <th>Profit</th>
            <th>Opened</th>
            <th>Synced</th>
          </tr>
        </thead>
        <tbody>
          ${list.map(t=>`
            <tr>
              <td>${tradeStatusBadge(t.status)}</td>
              <td><b>${t.trader_name||"Unknown"}</b></td>
              <td class="text-gray-400">${t.email||"-"}</td>
              <td class="gold font-bold">${t.mt5_login||"-"}</td>
              <td><b>${t.symbol||"-"}</b></td>
              <td>${tradeTypeBadge(t.trade_type)}</td>
              <td>${Number(t.volume||0).toFixed(2)}</td>
              <td>${Number(t.open_price||0)}</td>
              <td>${Number(t.current_price||0)}</td>
              <td>${Number(t.sl||0)}</td>
              <td>${Number(t.tp||0)}</td>
              <td class="font-black ${tradeProfitClass(t.profit)}">${money(t.profit)}</td>
              <td class="text-gray-400">${formatDate(t.opened_at)}</td>
              <td class="text-gray-400">${formatDate(t.synced_at)}</td>
            </tr>
          `).join("") || `<tr><td colspan="14" class="text-center text-gray-400 py-10">No trade records yet. Keep the MT5 rotation monitor running and open a test trade.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>`;
}


function leadPhoneClean(v){
  return normalizeNairaPhoneDigits(v);
}

function leadFirstName(t){
  return String(t?.name || "Legend").trim().split(/\s+/)[0] || "Legend";
}

function leadWhatsAppMessage(t){
  return `Hello ${leadFirstName(t)}, welcome to NairaPips. I noticed you created an account but have not started a challenge yet. Do you want me to guide you to pick the right account size and start today?`;
}

function leadWhatsAppUrl(t){
  const phone = leadPhoneClean(t.phone || t.whatsapp || t.whatsapp_number || "");
  return phone ? makeWhatsAppUrl(phone, leadWhatsAppMessage(t)) : "";
}
function leadMailUrl(t){
  const subject=encodeURIComponent("Your NairaPips account is ready");
  const body=encodeURIComponent(`Hello ${t.name||"Legend"},\n\nYour NairaPips trader dashboard is ready. The next step is to choose a challenge plan and upload proof of payment so we can activate your MT5 account.\n\nNairaPips Team`);
  return `mailto:${t.email||""}?subject=${subject}&body=${body}`;
}
function isUnconvertedLead(t){
  const s=String(t.status||"").toLowerCase(), p=String(t.payment_status||"").toLowerCase(), hasMt5=String(t.mt5_login||"").trim();
  return !hasMt5 && (["new_signup","registered","lead"].includes(s)||["","none","no_payment","null"].includes(p)||!p);
}
function leadAgeHours(t){const d=new Date(t.created_at||t.joined_at||t.approved_at||Date.now()).getTime();return Math.max(0,Math.round((Date.now()-d)/36e5))}
async function markLeadContacted(id){
  try{await postJSON(`${API_URL}/update_trader`,{id,admin_note:"Lead contacted by admin follow-up desk"});}catch(e){}
  alert("Marked as contacted. WhatsApp/email follow-up can continue manually.");
  loadData();
}
function leadsModule(){
  const leads=traders.filter(isUnconvertedLead).sort((a,b)=>new Date(b.created_at||0)-new Date(a.created_at||0));
  const hot=leads.filter(t=>leadAgeHours(t)>=1).length, withPhone=leads.filter(t=>String(t.phone||"").trim()).length;
  document.getElementById("content").innerHTML=`
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div><span class="badge">LEAD FOLLOW-UP ENGINE</span><h3 class="text-4xl font-black gold mt-3">New Signups To Chase</h3><p class="text-gray-400 mt-2">Every signup that has not bought a challenge appears here so admin can follow up immediately on WhatsApp.</p></div>
      <button onclick="loadData()" class="btn btn-gold">Refresh Leads</button>
    </div>
  </div>
  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Unconverted Leads",leads.length,"Signed up but not paid")}
    ${stat("With Phone",withPhone,"Can WhatsApp now")}
    ${stat("Hot Leads",hot,"Waiting over 1 hour")}
    ${stat("Follow-Up Rule","ASAP","Do not let leads go cold")}
  </div>
  <div class="grid xl:grid-cols-2 gap-5">
  ${leads.map(t=>{
    const wa=leadWhatsAppUrl(t), age=leadAgeHours(t);
    return `<div class="${age>=1?'lead-hot':'lead-warm'} rounded-3xl p-5">
      <div class="flex flex-wrap justify-between gap-3 mb-4"><div><h3 class="text-2xl font-black">${t.name||"Unnamed Lead"}</h3><p class="text-gray-400">${t.email||"No email"} • ${t.phone||"No phone"}</p></div><span class="badge">${age}h old</span></div>
      <div class="grid md:grid-cols-3 gap-3 mb-4"><div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Status</p><b class="gold">${t.status||"new_signup"}</b></div><div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Payment</p><b class="gold">${t.payment_status||"none"}</b></div><div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Lead Stage</p><b class="gold">${leadStatusValue(t)}</b></div></div>
      <div class="flex flex-wrap gap-2 mb-4">
        ${["Contacted","Interested","Not Interested","Follow Up Tomorrow","Converted"].map(status=>`<button class="btn btn-dark" onclick="setLeadStatus('${t.id}','${status}')">${status}</button>`).join("")}
      </div>
      <div class="lead-action-grid">
        ${wa?`<a href="${wa}" target="_blank" class="btn btn-gold text-center">Chat on WhatsApp</a>`:`<button class="btn btn-dark" disabled>No Phone</button>`}
        ${t.email?`<a href="${leadMailUrl(t)}" class="btn btn-dark text-center">Send Email</a>`:`<button class="btn btn-dark" disabled>No Email</button>`}
        <button onclick="markLeadContacted('${t.id}')" class="btn btn-green">Mark Contacted</button>
      </div>
    </div>`;
  }).join("")||empty("No unconverted leads right now.")}
  </div>`;
}

function overview(){
  const oldSetModule = setModule;
  const pendingPayments = traders.filter(t=>t.payment_status==="pending" || t.status==="payment_pending").length;
  const active = traders.filter(t=>t.status==="active").length;
  const funded = traders.filter(t=>t.status==="funded" || t.phase==="funded").length;
  const pendingPayouts = payouts.filter(p=>p.status==="pending").length;
  const openTickets = tickets.filter(t=>t.status==="open").length;
  const pendingPurchases = purchases.filter(p=>p.status==="pending_review" || p.payment_status==="pending").length;
  const availableMT5 = mt5pool.filter(m=>m.status==="available").length;

  document.getElementById("content").innerHTML = `
  <div class="grid md:grid-cols-3 xl:grid-cols-8 gap-4 mb-8">
    ${stat("Total Traders",traders.length,"All records")}
    ${stat("Active Traders",active,"Currently live")}
    ${stat("Funded / Live",funded,"Advanced accounts")}
    ${stat("Plans",plans.length,"Challenge offers")}
    ${stat("Pending Purchases",pendingPurchases,"Needs approval")}
    ${stat("MT5 Available",availableMT5,"Vault stock")}
    ${stat("Pending Payouts",pendingPayouts,"Needs action")}
    ${stat("Open Tickets",openTickets,"Support")}
  </div>

  <div class="lead-hot rounded-3xl p-6 mb-8">
    <div class="flex flex-wrap justify-between gap-4 items-center">
      <div><span class="badge">FOLLOW-UP ALERT</span><h3 class="text-3xl font-black gold mt-2">New Signup Follow-Up</h3><p class="text-gray-300">Do not let registered traders go cold. Open Lead Follow-Up and chat them on WhatsApp.</p></div>
      <button onclick="setModule('leads',document.querySelector('[data-module=leads]'))" class="btn btn-gold">Open Lead Follow-Up</button>
    </div>
  </div>
  <div class="grid lg:grid-cols-3 gap-6">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-bold gold mb-5">Challenge Purchase Queue</h3>
      ${purchases.filter(p=>p.status==="pending_review" || p.payment_status==="pending").slice(0,5).map(p=>`
        <div class="border-b border-white/10 py-3">
          <b>${p.trader_name||"Trader"}</b>
          <p class="text-gray-400">${p.plan_name||""} • ${money(p.fee)}</p>
          <small class="text-gray-500">${formatDate(p.created_at)}</small>
        </div>`).join("") || `<p class="text-gray-400">No pending challenge purchase.</p>`}
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-bold gold mb-5">MT5 Vault Pulse</h3>
      ${stat("Available",availableMT5,"Ready for assignment")}
      <div class="mt-4">${stat("Assigned",mt5pool.filter(m=>m.status==="assigned").length,"Already given out")}</div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-bold gold mb-5">Capital Alerts</h3>
      <p class="text-gray-400">Pending Payments: <b class="gold">${pendingPayments}</b></p>
      <p class="text-gray-400 mt-3">Pending Payouts: <b class="gold">${pendingPayouts}</b></p>
      <p class="text-gray-400 mt-3">Support Tickets: <b class="gold">${openTickets}</b></p>
      <p class="text-gray-400 mt-3">Active Announcements: <b class="gold">${announcements.length}</b></p>
    </div>
  </div>`;
}

/* PLANS */
function plansModule(){
  const s = q();
  const list = (plans || []).filter(p =>
    (p.name||"").toLowerCase().includes(s) ||
    String(p.account_size||"").includes(s) ||
    String(p.fee||"").includes(s) ||
    (p.description||"").toLowerCase().includes(s)
  );

  const totalPlans = plans.length;
  const totalCapital = plans.reduce((a,p)=>a+Number(p.account_size||0),0);
  const lowestFee = plans.length ? Math.min(...plans.map(p=>Number(p.fee||0)).filter(n=>!isNaN(n))) : 0;
  const highestCapital = plans.length ? Math.max(...plans.map(p=>Number(p.account_size||0)).filter(n=>!isNaN(n))) : 0;

  document.getElementById("content").innerHTML = `
  <div class="plan-hero p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5 items-start">
      <div>
        <span class="badge">NAIRAPIPS CHALLENGE SHOP</span>
        <h3 class="text-5xl font-black gold mt-3">Challenge Plans</h3>
        <p class="text-gray-400 mt-3 max-w-3xl">Create, price and control the plans traders see before buying. Bigger cards, cleaner rule display, and premium black-gold presentation.</p>
      </div>
      <div class="grid grid-cols-2 gap-3 min-w-[280px]">
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Plans Live</p><h2 class="text-3xl font-black gold">${totalPlans}</h2></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Top Capital</p><h2 class="text-2xl font-black gold">${money(highestCapital)}</h2></div>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Total Plans", totalPlans, "Challenge offers")}
    ${stat("Capital Display", money(totalCapital), "Combined account sizes")}
    ${stat("Lowest Fee", money(lowestFee || 0), "Entry point")}
    ${stat("Default Max DD", "20%", "No daily drawdown focus")}
  </div>

  <div class="grid xl:grid-cols-[440px_1fr] gap-6 mb-8">
    <div class="vault p-6 rounded-3xl h-fit">
      <div class="mb-5">
        <span class="badge">CREATE OFFER</span>
        <h3 class="text-3xl font-black gold mt-3">New Challenge Plan</h3>
        <p class="text-gray-400 mt-2">Use clean pricing, account size and rules. This section now looks like a real fintech plan builder.</p>
      </div>

      <label class="text-gray-400 text-sm">Plan Name</label>
      <input id="plan_name" placeholder="e.g Starter Legend" class="mb-3">
      <label class="text-gray-400 text-sm">Default Exness Server for this Plan</label>
      <select id="plan_server" class="mb-3">${serverOptions("Exness-MT5Trial9")}</select>
      <div class="grid grid-cols-2 gap-3">
        <div><label class="text-gray-400 text-sm">Account Size</label><input id="plan_size" placeholder="500000" class="mb-3"></div>
        <div><label class="text-gray-400 text-sm">Challenge Fee</label><input id="plan_fee" placeholder="25000" class="mb-3"></div>
      </div>

      <div class="grid grid-cols-3 gap-3 mb-3">
        <div><label class="text-gray-400 text-sm">Phase 1</label><input id="plan_phase1" placeholder="10" value="10"></div>
        <div><label class="text-gray-400 text-sm">Phase 2</label><input id="plan_phase2" placeholder="8" value="8"></div>
        <div><label class="text-gray-400 text-sm">Max DD</label><input id="plan_maxdd" placeholder="20" value="20"></div>
      </div>

      <div class="grid grid-cols-2 gap-3">
        <div><label class="text-gray-400 text-sm">Daily Drawdown</label><input id="plan_dailydd" placeholder="None" value="None" class="mb-3"></div>
        <div><label class="text-gray-400 text-sm">Payout Split</label><input id="plan_payout" placeholder="80%" value="80%" class="mb-3"></div>
      </div>
      <label class="text-gray-400 text-sm">Sales Description</label>
      <textarea id="plan_desc" rows="5" placeholder="Short powerful description for this challenge..." class="mb-4"></textarea>
      <button onclick="createPlan()" class="btn btn-gold w-full text-lg py-4">Create Challenge Plan</button>
    </div>

    <div>
      <div class="vault p-5 rounded-3xl mb-5 flex flex-wrap justify-between gap-4 items-center">
        <div>
          <h3 class="text-3xl font-black gold">Live Plan Cards</h3>
          <p class="text-gray-400">Bigger, clearer and more premium than the old small cards.</p>
        </div>
        <span class="badge">${list.length} visible</span>
      </div>
      <div class="grid 2xl:grid-cols-2 gap-6">
        ${list.map(planCard).join("") || empty("No challenge plans created yet.")}
      </div>
    </div>
  </div>`;
}

function planCard(p){
  const fee = Number(p.fee||0);
  const size = Number(p.account_size||0);
  const phase1 = p.phase1_target || 10;
  const phase2 = p.phase2_target || 8;
  const maxdd = p.max_drawdown || 20;
  const payout = p.payout_split || "80%";
  const daily = p.daily_drawdown || "None";
  return `
  <div class="plan-card p-7 rounded-3xl">
    <div class="relative z-[1]">
      <div class="flex flex-wrap justify-between gap-4 mb-6">
        <div>
          <span class="plan-pill">ACTIVE CHALLENGE</span>
          <h3 class="text-4xl font-black gold mt-4">${escapeHtml(p.name||"Challenge Plan")}</h3>
          <p class="text-gray-400 mt-3 max-w-xl">${escapeHtml(p.description||"Built for disciplined traders seeking structured capital through NairaPips.")}</p>
        </div>
        <div class="text-right">
          <p class="text-gray-500 text-sm">Challenge Fee</p>
          <div class="plan-price">${money(fee)}</div>
        </div>
      </div>

      <div class="card2 p-5 rounded-3xl mb-5 border border-yellow-900/30">
        <p class="text-gray-500 text-sm">Account Access</p>
        <div class="plan-size">${money(size)}</div>
        <p class="text-gray-400 mt-2">Capital size shown to traders for this challenge plan.</p>
      </div>

      <div class="grid md:grid-cols-3 gap-4 mb-5">
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Phase 1 Target</p><b class="text-2xl gold">${phase1}%</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Phase 2 Target</p><b class="text-2xl gold">${phase2}%</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Max Drawdown</p><b class="text-2xl text-red-400">${maxdd}%</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Daily Drawdown</p><b class="text-xl">${escapeHtml(daily)}</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Payout Split</p><b class="text-xl text-green-400">${escapeHtml(payout)}</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Default Server</p><b class="text-sm gold break-words">${escapeHtml(formatPlanServer(p))}</b></div>
        <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Updated</p><b class="text-sm">${formatDate(p.updated_at || p.created_at)}</b></div>
      </div>

      <div class="flex flex-wrap gap-3">
        <button onclick="editPlan('${escapeQuotes(p.id)}')" class="btn btn-gold">Edit Plan</button>
        <button onclick="deletePlan('${escapeQuotes(p.id)}')" class="btn btn-red">Delete Plan</button>
      </div>
    </div>
  </div>`;
}

async function createPlan(){
  if(!canDo("plans","create")){alert("Access denied: create plans");return;}
  const payload = {
    name:document.getElementById("plan_name").value,
    account_size:document.getElementById("plan_size").value,
    fee:document.getElementById("plan_fee").value,
    phase1_target:document.getElementById("plan_phase1").value,
    phase2_target:document.getElementById("plan_phase2").value,
    max_drawdown:document.getElementById("plan_maxdd").value,
    daily_drawdown:document.getElementById("plan_dailydd").value,
    payout_split:document.getElementById("plan_payout").value,
    mt5_server:document.getElementById("plan_server").value,
    default_server:document.getElementById("plan_server").value,
    description:document.getElementById("plan_desc").value,
    status:"active"
  };

  const res = await fetch(`${API_URL}/create_challenge_plan`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)
  });
  const data = await res.json();
  if(data.success){alert("Challenge plan created.");loadData();}
  else alert(data.error || "Failed to create plan");
}

async function editPlan(id){
  const p = plans.find(x=>String(x.id)===String(id));
  if(!p) return;

  const name = prompt("Plan name:", p.name || "");
  if(name === null) return;
  const account_size = prompt("Account size:", p.account_size || "");
  if(account_size === null) return;
  const fee = prompt("Challenge fee:", p.fee || "");
  if(fee === null) return;
  const phase1_target = prompt("Phase 1 target:", p.phase1_target || 10);
  if(phase1_target === null) return;
  const phase2_target = prompt("Phase 2 target:", p.phase2_target || 8);
  if(phase2_target === null) return;
  const max_drawdown = prompt("Max drawdown:", p.max_drawdown || 20);
  if(max_drawdown === null) return;
  const daily_drawdown = prompt("Daily drawdown:", p.daily_drawdown || "None");
  if(daily_drawdown === null) return;
  const payout_split = prompt("Payout split:", p.payout_split || "80%");
  if(payout_split === null) return;
  const mt5_server = prompt("Default Exness server for this plan:", getPlanServer(p) || "Exness-MT5Trial9");
  if(mt5_server === null) return;
  const description = prompt("Description:", p.description || "");
  if(description === null) return;

  const res = await fetch(`${API_URL}/update_challenge_plan`,{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({id,name,account_size,fee,phase1_target,phase2_target,max_drawdown,daily_drawdown,payout_split,mt5_server,default_server:mt5_server,description,status:"active"})
  });

  const data = await res.json();
  if(data.success){alert("Plan updated.");loadData();}
  else alert(data.error || "Update failed");
}

async function deletePlan(id){
  if(!canDo("plans","delete")){alert("Access denied: delete plans");return;}
  if(!confirm("Delete this challenge plan permanently?")) return;
  const res = await fetch(`${API_URL}/delete_challenge_plan`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id})
  });
  const data = await res.json();
  if(data.success){alert("Plan deleted.");loadData();}
  else alert(data.error || "Delete failed");
}

/* PURCHASES */
function purchasesModule(){
  const s = q();

  const list = purchases.filter(p =>
    (p.trader_name||"").toLowerCase().includes(s) ||
    (p.email||"").toLowerCase().includes(s) ||
    (p.phone||"").toLowerCase().includes(s) ||
    (p.plan_name||"").toLowerCase().includes(s) ||
    (p.status||"").toLowerCase().includes(s) ||
    (p.payment_status||"").toLowerCase().includes(s)
  );

  const pending = purchases.filter(p=>p.payment_status==="pending" || p.status==="pending_review");
  const approved = purchases.filter(p=>p.payment_status==="approved");
  const rejected = purchases.filter(p=>p.payment_status==="rejected");
  const totalFee = purchases.reduce((a,p)=>a+Number(p.fee||0),0);
  const approvedFee = approved.reduce((a,p)=>a+Number(p.fee||0),0);

  const stockMap = {};
  mt5pool.filter(m=>m.status==="available").forEach(m=>{
    const key = Number(m.account_size||0);
    stockMap[key] = (stockMap[key]||0)+1;
  });

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">Challenge Purchase Command</span>
        <h3 class="text-4xl font-black gold mt-3">Approval Engine</h3>
        <p class="text-gray-400 mt-2">Review payment proofs, approve buyers, and assign MT5 accounts from the vault.</p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[220px]">
        <p class="text-gray-500 text-sm">Pending Queue</p>
        <h2 class="text-5xl font-black gold">${pending.length}</h2>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-4 xl:grid-cols-7 gap-4 mb-8">
    ${stat("Total Purchases",purchases.length,"All challenge buys")}
    ${stat("Pending",pending.length,"Needs review")}
    ${stat("Approved",approved.length,"Activated")}
    ${stat("Rejected",rejected.length,"Declined")}
    ${stat("Available MT5",mt5pool.filter(m=>m.status==="available").length,"Ready to assign")}
    ${stat("Total Fees",money(totalFee),"All submitted purchases")}
    ${stat("Approved Fees",money(approvedFee),"Confirmed revenue")}
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <h3 class="text-3xl font-black gold mb-4">MT5 Stock Warning Board</h3>
    <div class="grid md:grid-cols-2 xl:grid-cols-4 gap-4">
      ${
        Object.keys(stockMap).length
        ? Object.entries(stockMap).sort((a,b)=>Number(a[0])-Number(b[0])).map(([size,count])=>`
          <div class="card2 p-5 rounded-2xl">
            <p class="text-gray-500 text-sm">Account Size</p>
            <h3 class="text-3xl font-black gold">${money(size)}</h3>
            <p class="text-gray-400 mt-2">Available left: <b>${count}</b></p>
          </div>
        `).join("")
        : `<div class="card2 p-5 rounded-2xl text-red-400">No available MT5 account in vault.</div>`
      }
    </div>
  </div>

  <div class="grid gap-5">
    ${list.map(purchaseCard).join("") || empty("No challenge purchases yet.")}
  </div>`;
}

function purchaseCard(p){
  const available = mt5pool.filter(m =>
    m.status==="available" &&
    Number(m.account_size||0) === Number(p.account_size||0)
  );

  const isPending = (p.payment_status==="pending" || p.status==="pending_review");
  const isApproved = p.payment_status==="approved";
  const isRejected = p.payment_status==="rejected";
  const noStock = available.length === 0 && isPending;

  return `
  <div class="vault p-6 rounded-3xl ${noStock ? "danger" : ""}">
    <div class="flex flex-wrap justify-between gap-4 mb-6">
      <div>
        <span class="badge">${p.payment_status||"pending"}</span>
        <span class="badge ml-2">${p.status||"pending_review"}</span>
        ${noStock ? `<span class="badge ml-2 text-red-400">NO MATCHING MT5 STOCK</span>` : ""}
        <h3 class="text-3xl font-black gold mt-3">${p.plan_name||"Challenge Purchase"}</h3>
        <p class="text-gray-400">${p.trader_name||""} • ${p.email||""} • ${p.phone||""}</p>
      </div>

      <div class="text-right">
        <p class="text-gray-500 text-sm">Submitted</p>
        <b>${formatDate(p.created_at)}</b>
        <p class="text-gray-500 text-sm mt-1">${p.purchase_month||""} ${p.purchase_year||""}</p>
      </div>
    </div>

    <div class="grid md:grid-cols-5 gap-4 mb-6">
      <div class="card2 p-5 rounded-2xl md:col-span-2">
        <p class="text-gray-500 text-sm">Account Size</p>
        <h3 class="text-4xl font-black gold">${money(p.account_size)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Fee Paid</p>
        <h3 class="text-2xl font-black">${money(p.fee)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Matching MT5 Left</p>
        <h3 class="text-3xl font-black ${available.length ? "text-green-400" : "text-red-400"}">${available.length}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Assigned MT5</p>
        <h3 class="text-xl font-black">${p.mt5_login||"Not assigned"}</h3>
      </div>
    </div>

    <div class="grid md:grid-cols-4 gap-4 mb-6">
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Approved</p><b>${formatDate(p.approved_at)}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Assigned</p><b>${formatDate(p.assigned_at)}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Rejected</p><b>${formatDate(p.rejected_at)}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">Purchase ID</p><b class="text-xs">${p.id}</b></div>
    </div>

    <div class="grid lg:grid-cols-[1fr_1fr] gap-5 mb-6">
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm mb-2">Admin Note</p>
        <p>${p.admin_note || "No admin note yet"}</p>
      </div>

      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm mb-3">Payment Proof</p>
        <a class="btn btn-gold inline-block" href="${p.payment_proof_url || "#"}" target="_blank">Open Receipt / Evidence</a>
      </div>
    </div>

    ${
      isApproved
      ? `<div class="card2 p-5 rounded-2xl border border-green-900/40">
          <h3 class="text-2xl font-black text-green-400 mb-2">Already Approved</h3>
          <p class="text-gray-400">This purchase has been approved and an MT5 account has been assigned.</p>
        </div>`
      : isRejected
      ? `<div class="card2 p-5 rounded-2xl border border-red-900/40">
          <h3 class="text-2xl font-black text-red-400 mb-2">Rejected Purchase</h3>
          <p class="text-gray-400">This purchase was rejected. Trader will see the rejection status.</p>
        </div>`
      : `<div class="grid md:grid-cols-[1fr_auto_auto] gap-3">
          <select id="purchase-mt5-${p.id}">
            <option value="">Auto assign matching MT5 from vault</option>
            ${available.map(m=>`<option value="${m.id}">${m.mt5_login} • ${m.mt5_server} • ${money(m.account_size)}</option>`).join("")}
          </select>
          <button class="btn btn-green" onclick="approvePurchase('${p.id}')">Approve + Assign MT5</button>
          <button class="btn btn-red" onclick="rejectPurchase('${p.id}')">Reject</button>
        </div>
        ${noStock ? `<p class="text-red-400 mt-4 font-bold">Add a ${money(p.account_size)} MT5 account to the vault before approval.</p>` : ""}`
    }
  </div>`;
}

async function approvePurchase(id){
  const select = document.getElementById(`purchase-mt5-${id}`);
  const mt5_id = select ? select.value : "";
  const note = prompt("Admin approval note:", "Challenge approved. MT5 assigned from vault.") || "";
  const res = await fetch(`${API_URL}/approve_challenge_purchase`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,mt5_id,admin_note:note,approved_by:currentAdmin?.username||"admin",admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"})
  });
  const data = await res.json();
  if(data.success){await logAdminAudit("challenge_purchases","mt5_account_assignment",`Challenge purchase ${id} approved and assigned MT5 ${mt5_id||"auto"}`,id);alert("Challenge approved and MT5 assigned.");loadData();}
  else alert(data.error || "Approval failed");
}

async function rejectPurchase(id){
  const note = prompt("Reason for rejection:", "Payment proof rejected.") || "";
  const res = await fetch(`${API_URL}/reject_challenge_purchase`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,admin_note:note})
  });
  const data = await res.json();
  if(data.success){alert("Challenge purchase rejected.");loadData();}
  else alert(data.error || "Rejection failed");
}

/* MT5 */
function mt5PoolModule(){
  const s = q();

  const available = mt5pool.filter(m => (m.status||"available") === "available");
  const assigned = mt5pool.filter(m => (m.status||"") === "assigned");
  const inactive = mt5pool.filter(m => (m.status||"") === "inactive" || (m.status||"") === "expired" || (m.status||"") === "archived");

  const filteredAvailable = available.filter(m =>
    (m.mt5_login||"").toLowerCase().includes(s) ||
    (m.mt5_server||"").toLowerCase().includes(s) ||
    (m.plan_name||"").toLowerCase().includes(s) ||
    String(m.account_size||"").includes(s)
  );

  const filteredAssigned = assigned.filter(m =>
    (m.mt5_login||"").toLowerCase().includes(s) ||
    (m.mt5_server||"").toLowerCase().includes(s) ||
    (m.plan_name||"").toLowerCase().includes(s) ||
    (m.assigned_trader_name||"").toLowerCase().includes(s) ||
    (m.assigned_email||"").toLowerCase().includes(s) ||
    String(m.account_size||"").includes(s)
  );

  const inventoryMap = {};
  available.forEach(m=>{
    const size = Number(m.account_size||0);
    const key = size || 0;
    if(!inventoryMap[key]) inventoryMap[key] = {size, count:0, plans:{}};
    inventoryMap[key].count++;
    const plan = m.plan_name || "No Plan";
    inventoryMap[key].plans[plan] = (inventoryMap[key].plans[plan] || 0) + 1;
  });

  const inventory = Object.values(inventoryMap).sort((a,b)=>a.size-b.size);

  document.getElementById("content").innerHTML = `
  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Total MT5 Stored",mt5pool.length,"All vault records")}
    ${stat("Available",available.length,"Unused accounts")}
    ${stat("Assigned",assigned.length,"Given to traders")}
    ${stat("Expired / Archived",inactive.length,"Old or inactive")}
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-4 mb-6">
      <div>
        <span class="badge">Inventory Intelligence</span>
        <h3 class="text-3xl font-black gold mt-3">Remaining MT5 Stock by Account Size</h3>
        <p class="text-gray-400 mt-2">Only AVAILABLE accounts are counted here. Assigned accounts are removed from available stock automatically.</p>
      </div>
      <div class="card2 p-4 rounded-2xl">
        <p class="text-gray-500 text-sm">Unused Accounts Left</p>
        <h2 class="text-4xl font-black gold">${available.length}</h2>
      </div>
    </div>

    <div class="grid md:grid-cols-2 xl:grid-cols-4 gap-4">
      ${
        inventory.map(item=>`
          <div class="card2 mt5-stock-card p-5 rounded-2xl">
            <p class="text-gray-500 text-sm">Account Size</p>
            <h3 class="text-3xl font-black gold">${money(item.size)}</h3>
            <p class="text-gray-400 mt-2">Available Left: <b>${item.count}</b></p>
            <div class="mt-3 text-xs text-gray-500">
              ${
                Object.entries(item.plans).map(([plan,count])=>`
                  <div class="flex justify-between border-b border-white/5 py-1">
                    <span>${plan}</span>
                    <b>${count}</b>
                  </div>
                `).join("")
              }
            </div>
          </div>
        `).join("") || `<div class="card2 p-5 rounded-2xl text-gray-400">No available MT5 account left in vault.</div>`
      }
    </div>
  </div>


  <div class="mt5-plan-factory p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5 mb-6">
      <div>
        <span class="badge">5-YEAR-OLD SIMPLE MT5 FACTORY</span>
        <h3 class="text-4xl font-black gold mt-3">Create MT5 Pool In 4 Simple Steps</h3>
        <p class="text-gray-400 mt-2">No commas. No technical setup. Select plan, paste logins, choose server, create pools. Challenge Plans module stays untouched.</p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[210px]">
        <p class="text-gray-500 text-sm">Plans Ready</p>
        <h2 class="text-5xl font-black gold">${plans.length}</h2>
      </div>
    </div>

    <div class="grid xl:grid-cols-[1.05fr_.95fr] gap-6">
      <div class="grid gap-5">
        <div class="vault p-5 rounded-3xl">
          <div class="flex gap-4 items-start">
            <div class="bg-gold rounded-2xl w-12 h-12 flex items-center justify-center text-2xl font-black shrink-0">1</div>
            <div class="w-full">
              <h4 class="text-2xl font-black gold mb-2">Choose Challenge Plan</h4>
              <p class="text-gray-400 text-sm mb-3">Pick the plan you already created. The system will copy the plan name and account size automatically.</p>
              <select id="bulk_plan_index" onchange="renderBulkPlanPreview(); renderSimpleMT5Preview();">
                <option value="">Choose plan</option>
                ${plans.map((p,i)=>`<option value="${i}">${escapeHtml(formatPlanLabel(p))}</option>`).join("")}
              </select>
            </div>
          </div>
        </div>

        <div class="vault p-5 rounded-3xl">
          <div class="flex gap-4 items-start">
            <div class="bg-gold rounded-2xl w-12 h-12 flex items-center justify-center text-2xl font-black shrink-0">2</div>
            <div class="w-full">
              <h4 class="text-2xl font-black gold mb-2">Paste MT5 Logins Only</h4>
              <p class="text-gray-400 text-sm mb-3">Paste one MT5 login per line. Example: 12345678 then next line 12345679.</p>
              <textarea id="bulk_mt5_lines" oninput="renderSimpleMT5Preview()" class="mt5-bulk-box mb-3" placeholder="12345678\n12345679\n12345680"></textarea>
              <button onclick="document.getElementById('bulk_mt5_lines').value='';renderSimpleMT5Preview();" class="btn btn-dark" type="button">Clear Logins</button>
            </div>
          </div>
        </div>
      </div>

      <div class="grid gap-5">
        <div class="vault p-5 rounded-3xl">
          <div class="flex gap-4 items-start">
            <div class="bg-gold rounded-2xl w-12 h-12 flex items-center justify-center text-2xl font-black shrink-0">3</div>
            <div class="w-full">
              <h4 class="text-2xl font-black gold mb-2">Select Server</h4>
              <p class="text-gray-400 text-sm mb-3">Use the plan server automatically, or click another Exness server.</p>
              <select id="bulk_default_server" onchange="renderSimpleMT5Preview()">${serverOptions(getPlanServer(getBulkSelectedPlan()) || "Exness-MT5Trial9")}</select>
            </div>
          </div>
        </div>

        <div class="vault p-5 rounded-3xl">
          <div class="flex gap-4 items-start">
            <div class="bg-gold rounded-2xl w-12 h-12 flex items-center justify-center text-2xl font-black shrink-0">4</div>
            <div class="w-full">
              <h4 class="text-2xl font-black gold mb-2">Password Mode</h4>
              <p class="text-gray-400 text-sm mb-3">For fastest work, use Auto Generate. The system creates master and investor passwords for every MT5 account.</p>
              <div class="grid md:grid-cols-2 gap-3 mb-3">
                <button id="bulkModeUniqueBtn" onclick="setBulkPasswordMode('unique')" type="button" class="btn btn-gold">Auto Generate</button>
                <button id="bulkModeSameBtn" onclick="setBulkPasswordMode('same')" type="button" class="btn btn-dark">Use Same Password</button>
              </div>
              <input id="bulk_password_mode" type="hidden" value="unique">
              <div id="bulk_same_password_box" class="hidden grid gap-3">
                <input id="bulk_master_same" placeholder="Master password for all accounts">
                <input id="bulk_investor_same" placeholder="Investor password for all accounts">
                <button type="button" onclick="generateBulkSamePasswords()" class="btn btn-gold">Generate Same Passwords</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div id="bulk_plan_preview" class="mt5-plan-preview mt-6">
      <h4 class="text-2xl font-black gold">No plan selected</h4>
      <p class="text-gray-400 mt-2">Choose a plan above. Then paste MT5 logins. The preview will show here before creation.</p>
    </div>

    <div id="simple_mt5_preview" class="card2 p-5 rounded-3xl mt-5 border border-yellow-900/30">
      <h4 class="text-2xl font-black gold mb-2">Ready Check</h4>
      <p class="text-gray-400">Waiting for plan, logins and server.</p>
    </div>

    <div class="grid md:grid-cols-[1fr_auto_auto] gap-3 mt-5 items-center">
      <div class="text-gray-400 text-sm">Final action: when the ready check is green, click the gold button. Every account will enter MT5 Pool as AVAILABLE.</div>
      <button onclick="createBulkMT5FromPlan()" class="btn btn-gold text-lg px-8 py-4" type="button">CREATE MT5 POOLS</button>
      <button onclick="clearBulkMT5Factory()" class="btn btn-red" type="button">Reset</button>
    </div>
  </div>
  <div class="vault p-6 rounded-3xl mb-8 border border-yellow-900/40">
    <div class="flex flex-wrap justify-between gap-5 mb-5">
      <div>
        <span class="badge">BULK SERVER REPAIR</span>
        <h3 class="text-3xl font-black gold mt-3">Fix Wrong MT5 Server On Many Accounts</h3>
        <p class="text-gray-400 mt-2">Use this when you created many pool accounts with one missing letter or wrong server name. It will update every matching account at once.</p>
      </div>
      <div class="card2 p-4 rounded-2xl min-w-[190px]">
        <p class="text-gray-500 text-sm">MT5 Records</p>
        <h2 class="text-4xl font-black gold">${mt5pool.length}</h2>
      </div>
    </div>

    <div class="grid lg:grid-cols-[1fr_1fr_auto] gap-4 items-end">
      <div>
        <label class="text-gray-400 text-sm">Wrong server currently saved</label>
        <input id="bulk_wrong_server" placeholder="Example: Exnes-MT5Trial9">
        <p class="text-gray-500 text-xs mt-2">Type the wrong server exactly as it appears on the affected cards.</p>
      </div>
      <div>
        <label class="text-gray-400 text-sm">Correct Exness server</label>
        <select id="bulk_correct_server">${serverOptions("Exness-MT5Trial9")}</select>
        <p class="text-gray-500 text-xs mt-2">Pick the correct server from the list.</p>
      </div>
      <button onclick="bulkCorrectMT5Server()" class="btn btn-gold px-8 py-4" type="button">Fix Matching Accounts</button>
    </div>
  </div>

  <div class="mt5-vault-shell mb-8">
    <div class="vault p-6 rounded-3xl">
      <div class="mb-5">
        <span class="badge">MT5 Vault Entry</span>
        <h3 class="mt5-panel-title font-black gold mt-3">Add MT5 To Vault</h3>
        <p class="text-gray-400 mt-2">Store unused Exness MT5 accounts here. They remain Available until assigned.</p>
      </div>

      <div class="grid gap-3 mb-4">
        <input id="mt5_plan" placeholder="Plan Name e.g RUBBY">
        <input id="mt5_size" placeholder="Account Size e.g 1000000">
        <input id="mt5_login" placeholder="MT5 Login">
        <select id="mt5_server">${serverOptions("Exness-MT5Trial9")}</select>
      </div>

      <div class="mt5-password-box mb-4">
        <div class="flex flex-wrap justify-between gap-3 mb-4">
          <div>
            <h4 class="text-xl font-black gold">MT5 Password Generator</h4>
            <p class="text-gray-500 text-sm">Inputs now stay clear and visible. Buttons sit under the fields.</p>
          </div>
          <span class="badge">Strong Password Vault</span>
        </div>

        <div class="mt5-password-row">
          <label class="text-gray-400 text-sm">Master Password</label>
          <input id="mt5_master" class="mt5-password-input" placeholder="Generate or paste MT5 Master Password" autocomplete="off">
          <div class="mt5-password-actions">
            <button onclick="generateMT5Password('mt5_master')" type="button" class="btn btn-gold">Generate Master</button>
            <button onclick="copyMT5Field('mt5_master')" type="button" class="btn btn-dark">Copy Master</button>
          </div>
        </div>

        <div class="mt5-password-row">
          <label class="text-gray-400 text-sm">Investor Password</label>
          <input id="mt5_investor" class="mt5-password-input" placeholder="Generate or paste MT5 Investor Password" autocomplete="off">
          <div class="mt5-password-actions">
            <button onclick="generateMT5Password('mt5_investor')" type="button" class="btn btn-green">Generate Investor</button>
            <button onclick="copyMT5Field('mt5_investor')" type="button" class="btn btn-dark">Copy Investor</button>
          </div>
        </div>

        <div class="mt5-mini-actions">
          <button onclick="generateMT5Password('mt5_master');generateMT5Password('mt5_investor')" type="button" class="btn btn-gold">Generate Both</button>
          <button onclick="clearMT5Passwords()" type="button" class="btn btn-dark">Clear</button>
          <button onclick="previewMT5PasswordRules()" type="button" class="btn btn-dark">Rules</button>
        </div>
      </div>

      <textarea id="mt5_note" rows="4" placeholder="Admin note" class="mb-4"></textarea>

      <button onclick="createMT5()" class="btn btn-gold w-full">Add Unused MT5 Account</button>
    </div>

    <div>
      <div class="vault p-6 rounded-3xl mb-6">
        <div class="flex flex-wrap justify-between gap-4 mb-5">
          <div>
            <h3 class="text-3xl font-black gold">Available MT5 Accounts</h3>
            <p class="text-gray-400">Only unused accounts appear here. This is your real stock room.</p>
          </div>
          <span class="badge">${filteredAvailable.length} visible</span>
        </div>

        <div class="grid gap-5">
          ${filteredAvailable.map(mt5AvailableCard).join("") || empty("No available MT5 accounts left. Add more accounts to the vault.")}
        </div>
      </div>

      <div class="vault p-6 rounded-3xl">
        <div class="flex flex-wrap justify-between gap-4 mb-5">
          <div>
            <h3 class="text-3xl font-black gold">Assigned MT5 Accounts</h3>
            <p class="text-gray-400">These accounts have already left the available pool and are attached to traders.</p>
          </div>
          <span class="badge">${filteredAssigned.length} visible</span>
        </div>

        <div class="grid gap-5">
          ${filteredAssigned.map(mt5AssignedCard).join("") || empty("No assigned MT5 accounts yet.")}
        </div>
      </div>
    </div>
  </div>`;
}

function mt5AvailableCard(m){
  return `
  <div class="mt5-card p-5 rounded-2xl border border-green-900/30">
    <div class="flex flex-wrap justify-between gap-4 mb-4">
      <div>
        <span class="badge">AVAILABLE</span>
        <h3 class="text-2xl font-black gold mt-3">${m.mt5_login||"MT5 Login"}</h3>
        <p class="text-gray-400">${m.mt5_server||""}</p>
      </div>
      <div class="text-right">
        <p class="text-gray-500 text-sm">Account Size</p>
        <b class="text-2xl">${money(m.account_size)}</b>
      </div>
    </div>

    <div class="mt5-info-grid mb-4">
      <div class="mt5-secret"><p class="text-gray-500 text-sm">Plan</p><b>${m.plan_name||"No plan"}</b></div>
      <div class="mt5-secret"><p class="text-gray-500 text-sm">Created</p><b>${formatDate(m.created_at)}</b></div>
    </div>

    <div class="mt5-secret-grid mb-4">
      <div class="mt5-secret"><p class="text-gray-500 text-sm">Master Password</p><span class="mt5-secret-value">${m.mt5_master_password||"—"}</span></div>
      <div class="mt5-secret"><p class="text-gray-500 text-sm">Investor Password</p><span class="mt5-secret-value">${m.mt5_investor_password||"—"}</span></div>
    </div>

    <p class="text-gray-400 mb-4">Note: ${m.admin_note||"No note"}</p>

    <div class="flex flex-wrap gap-3">
      <button onclick="editMT5('${m.id}')" class="btn btn-gold">Edit</button>
      <button onclick="deleteMT5('${m.id}')" class="btn btn-red">Delete</button>
    </div>
  </div>`;
}

function mt5AssignedCard(m){
  return `
  <div class="mt5-assigned-card p-5 rounded-2xl border border-yellow-900/30">
    <div class="mt5-assigned-head mb-5">
      <div class="min-w-0">
        <div class="flex flex-wrap gap-2 items-center mb-3">
          <span class="badge">ASSIGNED</span>
          <span class="badge">IN USE</span>
        </div>
        <h3 class="mt5-assigned-login font-black gold">${m.mt5_login||"MT5 Login"}</h3>
        <p class="text-gray-400 mt-2 break-words">${m.mt5_server||"No server saved"}</p>
      </div>
      <div class="mt5-assigned-size card2 p-4 rounded-2xl">
        <p class="text-gray-500 text-sm">Account Size</p>
        <b class="text-2xl gold block break-words">${money(m.account_size)}</b>
      </div>
    </div>

    <div class="mt5-assigned-grid mb-4">
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Plan</p>
        <b>${m.plan_name||"No plan"}</b>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Assigned Trader</p>
        <b>${m.assigned_trader_name||"Unknown"}</b>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Trader Email</p>
        <span class="mt5-assigned-value text-gray-200">${m.assigned_email||"—"}</span>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Assigned Date</p>
        <span class="mt5-assigned-value text-gray-200">${formatDate(m.assigned_at)}</span>
      </div>
    </div>

    <div class="mt5-assigned-grid mb-4">
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Master Password</p>
        <span class="mt5-secret-value">${m.mt5_master_password||"—"}</span>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Investor Password</p>
        <span class="mt5-secret-value">${m.mt5_investor_password||"—"}</span>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Assigned Email</p>
        <span class="mt5-assigned-value text-gray-200">${m.assigned_email||"—"}</span>
      </div>
      <div class="mt5-assigned-field">
        <p class="text-gray-500 text-sm">Status</p>
        <b class="gold">${String(m.status||"assigned").toUpperCase()}</b>
      </div>
    </div>

    <div class="mt5-assigned-note mb-4">
      <p class="text-gray-500 text-sm mb-1">Admin Note</p>
      <p class="text-gray-300">${m.admin_note||"No note"}</p>
    </div>

    <div class="mt5-assigned-actions">
      <button onclick="editMT5('${m.id}')" class="btn btn-gold">Edit / Recycle</button>
      <button class="btn btn-dark" disabled>Assigned - Locked</button>
    </div>
  </div>`;
}


function getBulkSelectedPlan(){
  const idx = document.getElementById("bulk_plan_index")?.value;
  if(idx === "" || idx === undefined || idx === null) return null;
  return plans[Number(idx)] || null;
}

function renderBulkPlanPreview(){
  const box = document.getElementById("bulk_plan_preview");
  if(!box) return;
  const p = getBulkSelectedPlan();
  if(!p){
    box.innerHTML = `<h4 class="text-2xl font-black gold">No plan selected</h4><p class="text-gray-400 mt-2">Choose a plan above. Then paste MT5 logins. The preview will show here before creation.</p>`;
    return;
  }
  box.innerHTML = `
    <div class="flex flex-wrap justify-between gap-4 items-start">
      <div>
        <span class="badge">SELECTED PLAN</span>
        <h4 class="text-3xl font-black gold mt-3">${p.name||"Challenge Plan"}</h4>
        <p class="text-gray-400 mt-2">This is the plan that will be attached to every MT5 pool account created below.</p>
      </div>
      <button type="button" onclick="loadBulkPlanToSingleMT5()" class="btn btn-dark">Use For Single Add</button>
    </div>
    <div class="mt5-plan-preview-grid">
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Account Size</p><b class="gold">${money(p.account_size)}</b></div>
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Fee</p><b>${money(p.fee)}</b></div>
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Phase 1</p><b>${p.phase1_target||10}%</b></div>
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Phase 2</p><b>${p.phase2_target||8}%</b></div>
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Max DD</p><b>${p.max_drawdown||20}%</b></div>
      <div class="mt5-plan-preview-item"><p class="text-gray-500 text-sm">Payout</p><b>${p.payout_split||"80%"}</b></div>
    </div>`;
}

function setBulkPasswordMode(mode){
  const hidden = document.getElementById("bulk_password_mode");
  if(hidden) hidden.value = mode;
  renderBulkPasswordMode();
  renderSimpleMT5Preview();
}

function renderBulkPasswordMode(){
  const mode = document.getElementById("bulk_password_mode")?.value || "unique";
  const box = document.getElementById("bulk_same_password_box");
  if(box) box.classList.toggle("hidden", mode !== "same");
  const uniqueBtn = document.getElementById("bulkModeUniqueBtn");
  const sameBtn = document.getElementById("bulkModeSameBtn");
  if(uniqueBtn) uniqueBtn.className = mode === "unique" ? "btn btn-gold" : "btn btn-dark";
  if(sameBtn) sameBtn.className = mode === "same" ? "btn btn-gold" : "btn btn-dark";
}

function makeMT5Password(length = 14){
  const upper = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const lower = "abcdefghijkmnopqrstuvwxyz";
  const numbers = "23456789";
  const symbols = "@#$%&*!?";
  const all = upper + lower + numbers + symbols;
  let password = "";
  password += upper[Math.floor(Math.random() * upper.length)];
  password += lower[Math.floor(Math.random() * lower.length)];
  password += numbers[Math.floor(Math.random() * numbers.length)];
  password += symbols[Math.floor(Math.random() * symbols.length)];
  for(let i=password.length; i<length; i++) password += all[Math.floor(Math.random() * all.length)];
  return password.split("").sort(() => Math.random() - 0.5).join("");
}

function generateBulkSamePasswords(){
  const m = document.getElementById("bulk_master_same");
  const i = document.getElementById("bulk_investor_same");
  if(m) m.value = makeMT5Password();
  if(i) i.value = makeMT5Password();
  renderSimpleMT5Preview();
}

function loadBulkPlanToSingleMT5(){
  const p = getBulkSelectedPlan();
  if(!p){alert("Select a challenge plan first.");return;}
  const plan = document.getElementById("mt5_plan");
  const size = document.getElementById("mt5_size");
  const server = document.getElementById("mt5_server");
  if(plan) plan.value = p.name || "";
  if(size) size.value = p.account_size || "";
  if(server && getPlanServer(p)) server.value = getPlanServer(p);
  alert("Plan name, account size and server loaded into the single MT5 add form.");
}

function syncBulkServerFromPlan(){
  const p = getBulkSelectedPlan();
  const server = document.getElementById("bulk_default_server");
  if(server && p && getPlanServer(p)){
    server.value = getPlanServer(p);
  }
}

function parseBulkMT5Lines(){
  const raw = document.getElementById("bulk_mt5_lines")?.value || "";
  const defaultServer = (document.getElementById("bulk_default_server")?.value || "").trim();
  return raw.split("\n")
    .map(line=>line.trim())
    .filter(Boolean)
    .map(line=>{
      const login = line.split(/[ ,;|]+/)[0].trim();
      return {mt5_login: login, mt5_server: defaultServer};
    })
    .filter(x=>x.mt5_login);
}

function renderSimpleMT5Preview(){
  const box = document.getElementById("simple_mt5_preview");
  if(!box) return;
  const p = getBulkSelectedPlan();
  const rows = parseBulkMT5Lines();
  const server = (document.getElementById("bulk_default_server")?.value || "").trim();
  const mode = document.getElementById("bulk_password_mode")?.value || "unique";
  const sameMaster = document.getElementById("bulk_master_same")?.value || "";
  const sameInvestor = document.getElementById("bulk_investor_same")?.value || "";
  const ready = !!p && rows.length > 0 && !!server && (mode === "unique" || (sameMaster && sameInvestor));
  const sample = rows.slice(0,6).map(r=>`
    <tr>
      <td class="gold font-bold">${r.mt5_login}</td>
      <td>${p ? (p.name||"Challenge Plan") : "Choose plan"}</td>
      <td>${p ? money(p.account_size) : "—"}</td>
      <td>${server || "Add server"}</td>
      <td>${mode === "unique" ? "Auto generated" : "Same password"}</td>
    </tr>`).join("");
  box.innerHTML = `
    <div class="flex flex-wrap justify-between gap-4 mb-4">
      <div>
        <span class="badge ${ready ? "text-green-400" : "text-yellow-400"}">${ready ? "READY TO CREATE" : "COMPLETE THE 4 STEPS"}</span>
        <h4 class="text-2xl font-black gold mt-3">${rows.length} MT5 login(s) detected</h4>
        <p class="text-gray-400 mt-1">${ready ? "Everything is ready. Click CREATE MT5 POOLS." : "Select plan, paste logins, add server, and choose password mode."}</p>
      </div>
      <div class="card p-4 rounded-2xl min-w-[170px]"><p class="text-gray-500 text-sm">Status</p><h3 class="text-2xl font-black ${ready ? "text-green-400" : "text-yellow-400"}">${ready ? "GOOD" : "WAITING"}</h3></div>
    </div>
    <div class="tableWrap">
      <table style="min-width:780px">
        <thead><tr><th>MT5 Login</th><th>Plan</th><th>Size</th><th>Server</th><th>Password</th></tr></thead>
        <tbody>${sample || `<tr><td colspan="5" class="text-center text-gray-400 py-6">Paste MT5 logins to preview.</td></tr>`}</tbody>
      </table>
    </div>
    ${rows.length > 6 ? `<p class="text-gray-500 text-sm mt-3">Showing first 6 only. Total ready: ${rows.length}</p>` : ""}`;
}

async function createBulkMT5FromPlan(){
  const p = getBulkSelectedPlan();
  if(!p){alert("Step 1: Choose a challenge plan first.");return;}
  const rows = parseBulkMT5Lines();
  if(!rows.length){alert("Step 2: Paste at least one MT5 login.");return;}
  const server = (document.getElementById("bulk_default_server")?.value || "").trim();
  if(!server){alert("Step 3: Enter the MT5 server once.");return;}

  const mode = document.getElementById("bulk_password_mode")?.value || "unique";
  const sameMaster = document.getElementById("bulk_master_same")?.value || "";
  const sameInvestor = document.getElementById("bulk_investor_same")?.value || "";
  if(mode === "same" && (!sameMaster || !sameInvestor)){
    alert("Step 4: Generate or enter the same Master and Investor passwords.");
    return;
  }

  if(!confirm(`Create ${rows.length} MT5 pool account(s)?\n\nPlan: ${p.name || "Challenge Plan"}\nSize: ${money(p.account_size)}\nServer: ${server}`)) return;

  let success = 0;
  let failed = 0;
  for(const r of rows){
    const payload = {
      plan_name: p.name || "Challenge Plan",
      account_size: p.account_size || 0,
      mt5_login: r.mt5_login,
      mt5_server: server,
      mt5_master_password: mode === "unique" ? makeMT5Password() : sameMaster,
      mt5_investor_password: mode === "unique" ? makeMT5Password() : sameInvestor,
      admin_note: `Simple bulk created from challenge plan: ${p.name || "Challenge Plan"}`,
      status:"available"
    };
    try{
      const res = await fetch(`${API_URL}/create_mt5_account`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      const data = await res.json();
      if(data.success) success++; else failed++;
    }catch(e){ failed++; }
  }
  alert(`Done.\n\nCreated: ${success}\nFailed: ${failed}`);
  if(success) loadData();
}

function clearBulkMT5Factory(){
  const fields = ["bulk_plan_index","bulk_default_server","bulk_mt5_lines","bulk_master_same","bulk_investor_same"];
  fields.forEach(id=>{const el=document.getElementById(id); if(el) el.value="";});
  const mode = document.getElementById("bulk_password_mode");
  if(mode) mode.value = "unique";
  renderBulkPasswordMode();
  renderBulkPlanPreview();
  renderSimpleMT5Preview();
}

function generateMT5Password(inputId, length = 14){
  const upper = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const lower = "abcdefghijkmnopqrstuvwxyz";
  const numbers = "23456789";
  const symbols = "@#$%&*!?";
  const all = upper + lower + numbers + symbols;

  let password = "";
  password += upper[Math.floor(Math.random() * upper.length)];
  password += lower[Math.floor(Math.random() * lower.length)];
  password += numbers[Math.floor(Math.random() * numbers.length)];
  password += symbols[Math.floor(Math.random() * symbols.length)];

  for(let i=password.length; i<length; i++){
    password += all[Math.floor(Math.random() * all.length)];
  }

  password = password.split("").sort(() => Math.random() - 0.5).join("");

  const field = document.getElementById(inputId);
  if(field){
    field.value = password;
    field.focus();
  }
}

async function copyMT5Field(inputId){
  const field = document.getElementById(inputId);
  if(!field || !field.value){
    alert("Nothing to copy yet. Generate the password first.");
    return;
  }

  try{
    await navigator.clipboard.writeText(field.value);
    alert("Password copied.");
  }catch(e){
    field.select();
    document.execCommand("copy");
    alert("Password copied.");
  }
}

function clearMT5Passwords(){
  const master = document.getElementById("mt5_master");
  const investor = document.getElementById("mt5_investor");
  if(master) master.value = "";
  if(investor) investor.value = "";
}

function previewMT5PasswordRules(){
  alert("Password rules:\\n\\n• 14 characters\\n• Uppercase letters\\n• Lowercase letters\\n• Numbers\\n• Symbols: @ # $ % & * ! ?\\n\\nUse different Master and Investor passwords for every MT5 account.");
}


async function createMT5(){
  if(!canDo("mt5pool","create")){alert("Access denied: create MT5 pool");return;}
  const payload = {
    plan_name:document.getElementById("mt5_plan").value,
    account_size:document.getElementById("mt5_size").value,
    mt5_login:document.getElementById("mt5_login").value,
    mt5_server:document.getElementById("mt5_server").value,
    mt5_master_password:document.getElementById("mt5_master").value,
    mt5_investor_password:document.getElementById("mt5_investor").value,
    admin_note:document.getElementById("mt5_note").value,
    status:"available"
  };

  const res = await fetch(`${API_URL}/create_mt5_account`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)
  });
  const data = await res.json();
  if(data.success){alert("MT5 account added to vault.");loadData();}
  else alert(data.error || "Failed to add MT5");
}

async function editMT5(id){
  const m = mt5pool.find(x=>String(x.id)===String(id));
  if(!m) return;
  const plan_name = prompt("Plan name:", m.plan_name||"");
  if(plan_name === null) return;
  const account_size = prompt("Account size:", m.account_size||"");
  if(account_size === null) return;
  const mt5_login = prompt("MT5 login:", m.mt5_login||"");
  if(mt5_login === null) return;
  const mt5_server = prompt("MT5 server - correct any missing letter here:", m.mt5_server||"");
  if(mt5_server === null) return;
  const mt5_master_password = prompt("Master password:", m.mt5_master_password||"");
  if(mt5_master_password === null) return;
  const mt5_investor_password = prompt("Investor password:", m.mt5_investor_password||"");
  if(mt5_investor_password === null) return;
  const status = prompt("Status: available / assigned / inactive", m.status||"available");
  if(status === null) return;
  const admin_note = prompt("Admin note:", m.admin_note||"");
  if(admin_note === null) return;

  const res = await fetch(`${API_URL}/update_mt5_account`,{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({id,plan_name,account_size,mt5_login,mt5_server,mt5_master_password,mt5_investor_password,status,admin_note})
  });
  const data = await res.json();
  if(data.success){alert("MT5 account updated.");loadData();}
  else alert(data.error || "Update failed");
}

async function bulkCorrectMT5Server(){
  const wrong = (document.getElementById("bulk_wrong_server")?.value || "").trim();
  const correct = (document.getElementById("bulk_correct_server")?.value || "").trim();
  if(!wrong){alert("Type the wrong server exactly as it appears first.");return;}
  if(!correct){alert("Choose the correct Exness server.");return;}
  const matches = mt5pool.filter(m=>String(m.mt5_server||"").trim() === wrong);
  if(!matches.length){alert(`No MT5 pool account found with server: ${wrong}`);return;}
  if(!confirm(`Fix ${matches.length} MT5 account(s)?

From: ${wrong}
To: ${correct}`)) return;

  let success = 0;
  let failed = 0;
  for(const m of matches){
    const payload = {
      id:m.id,
      plan_name:m.plan_name || "",
      account_size:m.account_size || "",
      mt5_login:m.mt5_login || "",
      mt5_server:correct,
      mt5_master_password:m.mt5_master_password || "",
      mt5_investor_password:m.mt5_investor_password || "",
      status:m.status || "available",
      admin_note:(m.admin_note || "") + ` | Server corrected from ${wrong} to ${correct}`
    };
    try{
      const res = await fetch(`${API_URL}/update_mt5_account`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      const data = await res.json();
      if(data.success) success++; else failed++;
    }catch(e){ failed++; }
  }
  alert(`Server correction finished.

Updated: ${success}
Failed: ${failed}`);
  loadData();
}

async function deleteMT5(id){
  if(!canDo("mt5pool","delete")){alert("Access denied: delete MT5 pool");return;}
  if(!confirm("Delete this MT5 account from vault?")) return;
  const res = await fetch(`${API_URL}/delete_mt5_account`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id})
  });
  const data = await res.json();
  if(data.success){alert("MT5 account deleted.");loadData();}
  else alert(data.error || "Delete failed");
}

/* PAYMENTS */
function paymentsModule(){
  const list = filteredTraders().filter(t=>t.payment_status==="pending" || t.status==="payment_pending");
  document.getElementById("content").innerHTML = `
  <div class="mb-5 flex flex-wrap gap-3">
    <span class="badge">${list.length} pending payment reviews</span>
    <span class="badge">Receipt verification</span>
    <span class="badge">Manual MT5 assignment</span>
  </div>
  <div class="grid gap-5">${list.map(paymentCard).join("") || empty("No pending payments.")}</div>`;
}

function paymentCard(t){
return `
<div class="vault p-6 rounded-3xl">
  <div class="flex flex-wrap justify-between gap-4 mb-5">
    <div>
      <h3 class="text-2xl font-bold">${t.name||"No Name"}</h3>
      <p class="text-gray-400">${t.email||""} • ${t.phone||""}</p>
      <p class="text-gray-600 text-xs">Submitted: ${formatDate(t.created_at)}</p>
    </div>
    <span class="badge">${t.payment_status||"pending"}</span>
  </div>

  <div class="grid md:grid-cols-4 gap-4 mb-5">
    <div><p class="text-gray-500 text-sm">Plan</p><b>${t.selected_plan||""}</b></div>
    <div><p class="text-gray-500 text-sm">Reference</p><b>${t.account_reference||"Generating..."}</b></div>
    <div><p class="text-gray-500 text-sm">Receipt</p><a class="gold underline" href="${t.payment_proof_url}" target="_blank">Open Receipt</a></div>
    <div><p class="text-gray-500 text-sm">Record ID</p><b class="text-xs">${t.id}</b></div>
  </div>

  <div class="grid md:grid-cols-2 gap-4 mb-5">
    <input id="size-${t.id}" placeholder="Account Size e.g 3000000">
    <input id="login-${t.id}" placeholder="MT5 Login">
    <input id="server-${t.id}" placeholder="MT5 Server e.g Exness-MT5Trial9">
    <input id="master-${t.id}" placeholder="MT5 Master Password">
    <input id="investor-${t.id}" placeholder="MT5 Investor Password">
    <select id="phase-${t.id}">
      <option value="phase1">phase1</option>
      <option value="phase2">phase2</option>
      <option value="funded">funded</option>
    </select>
  </div>

  <div class="flex flex-wrap gap-3">
    <button class="btn btn-gold" onclick="approvePayment('${t.id}')">Approve + Activate</button>
    <button class="btn btn-red" onclick="rejectPayment('${t.id}')">Reject</button>
    <a class="btn btn-dark" href="${t.payment_proof_url}" target="_blank">View Receipt</a>
  </div>
</div>`;
}

async function approvePayment(id){
  if(!canDo("payments","approve")){alert("Access denied: approve payments");return;}
  const payload = {
    id,
    account_size: document.getElementById(`size-${id}`).value,
    mt5_login: document.getElementById(`login-${id}`).value,
    mt5_server: document.getElementById(`server-${id}`).value,
    mt5_master_password: document.getElementById(`master-${id}`).value,
    mt5_investor_password: document.getElementById(`investor-${id}`).value,
    phase: document.getElementById(`phase-${id}`).value,
    approved_by: currentAdmin?.username || "admin",
    admin_name: currentAdmin?.name || currentAdmin?.username || "admin",
    admin_username: currentAdmin?.username || "admin"
  };

  const res = await fetch(`${API_URL}/approve_payment`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)
  });
  const data = await res.json();
  if(data.success){await logAdminAudit("payments","payment_approved",`Trader payment approved and activated: ${id}`,id);alert("Payment approved and trader activated.");loadData();}
  else alert(data.error || "Approval failed");
}

async function rejectPayment(id){
  if(!canDo("payments","approve")){alert("Access denied: reject payments");return;}
  const res = await fetch(`${API_URL}/reject_payment`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,admin_note:"Payment rejected"})
  });
  const data = await res.json();
  if(data.success){alert("Payment rejected.");loadData();}
  else alert(data.error || "Rejection failed");
}

/* TRADERS */
function tradersModule(){
  const list = filteredTraders();
  document.getElementById("content").innerHTML = `
  <div class="tableWrap">
  <table>
    <tr>
      <th>Trader</th><th>Reference</th><th>Joined</th><th>Approved</th><th>Last Login</th><th>Plan</th><th>Payment</th><th>Status</th><th>Phase</th><th>MT5</th><th>Equity</th><th>Profit/DD</th><th>Actions</th>
    </tr>
    ${list.map(t=>`
    <tr>
      <td><b>${t.name||""}</b><br>${t.email||""}<br>${t.phone||""}</td>
      <td>${t.account_reference||"—"}</td>
      <td>${formatDate(t.created_at)}</td>
      <td>${formatDate(t.approved_at)}</td>
      <td>${formatDate(t.last_login_at)}</td>
      <td>${t.selected_plan||""}</td>
      <td>${t.payment_status||""}</td>
      <td>${t.status||""}</td>
      <td>${t.phase||""}</td>
      <td>${t.mt5_login||""}<br>${t.mt5_server||""}</td>
      <td>${money(t.equity||t.balance||0)}</td>
      <td>${pct(t.profit_percent)} / ${pct(t.drawdown_percent)}</td>
      <td>
        <button class="btn btn-green" onclick="activateTrader('${t.id}')">Activate</button>
        <button class="btn btn-dark" onclick="deactivateTrader('${t.id}')">Deactivate</button>
        <button class="btn btn-red" onclick="deleteTrader('${t.id}')">Delete</button>
<button onclick="openMT5Reset('${t.id}')" class="btn btn-gold mt-2">Reset MT5</button>
      </td>
    </tr>`).join("")}
  </table>
  </div>`;
}



function openMT5Reset(id){
  const trader = traders.find(t => String(t.id) === String(id));
  if(!trader){
    alert("Trader not found");
    return;
  }

  const login = prompt("New MT5 Login:", trader.mt5_login || "");
  if(login === null) return;

  const server = prompt("New MT5 Server:", trader.mt5_server || "Exness-MT5Trial9");
  if(server === null) return;

  const master = prompt("New Master Password:", trader.mt5_password || trader.master_password || "");
  if(master === null) return;

  const investor = prompt("New Investor Password:", trader.mt5_investor_password || trader.investor_password || "");
  if(investor === null) return;

  const reason = prompt("Admin note only. Trader will not see this exact note:", "MT5 login details updated");
  if(reason === null) return;

  const publicNote = "Your MT5 login details have been updated. Please use the latest details shown on your dashboard.";

  const payload = {
    id:id,
    trader_id:id,
    mt5_login:login,
    mt5_server:server,
    mt5_password:master,
    mt5_investor_password:investor,
    master_password:master,
    investor_password:investor,
    status: trader.status || "active",
    phase: trader.phase || "phase1",
    mt5_updated_by: currentAdmin?.username || "admin",
    admin_name: currentAdmin?.name || currentAdmin?.username || "admin",
    admin_username: currentAdmin?.username || "admin",
    mt5_reset_reason: reason || "MT5 login details updated",
    admin_note: reason || "MT5 login details updated",
    trader_note: publicNote,
    mt5_notice: publicNote
  };

  async function tryReset(){
    let lastError = "";

    const endpoints = [
      `${API_URL}/update_trader`,
      `${API_URL}/update_trader_mt5`,
      `${API_URL}/reset_trader_mt5`
    ];

    for(const url of endpoints){
      try{
        const res = await fetch(url,{
          method:"POST",
          headers:{"Content-Type":"application/json"},
          body:JSON.stringify(payload)
        });

        const data = await res.json().catch(()=>({}));

        if(res.ok && data.success !== false){
          await logAdminAudit("mt5","mt5_account_update",`MT5 reset/update for trader ${id}: ${login} / ${server}`,id);
          alert("MT5 details updated successfully.");
          loadData();
          return;
        }

        lastError = data.error || data.message || `Failed on ${url}`;
      }catch(e){
        lastError = e.message || "Connection error";
      }
    }

    alert("MT5 reset could not reach a working backend update route. This is admin-only. It is NOT caused by the note you typed. Error: " + lastError);
  }

  if(confirm("Reset this trader's MT5 details now?")){
    tryReset();
  }
}


async function activateTrader(id){
  const res = await fetch(`${API_URL}/activate_trader`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"})});
  const data = await res.json();
  if(data.success){await logAdminAudit("traders","trader_activated",`Trader activated: ${id}`,id);alert("Trader activated.");loadData();} else alert("Failed");
}
async function deactivateTrader(id){
  const res = await fetch(`${API_URL}/deactivate_trader`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id})});
  const data = await res.json();
  if(data.success){alert("Trader deactivated.");loadData();} else alert("Failed");
}
async function deleteTrader(id){
  if(!canDo("traders","delete")){alert("Access denied: delete traders");return;}
  const trader = traders.find(t=>String(t.id)===String(id)) || {};
  const funded = ["funded","live"].includes(String(trader.status||"").toLowerCase()) || ["funded","live"].includes(String(trader.phase||"").toLowerCase()) || trader.funded_at;
  const approvedPayment = String(trader.payment_status||"").toLowerCase()==="approved";
  if(funded){alert("Funded/live traders cannot be deleted in production. Deactivate or mark as test instead.");return;}
  if(approvedPayment){alert("Traders with approved payments cannot be deleted in production. Mark as test or exclude from revenue instead.");return;}
  if(prompt("Dangerous action. Type DELETE TRADER to continue:") !== "DELETE TRADER") return;
  const res = await fetch(`${API_URL}/delete_trader`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"})});
  const data = await res.json();
  if(data.success){await logAdminAudit("traders","trader_deleted",`Trader deleted from admin: ${id}`,id);alert("Deleted.");loadData();} else alert(data.error || "Failed");
}

/* ADD TRADER */
function addTraderModule(){
  document.getElementById("content").innerHTML = `
  <div class="vault p-6 rounded-3xl">
    <h3 class="text-2xl font-bold gold mb-5">Manual Add Trader</h3>
    <div class="grid md:grid-cols-2 gap-4 mb-5">
      <input id="add_name" placeholder="Full Name">
      <input id="add_phone" placeholder="Phone">
      <input id="add_email" placeholder="Email">
      <input id="add_plan" placeholder="Selected Plan">
      <input id="add_size" placeholder="Account Size e.g 3000000">
      <input id="add_mt5" placeholder="MT5 Login">
      <input id="add_server" placeholder="MT5 Server">
      <input id="add_master" placeholder="Master Password">
      <input id="add_investor" placeholder="Investor Password">
      <select id="add_phase"><option value="phase1">phase1</option><option value="phase2">phase2</option><option value="funded">funded</option></select>
    </div>
    <button class="btn btn-gold" onclick="manualAddTrader()">Add Trader</button>
  </div>`;
}

async function manualAddTrader(){
  const payload = {
    name:document.getElementById("add_name").value,
    phone:document.getElementById("add_phone").value,
    email:document.getElementById("add_email").value,
    selected_plan:document.getElementById("add_plan").value,
    account_size:document.getElementById("add_size").value,
    balance:document.getElementById("add_size").value,
    mt5_login:document.getElementById("add_mt5").value,
    mt5_server:document.getElementById("add_server").value,
    mt5_master_password:document.getElementById("add_master").value,
    mt5_investor_password:document.getElementById("add_investor").value,
    phase:document.getElementById("add_phase").value,
    status:"active",
    payment_status:"approved"
  };

  const res = await fetch(`${API_URL}/traders`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)
  });
  const data = await res.json();
  if(data.success){alert("Trader added.");loadData();}
  else alert(data.error || "Failed to add trader");
}

function bulkTraderModule(){
  document.getElementById("content").innerHTML = `
  <div class="vault p-6 rounded-3xl">
    <h3 class="text-2xl font-bold gold mb-5">Bulk Add Traders</h3>
    <p class="text-gray-400 mb-4">Format per line: name,phone,email,plan,account_size</p>
    <textarea id="bulkText" rows="12" placeholder="John Doe,08012345678,john@email.com,₦500,000 Account,500000"></textarea>
    <button class="btn btn-gold mt-5" onclick="bulkAddTraders()">Bulk Add</button>
  </div>`;
}

async function bulkAddTraders(){
  const lines = document.getElementById("bulkText").value.trim().split("\\n");
  let success = 0;
  for(const line of lines){
    const p = line.split(",");
    if(p.length < 5) continue;
    const payload = {
      name:p[0].trim(), phone:p[1].trim(), email:p[2].trim(),
      selected_plan:p[3].trim(), account_size:p[4].trim(), balance:p[4].trim(),
      payment_status:"approved", status:"active", phase:"phase1"
    };
    try{
      await fetch(`${API_URL}/traders`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      success++;
    }catch(e){}
  }
  alert(success + " traders processed.");
  loadData();
}

/* PAYOUTS */

function payoutKpi(label,value,extra="",tone="gold"){
  const cls = tone==="red" ? "text-red-400" : tone==="green" ? "text-green-400" : "gold";
  return `<div class="vault p-5 rounded-2xl overflow-hidden min-w-0">
    <p class="text-gray-400 text-sm">${label}</p>
    <h3 class="kpi-amount font-black ${cls}">${value}</h3>
    <p class="text-gray-500 text-xs mt-1 break-words">${extra}</p>
  </div>`;
}


function payoutsModule(){
  const s = q();

  const list = payouts.filter(p =>
    (p.trader_name||"").toLowerCase().includes(s) ||
    (p.email||"").toLowerCase().includes(s) ||
    (p.phone||"").toLowerCase().includes(s) ||
    (p.status||"").toLowerCase().includes(s) ||
    (p.bank_name||"").toLowerCase().includes(s) ||
    (p.account_number||"").toLowerCase().includes(s)
  );

  const pending = payouts.filter(p=>p.status==="pending");
  const approved = payouts.filter(p=>p.status==="approved");
  const paid = payouts.filter(p=>p.status==="paid");
  const rejected = payouts.filter(p=>p.status==="rejected");

  const sum = (arr)=>arr.reduce((a,p)=>a+Number(p.amount||0),0);

  const pendingAmount = sum(pending);
  const approvedAmount = sum(approved);
  const paidAmount = sum(paid);
  const rejectedAmount = sum(rejected);

  const fundedTraders = traders.filter(t=>{
    const status = String(t.status||"").toLowerCase();
    const phase = String(t.phase||"").toLowerCase();
    return status !== "breached" && (status==="funded" || status==="live" || phase==="funded" || phase==="live");
  });

  const eligibleFunded = fundedTraders.filter(t=>Number(t.profit||0)>0);
  const totalFundedProfit = fundedTraders.reduce((a,t)=>a+Number(t.profit||0),0);
  const payoutPressure = totalFundedProfit > 0 ? Math.min(100,(pendingAmount+approvedAmount)/totalFundedProfit*100) : 0;

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">Payout Intelligence</span>
        <h3 class="text-4xl font-black gold mt-3">Payout Command Center</h3>
        <p class="text-gray-400 mt-2">
          Review payout requests, control payout liability, detect payout pressure and protect business cashflow.
        </p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[260px]">
        <p class="text-gray-500 text-sm">Payout Pressure</p>
        <h2 class="text-5xl font-black ${payoutPressure > 60 ? "text-red-400" : payoutPressure > 30 ? "text-yellow-400" : "text-green-400"}">${payoutPressure.toFixed(1)}%</h2>
        <p class="text-gray-500 text-xs mt-1">Pending + approved vs funded profit</p>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
    ${payoutKpi("Pending Requests",pending.length,"Needs review","gold")}
    ${payoutKpi("Pending Amount",money(pendingAmount),"Awaiting decision","gold")}
    ${payoutKpi("Approved Amount",money(approvedAmount),"Liability before paid","gold")}
    ${payoutKpi("Paid Out",money(paidAmount),"Completed payouts","green")}
    ${payoutKpi("Rejected Amount",money(rejectedAmount),"Declined requests","red")}
    ${payoutKpi("Eligible Funded",eligibleFunded.length,"Can request payout","green")}
  </div>

  <div class="grid lg:grid-cols-3 gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Payout Risk Logic</h3>
      <p class="text-gray-400">
        Payout pressure compares pending and approved payout liability against visible funded trader profit.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Risk signal</p>
        <p class="${payoutPressure > 60 ? "text-red-400" : payoutPressure > 30 ? "text-yellow-400" : "text-green-400"} font-bold">
          ${payoutPressure > 60 ? "High payout pressure" : payoutPressure > 30 ? "Moderate payout pressure" : "Healthy payout pressure"}
        </p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Approval Discipline</h3>
      <p class="text-gray-400">
        Approve only after checking MT5 performance, rule compliance, trader identity and payout history.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Admin rule</p>
        <p class="text-gray-300">Review → Approve → Mark Paid</p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Cashflow Protection</h3>
      <p class="text-gray-400">
        Approved payouts become liability. Paid payouts reduce net revenue immediately.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Current liability</p>
        <p class="text-red-400 font-bold">${money(pendingAmount + approvedAmount)}</p>
      </div>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-4 mb-5">
      <div>
        <h3 class="text-3xl font-black gold">Payout Requests Queue</h3>
        <p class="text-gray-400">Process payout requests with approval intelligence.</p>
      </div>
      <span class="badge">${list.length} visible</span>
    </div>

    <div class="grid gap-5">
      ${list.map(payoutCard).join("") || empty("No payout requests yet.")}
    </div>
  </div>`;
}

function payoutRiskBadge(p){
  const amount = Number(p.amount||0);
  const trader = traders.find(t =>
    String(t.id||"")===String(p.trader_id||"") ||
    String(t.email||"").toLowerCase()===String(p.email||"").toLowerCase() ||
    String(t.phone||"").toLowerCase()===String(p.phone||"").toLowerCase()
  );

  const profit = Number(trader?.profit||0);
  const status = String(trader?.status||"").toLowerCase();

  if(status==="breached") return `<span class="badge text-red-400">BREACHED TRADER</span>`;
  if(profit > 0 && amount > profit) return `<span class="badge text-red-400">ABOVE PROFIT</span>`;
  if(amount >= 500000) return `<span class="badge text-yellow-400">HIGH VALUE</span>`;
  if(p.status==="paid") return `<span class="badge text-green-400">PAID</span>`;
  return `<span class="badge text-green-400">NORMAL</span>`;
}

function payoutCard(p){
  const payoutStatus = String(p.status||"pending").toLowerCase();
  const payoutActions = payoutStatus === "pending"
    ? `<button class="btn btn-green" onclick="approvePayout('${p.id}')">Approve</button>
      <button class="btn btn-red" onclick="rejectPayout('${p.id}')">Reject</button>
      <button class="btn btn-dark" onclick="payoutReview('${p.id}')">Review</button>`
    : payoutStatus === "approved"
    ? `<button class="btn btn-gold" onclick="markPayoutPaid('${p.id}')">Mark Paid</button>
      <button class="btn btn-dark" onclick="payoutReview('${p.id}')">Review</button>`
    : `<button class="btn btn-dark" onclick="payoutReview('${p.id}')">Review</button>`;
  const trader = traders.find(t =>
    String(t.id||"")===String(p.trader_id||"") ||
    String(t.email||"").toLowerCase()===String(p.email||"").toLowerCase() ||
    String(t.phone||"").toLowerCase()===String(p.phone||"").toLowerCase()
  );

  const traderProfit = Number(trader?.profit||0);
  const requestAmount = Number(p.amount||0);
  const profitCoverage = traderProfit > 0 ? Math.min(999,requestAmount/traderProfit*100) : 0;

  return `
  <div class="vault p-6 rounded-3xl overflow-hidden">
    <div class="flex flex-wrap justify-between gap-5 mb-6">
      <div class="min-w-0 flex-1">
        <div class="flex flex-wrap gap-2 mb-3">
          <span class="badge">${p.status||"pending"}</span>
          ${payoutRiskBadge(p)}
        </div>
        <h3 class="text-3xl font-black gold break-words">${p.trader_name||"Trader Payout"}</h3>
        <p class="text-gray-400 break-words">${p.email||""} • ${p.phone||""}</p>
      </div>

      <div class="text-right min-w-[220px] max-w-full">
        <p class="text-gray-500 text-sm">Requested Amount</p>
        <h3 class="payout-amount-main font-black gold">${money(p.amount)}</h3>
      </div>
    </div>

    <div class="grid payout-bank-grid gap-4 mb-6">
      <div class="card2 p-5 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Bank</p>
        <h3 class="payout-field-value font-black">${p.bank_name||"Not provided"}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Account Number</p>
        <h3 class="payout-field-value font-black">${p.account_number||"Not provided"}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Account Name</p>
        <h3 class="payout-field-value font-black">${p.account_name||"Not provided"}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Trader Profit</p>
        <h3 class="payout-field-value font-black text-green-400">${money(traderProfit)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Profit Coverage</p>
        <h3 class="payout-field-value font-black ${profitCoverage > 100 ? "text-red-400" : "text-green-400"}">${profitCoverage.toFixed(1)}%</h3>
      </div>
    </div>

    <div class="grid payout-date-grid gap-4 mb-6">
      <div class="card2 p-4 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Requested</p>
        <b class="payout-date-value block">${formatDate(p.requested_at || p.created_at)}</b>
      </div>
      <div class="card2 p-4 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Approved</p>
        <b class="payout-date-value block">${formatDate(p.approved_at)}</b>
      </div>
      <div class="card2 p-4 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Paid</p>
        <b class="payout-date-value block">${formatDate(p.paid_at)}</b>
      </div>
      <div class="card2 p-4 rounded-2xl payout-field">
        <p class="text-gray-500 text-sm mb-2">Rejected</p>
        <b class="payout-date-value block">${formatDate(p.rejected_at)}</b>
      </div>
    </div>

    <div class="card2 p-5 rounded-2xl mb-6 overflow-hidden">
      <p class="text-gray-500 text-sm">Payout Note</p>
      <p class="text-gray-300 mt-2 break-words">${p.note || p.admin_note || "No payout note yet."}</p>
    </div>

    <div class="grid md:grid-cols-4 gap-3">
      ${payoutActions}
    </div>
  </div>`;
}

function payoutReview(id){
  const p = payouts.find(x=>String(x.id)===String(id));
  if(!p){alert("Payout not found");return;}

  const trader = traders.find(t =>
    String(t.id||"")===String(p.trader_id||"") ||
    String(t.email||"").toLowerCase()===String(p.email||"").toLowerCase() ||
    String(t.phone||"").toLowerCase()===String(p.phone||"").toLowerCase()
  );

  alert(
    `Payout Review\\n\\nTrader: ${p.trader_name || "Unknown"}\\nRequested: ${money(p.amount)}\\nBank: ${p.bank_name || "N/A"}\\nAccount: ${p.account_number || "N/A"}\\nTrader Profit: ${money(trader?.profit || 0)}\\nTrader Status: ${trader?.status || "Unknown"}\\n\\nCheck MT5, rules and identity before payment.`
  );
}

async function approvePayout(id){
  const admin_note = prompt("Approval note:", "Payout approved after admin review.") || "";
  try{
    const data = await postJSON(`${API_URL}/approve_payout`, {id, admin_note, admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"});
    await logAdminAudit("payouts","payout_approved",`Payout approved: ${id}`,id);
    alert(data.message || "Payout approved.");
    loadData();
  }catch(e){
    alert(e.message || "Payout approval failed");
  }
}

async function markPayoutPaid(id){
  const admin_note = prompt("Payment note:", "Payout marked as paid.") || "";
  try{
    const data = await postJSON(`${API_URL}/mark_payout_paid`, {id, admin_note, admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"});
    await logAdminAudit("payouts","payout_paid",`Payout marked paid: ${id}`,id);
    alert(data.message || "Payout marked paid.");
    loadData();
  }catch(e){
    alert(e.message || "Could not mark payout paid");
  }
}

async function rejectPayout(id){
  const admin_note = prompt("Reason for rejection:", "Payout rejected after admin review.") || "";
  try{
    const data = await postJSON(`${API_URL}/reject_payout`, {id, admin_note, admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin"});
    await logAdminAudit("payouts","payout_rejected",`Payout rejected: ${id}`,id);
    alert(data.message || "Payout rejected.");
    loadData();
  }catch(e){
    alert(e.message || "Payout rejection failed");
  }
}



function supportModule(){
  const s = q();
  const list = tickets.filter(t =>
    (t.trader_name||"").toLowerCase().includes(s) ||
    (t.email||"").toLowerCase().includes(s) ||
    (t.status||"").toLowerCase().includes(s) ||
    (t.priority||"").toLowerCase().includes(s) ||
    (t.subject||"").toLowerCase().includes(s)
  );

  document.getElementById("content").innerHTML = `
  <div class="grid md:grid-cols-4 gap-4 mb-6">
    ${stat("Open",tickets.filter(t=>t.status==="open").length)}
    ${stat("Replied",tickets.filter(t=>t.status==="replied").length)}
    ${stat("Closed",tickets.filter(t=>t.status==="closed").length)}
    ${stat("Urgent",tickets.filter(t=>t.priority==="urgent").length)}
  </div>
  <div class="grid gap-5">${list.map(ticketCard).join("") || empty("No support tickets yet.")}</div>`;
}

function ticketCard(t){
  return `
  <div class="vault p-6 rounded-3xl">
    <div class="flex flex-wrap justify-between gap-4 mb-5">
      <div>
        <h3 class="text-2xl font-bold">${t.subject||"Support Ticket"}</h3>
        <p class="text-gray-400">${t.trader_name||""} • ${t.email||""} • ${t.phone||""}</p>
        <p class="text-gray-600 text-xs">Created: ${formatDate(t.created_at)}</p>
      </div>
      <div class="flex gap-2">
        <span class="badge">${t.status||"open"}</span>
        <span class="badge">${t.priority||"normal"}</span>
      </div>
    </div>

    <div class="card2 p-4 rounded-2xl mb-5">
      <p class="text-gray-500 text-sm">Trader Message</p>
      <p>${t.message||""}</p>
    </div>

    <div class="card2 p-4 rounded-2xl mb-5">
      <p class="text-gray-500 text-sm">Current Admin Reply</p>
      <p class="gold">${t.admin_reply || "No reply yet"}</p>
      <small class="text-gray-500">Replied: ${formatDate(t.replied_at)}</small>
    </div>

    <textarea id="reply-${t.id}" rows="4" placeholder="Write admin reply..." class="mb-4"></textarea>

    <div class="flex flex-wrap gap-3">
      <button class="btn btn-gold" onclick="replyTicket('${t.id}')">Send Reply</button>
      <button class="btn btn-green" onclick="closeTicket('${t.id}')">Close Ticket</button>
    </div>
  </div>`;
}

async function replyTicket(id){
  const admin_reply = document.getElementById(`reply-${id}`).value.trim();
  if(!admin_reply){alert("Write reply first.");return;}
  const res = await fetch(`${API_URL}/reply_support_ticket`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,admin_reply})
  });
  const data = await res.json();
  if(data.success){alert("Reply sent.");loadData();} else alert(data.error || "Reply failed");
}

async function closeTicket(id){
  if(!confirm("Close this ticket?")) return;
  const res = await fetch(`${API_URL}/close_support_ticket`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id})
  });
  const data = await res.json();
  if(data.success){alert("Ticket closed.");loadData();} else alert(data.error || "Close failed");
}

/* ANNOUNCEMENTS */
function announcementsModule(){
  document.getElementById("content").innerHTML = `
  <div class="grid lg:grid-cols-3 gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-bold gold mb-5">Create Announcement</h3>
      <input id="ann_title" placeholder="Announcement title" class="mb-4">
      <textarea id="ann_message" rows="6" placeholder="Announcement message" class="mb-4"></textarea>
      <select id="ann_type" class="mb-4">
        <option value="public_notice">Public Notice</option>
        <option value="trader_notice">Trader Notice</option>
        <option value="marketing">Marketing</option>
        <option value="referral">Referral</option>
        <option value="maintenance">Maintenance</option>
        <option value="payout_update">Payout Update</option>
        <option value="competition">Competition</option>
      </select>
      <select id="ann_landing" class="mb-4">
        <option value="true">Show on Landing Page</option>
        <option value="false">Do NOT show on Landing Page</option>
      </select>
      <select id="ann_dashboard" class="mb-4">
        <option value="true">Show on Trader Dashboard</option>
        <option value="false">Do NOT show on Trader Dashboard</option>
      </select>
      <button class="btn btn-gold w-full" onclick="createAnnouncement()">Publish Announcement</button>
    </div>

    <div class="lg:col-span-2">
      <div class="grid gap-5">
        ${announcements.map(announcementCard).join("") || empty("No active announcements yet.")}
      </div>
    </div>
  </div>`;
}

function announcementCard(a){
  return `
  <div class="vault p-6 rounded-3xl">
    <div class="flex flex-wrap justify-between gap-4 mb-4">
      <div>
        <h3 class="text-2xl font-bold gold">${a.title}</h3>
        <p class="text-gray-500 text-sm">${a.type} • ${formatDate(a.created_at)}</p>
      </div>
      <span class="badge">${a.status||"active"}</span>
    </div>
    <p class="text-gray-300 mb-4">${a.message}</p>
    <div class="flex flex-wrap gap-3 mb-4">
      <span class="badge">Landing: ${a.show_on_landing ? "Yes" : "No"}</span>
      <span class="badge">Trader Dashboard: ${a.show_on_dashboard ? "Yes" : "No"}</span>
    </div>
    <button class="btn btn-red" onclick="disableAnnouncement('${a.id}')">Disable</button>
  </div>`;
}

async function createAnnouncement(){
  const title = document.getElementById("ann_title").value.trim();
  const message = document.getElementById("ann_message").value.trim();
  const type = document.getElementById("ann_type").value;
  const show_on_landing = document.getElementById("ann_landing").value === "true";
  const show_on_dashboard = document.getElementById("ann_dashboard").value === "true";

  if(!title || !message){alert("Title and message required.");return;}

  const res = await fetch(`${API_URL}/create_announcement`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({title,message,type,show_on_landing,show_on_dashboard,created_by:"admin"})
  });
  const data = await res.json();
  if(data.success){alert("Announcement published.");loadData();}
  else alert(data.error || "Announcement failed");
}

async function disableAnnouncement(id){
  if(!confirm("Disable this announcement?")) return;
  const res = await fetch(`${API_URL}/disable_announcement`,{
    method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id})
  });
  const data = await res.json();
  if(data.success){alert("Announcement disabled.");loadData();}
  else alert(data.error || "Disable failed");
}

/* TIMELINE / REVENUE / DATABASE */
function timelineModule(){
  const list = filteredTraders();
  document.getElementById("content").innerHTML = `
  <div class="grid gap-5">
    ${list.map(t=>`
    <div class="vault p-6 rounded-3xl">
      <div class="flex flex-wrap justify-between gap-4 mb-5">
        <div>
          <h3 class="text-2xl font-bold">${t.name||"Trader"}</h3>
          <p class="text-gray-400">${t.account_reference||"No reference"} • ${t.selected_plan||""}</p>
        </div>
        <span class="badge">${t.status||"processing"}</span>
      </div>
      <div class="grid md:grid-cols-3 lg:grid-cols-6 gap-4">
        ${timelineBox("Joined",formatDate(t.created_at))}
        ${timelineBox("Approved",formatDate(t.approved_at))}
        ${timelineBox("Started",formatDate(t.challenge_started_at))}
        ${timelineBox("Last Login",formatDate(t.last_login_at))}
        ${timelineBox("Funded",formatDate(t.funded_at))}
        ${timelineBox("Days Left",t.trading_days_left ?? 30)}
      </div>
    </div>`).join("")}
  </div>`;
}

function timelineBox(label,value){
  return `<div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-sm">${label}</p><b class="gold">${value}</b></div>`;
}


const LEAD_STATUS_KEY = "nairapips_lead_statuses";

function revenueIsSuperAdmin(){return !currentAdmin || currentAdmin.role === "super_admin" || currentAdmin.permissions === "all";}
function normalizeRevenueDateToIso(value){
  const v = String(value || "").trim();
  if(!v) return "";
  if(/^\d{4}-\d{2}-\d{2}/.test(v)) return v.slice(0,10);
  const parts = v.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if(parts){
    const day = parts[1].padStart(2,"0");
    const month = parts[2].padStart(2,"0");
    return `${parts[3]}-${month}-${day}`;
  }
  const d = new Date(v);
  return isNaN(d.getTime()) ? "" : d.toISOString().slice(0,10);
}
function formatRevenueDisplayDate(value){
  const iso = normalizeRevenueDateToIso(value);
  if(!iso) return "Not set";
  const [y,m,d] = iso.split("-");
  return `${d}/${m}/${y}`;
}
function applyBusinessSettingsFromServer(){
  const settings = businessSettingsCache || {};
  businessSettingsCache = {
    revenue_launch_date: normalizeRevenueDateToIso(settings.revenue_launch_date || settings.launch_date || ""),
    production_mode: settings.production_mode === "live" ? "live" : "test"
  };
}
async function refreshBusinessSettingsFromBackend(){
  const res = await getObject(`${API_URL}/business_settings`, {});
  const settings = res.data || res || {};
  businessSettingsCache = {
    revenue_launch_date: normalizeRevenueDateToIso(settings.revenue_launch_date || settings.launch_date || ""),
    production_mode: settings.production_mode === "live" ? "live" : "test"
  };
  return businessSettingsCache;
}
let revenueUiState = {saving:"", message:"", messageType:""};
let latestRevenueResetBatch = [];
function setRevenueMessage(message,type="success"){revenueUiState.message=message||""; revenueUiState.messageType=type;}
function productionMode(){return (businessSettingsCache?.production_mode === "live" ? "live" : "test");}
function isLiveMode(){return productionMode() === "live";}
async function setProductionMode(mode){
  if(!revenueIsSuperAdmin()){setRevenueMessage("Only super admin can switch production mode.","error"); revenueModule(); return;}
  const safeMode = mode === "live" ? "live" : "test";
  revenueUiState.saving = "mode";
  setRevenueMessage(`Saving ${safeMode.toUpperCase()} MODE...`,"info");
  revenueModule();
  try{
    await postJSON(`${API_URL}/business_settings`,{production_mode:safeMode, revenue_launch_date:revenueLaunchDateValue(), admin_name:currentAdmin?.name||currentAdmin?.username||"admin", admin_username:currentAdmin?.username||"admin"});
    await refreshBusinessSettingsFromBackend();
  }catch(e){
    revenueUiState.saving = "";
    setRevenueMessage("Production mode could not save to Supabase: " + e.message,"error");
    revenueModule();
    return;
  }
  revenueUiState.saving = "";
  setRevenueMessage(`${safeMode.toUpperCase()} MODE saved. Revenue calculations refreshed.`,"success");
  logAdminAudit("revenue", "production_mode", `Production mode set to ${safeMode.toUpperCase()}`, "production_mode");
  revenueModule();
}
function revenueLaunchDateValue(){return businessSettingsCache?.revenue_launch_date || "";}
function revenueLaunchDate(){const v=revenueLaunchDateValue(); if(!v) return null; const d=new Date(v+"T00:00:00"); return isNaN(d.getTime())?null:d;}
function revenueLastReset(){return latestRevenueResetBatch || [];}
function revenueSaveLastReset(rows){latestRevenueResetBatch = rows || [];}
function revenueHiddenIds(){return {};}
function revenueSaveHiddenIds(data){}
function revenueRecordId(type,row){return `${type}:${row?.id || row?.account_reference || row?.created_at || row?.requested_at || row?.approved_at || "unknown"}`;}
function testRecords(){return {};}
function saveTestRecords(data){}
function testRecordKey(type,row){return revenueRecordId(type,row);}
function isMarkedTest(type,row){return row?.mark_as_test===true || String(row?.mark_as_test||"").toLowerCase()==="true";}
function revenueIsExcluded(type,row){
  if(!isLiveMode()) return false;
  if(row?.excluded_from_revenue===true || String(row?.excluded_from_revenue||"").toLowerCase()==="true") return true;
  if(isMarkedTest(type,row)) return true;
  return false;
}
function revenueAfterLaunch(date){const launch=revenueLaunchDate(); if(!launch) return true; return !!date && date >= launch;}
async function setRecordTestFlag(type,row,marked=true){
  if(!revenueIsSuperAdmin()) throw new Error("Only super admin can change revenue flags.");
  const tableMap={trader:"traders",purchase:"challenge_purchases",payout:"payouts",payment:"payments",referral:"referrals"};
  if(!row?.id) throw new Error("Record id is required to update revenue flags.");
  await postJSON(`${API_URL}/mark_record_test`,{table:tableMap[type]||type,id:row.id,mark_as_test:marked,excluded_from_revenue:marked,admin_name:currentAdmin?.name||currentAdmin?.username||"admin"});
  row.mark_as_test = marked;
  row.excluded_from_revenue = marked;
}
async function setRevenueLaunchDate(){
  if(!revenueIsSuperAdmin()){setRevenueMessage("Only super admin can change revenue cleanup settings.","error"); revenueModule(); return;}
  const value=normalizeRevenueDateToIso(document.getElementById("revenueLaunchDate")?.value || "");
  revenueUiState.saving = "launch";
  setRevenueMessage(value ? `Saving launch date ${formatRevenueDisplayDate(value)}...` : "Clearing launch date...","info");
  revenueModule();
  try{
    await postJSON(`${API_URL}/business_settings`,{revenue_launch_date:value, production_mode:productionMode(), admin_name:currentAdmin?.name||currentAdmin?.username||"admin", admin_username:currentAdmin?.username||"admin"});
    await refreshBusinessSettingsFromBackend();
  }catch(e){
    revenueUiState.saving = "";
    setRevenueMessage("Launch date could not save to Supabase: " + e.message,"error");
    revenueModule();
    return;
  }
  revenueUiState.saving = "";
  setRevenueMessage(value ? `Launch date saved: ${formatRevenueDisplayDate(value)}. Revenue calculations refreshed.` : "Launch date cleared. Revenue calculations refreshed.","success");
  logAdminAudit("revenue","launch_date_set",value?`Business launch date set to ${value}`:"Business launch date cleared","business_launch_date");
  revenueModule();
}
async function clearRevenueLaunchDate(){
  if(!revenueIsSuperAdmin()){setRevenueMessage("Only super admin can change revenue cleanup settings.","error"); revenueModule(); return;}
  revenueUiState.saving = "launch";
  setRevenueMessage("Clearing launch date...","info");
  revenueModule();
  try{
    await postJSON(`${API_URL}/business_settings`,{revenue_launch_date:"", production_mode:productionMode(), admin_name:currentAdmin?.name||currentAdmin?.username||"admin", admin_username:currentAdmin?.username||"admin"});
    await refreshBusinessSettingsFromBackend();
  }catch(e){
    revenueUiState.saving = "";
    setRevenueMessage("Launch date could not clear in Supabase: " + e.message,"error");
    revenueModule();
    return;
  }
  revenueUiState.saving = "";
  setRevenueMessage("Launch date cleared in Supabase. Revenue calculations refreshed.","success");
  logAdminAudit("revenue","launch_date_cleared","Business launch date cleared","business_launch_date");
  revenueModule();
}

window.nairaPipsRevenueModeClick = async function(mode){
  await setProductionMode(mode);
};
window.nairaPipsSaveLaunchDateClick = async function(){
  await setRevenueLaunchDate();
};
window.nairaPipsClearLaunchDateClick = async function(){
  await clearRevenueLaunchDate();
};
function adminAuditQueue(){try{return JSON.parse(localStorage.getItem("nairapips_pending_audit_logs")||"[]")||[];}catch(e){return [];}}
function saveAdminAuditQueue(rows){localStorage.setItem("nairapips_pending_audit_logs",JSON.stringify(rows||[]));}
async function logAdminAudit(module,action,details,recordAffected=""){
  const payload={module,action,details,record_affected:recordAffected,admin_name:currentAdmin?.name||currentAdmin?.username||"admin",admin_username:currentAdmin?.username||"admin",admin_role:currentAdmin?.role||"admin",created_at:new Date().toISOString()};
  try{await postJSON(`${API_URL}/audit_event`,payload);}catch(e){const q=adminAuditQueue(); q.push(payload); saveAdminAuditQueue(q.slice(-100)); console.warn("Audit log queued locally until backend audit route is available:", e.message);}
}

function csvEscape(v){return `"${String(v??"").replace(/"/g,'""')}"`;}
function downloadCSV(name, headers, rows){
  const body=[headers.map(csvEscape).join(","),...rows.map(r=>headers.map(h=>csvEscape(r[h])).join(","))].join("\n");
  const blob=new Blob([body],{type:"text/csv;charset=utf-8"});
  const url=URL.createObjectURL(blob); const a=document.createElement("a");
  a.href=url; a.download=name; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
}
function exportTradersCSV(){downloadCSV(`nairapips-traders-${new Date().toISOString().slice(0,10)}.csv`,["name","email","phone","status","phase","payment_status","account_size","mt5_login","created_at"],traders);}
function exportPaymentsCSV(){downloadCSV(`nairapips-payments-${new Date().toISOString().slice(0,10)}.csv`,["trader_name","email","phone","plan_name","account_size","fee","payment_status","status","created_at","approved_at"],purchases);}
function exportPayoutsCSV(){downloadCSV(`nairapips-payouts-${new Date().toISOString().slice(0,10)}.csv`,["trader_name","email","phone","amount","status","requested_at","approved_at","paid_at"],payouts);}
function exportRevenueCSV(){
  const rows=[
    {metric:"Gross Revenue",value:window.__revenueSnapshot?.salesAll||0},
    {metric:"Approved Payouts",value:window.__revenueSnapshot?.approvedLiability||0},
    {metric:"Pending Payouts",value:window.__revenueSnapshot?.pendingLiability||0},
    {metric:"Net Revenue",value:window.__revenueSnapshot?.netRevenue||0},
    {metric:"Active Traders",value:window.__revenueSnapshot?.activeTraders||0},
    {metric:"Funded Traders",value:window.__revenueSnapshot?.fundedTraders||0},
    {metric:"Conversion Rate",value:window.__revenueSnapshot?.conversionRate||"0%"}
  ];
  downloadCSV(`nairapips-revenue-${new Date().toISOString().slice(0,10)}.csv`,["metric","value"],rows);
}
function exportLeadsCSV(){const rows=traders.filter(isUnconvertedLead).map(t=>({...t,lead_status:leadStatusValue(t)})); downloadCSV(`nairapips-leads-${new Date().toISOString().slice(0,10)}.csv`,["name","email","phone","status","payment_status","lead_status","created_at"],rows);}

function leadStatuses(){try{return JSON.parse(localStorage.getItem(LEAD_STATUS_KEY)||"{}")||{};}catch(e){return {};}}
function saveLeadStatuses(data){localStorage.setItem(LEAD_STATUS_KEY,JSON.stringify(data||{}));}
function leadStatusValue(t){const v=leadStatuses()[String(t.id)]; return (v && v.status) || t.lead_status || "New";}
async function setLeadStatus(id,status){
  try{
    await postJSON(`${API_URL}/update_status`,{id,lead_status:status,follow_up_at:status==="Follow Up Tomorrow"?new Date(Date.now()+86400000).toISOString():null,admin_note:`Lead status: ${status}`});
  }catch(e){
    alert(e.message || "Lead status could not be saved. Check backend lead_status/follow_up_at columns.");
    return;
  }
  const data=leadStatuses(); data[String(id)]={status,updated_at:new Date().toISOString()}; saveLeadStatuses(data);
  logAdminAudit("leads","lead_status_update",`Lead ${id} set to ${status}`,id);
  leadsModule();
}
async function hideTestRevenue(){
  if(!revenueIsSuperAdmin()){setRevenueMessage("Only super admin can hide revenue records.","error"); revenueModule(); return;}
  const typed=prompt('Type RESET REVENUE to hide test revenue from reporting. This does not delete customer data.');
  if(typed!=="RESET REVENUE") return;
  revenueUiState.saving = "hide";
  setRevenueMessage("Hiding test revenue records in Supabase...","info");
  revenueModule();
  const launch=revenueLaunchDate(); let count=0;
  const latestReset=[];
  const shouldHidePurchase=p=>{const d=new Date(p.approved_at||p.created_at||""); const before=launch&&!isNaN(d.getTime())&&d<launch; const text=`${p.trader_name||""} ${p.email||""} ${p.phone||""} ${p.plan_name||""} ${p.admin_note||""}`.toLowerCase(); return before || /\b(test|demo|dummy|sample)\b/.test(text);};
  const shouldHidePayout=p=>{const d=new Date(p.paid_at||p.approved_at||p.requested_at||p.created_at||""); const before=launch&&!isNaN(d.getTime())&&d<launch; const text=`${p.trader_name||""} ${p.email||""} ${p.phone||""} ${p.note||""} ${p.admin_note||""}`.toLowerCase(); return before || /\b(test|demo|dummy|sample)\b/.test(text);};
  try{
    for(const p of purchases){if(shouldHidePurchase(p)){await setRecordTestFlag("purchase",p,true); latestReset.push({type:"purchase",id:p.id}); count++;}}
    for(const p of payouts){if(shouldHidePayout(p)){await setRecordTestFlag("payout",p,true); latestReset.push({type:"payout",id:p.id}); count++;}}
  }catch(e){
    revenueUiState.saving = "";
    setRevenueMessage("Hide Test Revenue failed: " + e.message,"error");
    revenueModule();
    return;
  }
  revenueSaveLastReset(latestReset);
  logAdminAudit("revenue","revenue_reset",`${count} test revenue record(s) hidden from reporting only. Customer data was not deleted.`,"revenue_reporting");
  revenueUiState.saving = "";
  setRevenueMessage(`${count} record(s) hidden from revenue reporting. No customer data was deleted.`,"success");
  revenueModule();
}
async function restoreHiddenRevenue(){
  if(!revenueIsSuperAdmin()){setRevenueMessage("Only super admin can restore hidden revenue records.","error"); revenueModule(); return;}
  const latestReset = revenueLastReset();
  if(!latestReset.length){setRevenueMessage("There is no latest Revenue Reset batch to restore.","error"); revenueModule(); return;}
  const typed=prompt('Type RESET REVENUE to restore only the latest Revenue Reset batch.');
  if(typed!=="RESET REVENUE") return;
  revenueUiState.saving = "restore";
  setRevenueMessage("Restoring latest Revenue Reset batch...","info");
  revenueModule();
  let count=0;
  try{
    for(const item of latestReset){
      const row = item.type==="purchase"
        ? purchases.find(p=>String(p.id)===String(item.id))
        : payouts.find(p=>String(p.id)===String(item.id));
      if(row){await setRecordTestFlag(item.type,row,false); count++;}
    }
  }catch(e){
    revenueUiState.saving = "";
    setRevenueMessage("Restore Hidden Revenue failed: " + e.message,"error");
    revenueModule();
    return;
  }
  revenueSaveLastReset([]);
  logAdminAudit("revenue","revenue_restore","Hidden revenue records restored to reporting. Customer data was not changed.","revenue_reporting");
  revenueUiState.saving = "";
  setRevenueMessage(`${count} record(s) restored from the latest Revenue Reset batch.`,"success");
  revenueModule();
}

async function revenueModule(){
  const content = document.getElementById("content");
  if(content){
    content.innerHTML = `
    <div class="vault p-10 rounded-3xl flex items-center gap-5">
      <div class="loader"></div>
      <div>
        <h3 class="text-2xl font-black gold">Loading backend revenue summary...</h3>
        <p class="text-gray-400">Revenue is calculated directly from Supabase on the backend.</p>
      </div>
    </div>`;
  }

  let summary;
  try{
    const res = await getObject(`${API_URL}/revenue_summary`, {});
    summary = res.data || res || {};
  }catch(e){
    if(content){
      content.innerHTML = `<div class="vault p-8 rounded-3xl"><h3 class="text-3xl font-black text-red-400">Revenue summary failed</h3><p class="text-gray-400 mt-2">${e.message || "Could not load backend revenue summary."}</p></div>`;
    }
    return;
  }

  businessSettingsCache = {
    production_mode: summary.production_mode_used === "live" ? "live" : "test",
    revenue_launch_date: normalizeRevenueDateToIso(summary.launch_date_used || "")
  };

  const launchValue = businessSettingsCache.revenue_launch_date || "";
  const latestResetCount = revenueLastReset().length;
  const modeLabel = summary.production_mode_used === "live" ? "LIVE MODE" : "TEST MODE";
  const modeSubtext = summary.production_mode_used === "live"
    ? "Production-only: test/excluded records are hidden."
    : "Test-inclusive: backend keeps test records visible while still applying the launch date.";
  const saving = revenueUiState.saving;
  const msgClass = revenueUiState.messageType === "error" ? "text-red-400 border-red-900/40" : revenueUiState.messageType === "info" ? "text-yellow-300 border-yellow-900/40" : "text-green-400 border-green-900/40";

  const weeklySales = Number(summary.weekly_sales || 0);
  const monthlySales = Number(summary.monthly_sales || 0);
  const yearlySales = Number(summary.yearly_sales || 0);
  const weeklyPayouts = Number(summary.weekly_payouts || 0);
  const monthlyPayouts = Number(summary.monthly_payouts || 0);
  const yearlyPayouts = Number(summary.yearly_payouts || 0);
  const weeklyRevenue = Number(summary.weekly_net || 0);
  const monthlyRevenue = Number(summary.monthly_net || 0);
  const yearlyRevenue = Number(summary.yearly_net || 0);
  const salesAll = Number(summary.gross_revenue || 0);
  const paidAll = Number(summary.paid_payouts || 0);
  const approvedLiability = Number(summary.approved_payouts || 0);
  const pendingLiability = Number(summary.pending_payouts || 0);
  const pendingSales = Number(summary.pending_sales || 0);
  const rejectedSales = Number(summary.rejected_sales || 0);
  const netRevenue = Number(summary.net_revenue || 0);
  const activeTraders = Number(summary.active_traders || 0);
  const fundedTraders = Number(summary.funded_traders || 0);
  const conversionRate = summary.conversion_rate || "0%";
  const excludedRevenueCount = Number(summary.excluded_purchases || 0) + Number(summary.excluded_payouts || 0);
  const planRows = summary.plan_rows || [];
  const monthRows = summary.month_rows || [];
  const countedPurchases = Number(summary.counted_purchases || 0);

  window.__revenueSnapshot = {salesAll, approvedLiability, pendingLiability, netRevenue, activeTraders, fundedTraders, conversionRate};
  const debugSummary = {
    totalPurchasesLoaded:Number(summary.total_purchases_loaded || 0),
    totalPayoutsLoaded:Number(summary.total_payouts_loaded || 0),
    purchasesCounted:countedPurchases,
    payoutsCounted:Number(summary.counted_payouts || 0),
    excludedPurchases:Number(summary.excluded_purchases || 0),
    supabaseProductionMode:summary.production_mode_used || "Not loaded",
    supabaseRevenueLaunchDate:summary.launch_date_used || "Not set",
    launchDateUsed:summary.launch_date_used || "Not set",
    launchDateDisplay:formatRevenueDisplayDate(summary.launch_date_used),
    currentMode:modeLabel
  };

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">Revenue Intelligence</span>
        <h3 class="text-4xl font-black gold mt-3">Sales, Payouts & Net Revenue</h3>
        <p class="text-gray-400 mt-2">
          Weekly, monthly and yearly command view for NairaPips challenge sales and payout pressure.
        </p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[250px]">
        <p class="text-gray-500 text-sm">Total Net Position</p>
        <h2 class="text-4xl font-black ${netRevenue >= 0 ? "text-green-400" : "text-red-400"}">${money(netRevenue)}</h2>
      </div>
    </div>
  </div>

  ${revenueIsSuperAdmin() ? `
  <div class="vault p-6 rounded-3xl mb-8 staff-danger-zone">
    <div class="flex flex-wrap justify-between gap-5 items-start mb-5">
      <div>
        <span class="badge text-red-400">PRODUCTION REVENUE CONTROL</span>
        <h3 class="text-3xl font-black gold mt-3">Revenue Launch Mode</h3>
        <p class="text-gray-300 mt-2">This does not delete customer data. It only removes test data from revenue reporting.</p>
        <p class="text-gray-500 text-sm mt-2">LIVE MODE hides test data and counts only production records after the launch date.</p>
      </div>
      <div class="card2 p-4 rounded-2xl min-w-[230px]">
        <p class="text-gray-500 text-sm">Hidden / Filtered Records</p>
        <h3 class="text-3xl font-black gold">${excludedRevenueCount}</h3>
        <p class="text-gray-500 text-xs mt-1">${latestResetCount} record(s) in latest reset batch (local restore list)</p>
      </div>
    </div>
    ${revenueUiState.message ? `<div class="card2 p-4 rounded-2xl mb-5 border ${msgClass}">${revenueUiState.message}</div>` : ""}
    <div class="production-warning p-4 rounded-2xl mb-5">
      <div class="flex flex-wrap justify-between gap-4 items-center">
        <div><h4 class="text-xl font-black gold">Production Mode</h4><p class="text-gray-400 text-sm">${modeSubtext}</p></div>
        <div>
          <div class="production-toggle">
            <button type="button" onclick="window.nairaPipsRevenueModeClick('test')" class="btn ${productionMode()==='test'?'btn-gold':'btn-dark'}" ${saving==="mode"?"disabled":""}>${saving==="mode" && productionMode()==='test' ? "Saving..." : "TEST MODE"}</button>
            <button type="button" onclick="window.nairaPipsRevenueModeClick('live')" class="btn ${productionMode()==='live'?'btn-gold':'btn-dark'}" ${saving==="mode"?"disabled":""}>${saving==="mode" && productionMode()==='live' ? "Saving..." : "LIVE MODE"}</button>
          </div>
          <p class="text-gray-400 text-sm mt-3 text-center">Current Mode: <b class="gold">${modeLabel}</b></p>
        </div>
      </div>
    </div>
    <div class="grid lg:grid-cols-[1fr_auto_auto] gap-3 items-end">
      <div><label class="text-gray-400 text-sm">Business Launch Date</label><input id="revenueLaunchDate" type="date" value="${launchValue}"><p class="text-gray-500 text-xs mt-2">Revenue reports count records on or after this date. Display: ${formatRevenueDisplayDate(launchValue)}</p></div>
      <button type="button" onclick="window.nairaPipsSaveLaunchDateClick()" class="btn btn-gold" ${saving==="launch"?"disabled":""}>${saving==="launch"?"Saving...":"Save Launch Date"}</button>
      <button type="button" onclick="window.nairaPipsClearLaunchDateClick()" class="btn btn-dark" ${saving==="launch"?"disabled":""}>${saving==="launch"?"Saving...":"Clear Date"}</button>
    </div>
    <div class="flex flex-wrap gap-3 mt-5">
      <button onclick="hideTestRevenue()" class="btn btn-red" ${saving==="hide"?"disabled":""}>${saving==="hide"?"Hiding...":"Hide Test Revenue"}</button>
      <button onclick="restoreHiddenRevenue()" class="btn btn-green" ${saving==="restore"?"disabled":""}>${saving==="restore"?"Restoring...":"Restore Hidden Revenue"}</button>
      <button onclick="exportTradersCSV()" class="btn btn-dark">Export Traders CSV</button>
      <button onclick="exportPaymentsCSV()" class="btn btn-dark">Export Payments CSV</button>
      <button onclick="exportPayoutsCSV()" class="btn btn-dark">Export Payouts CSV</button>
      <button onclick="exportRevenueCSV()" class="btn btn-dark">Export Revenue CSV</button>
      <button onclick="exportLeadsCSV()" class="btn btn-dark">Export Leads CSV</button>
    </div>
  </div>` : ""}

  <div class="grid md:grid-cols-3 gap-5 mb-8">
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Weekly Sales</p>
      <h3 class="text-4xl font-black gold">${money(weeklySales)}</h3>
      <p class="text-gray-500 mt-2">Approved challenge fees this week</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Weekly Payouts</p>
      <h3 class="text-4xl font-black text-red-400">${money(weeklyPayouts)}</h3>
      <p class="text-gray-500 mt-2">Paid payouts this week</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Weekly Revenue</p>
      <h3 class="text-4xl font-black ${weeklyRevenue >= 0 ? "text-green-400" : "text-red-400"}">${money(weeklyRevenue)}</h3>
      <p class="text-gray-500 mt-2">Sales minus paid payouts</p>
    </div>
  </div>

  <div class="grid md:grid-cols-3 gap-5 mb-8">
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Monthly Sales</p>
      <h3 class="text-4xl font-black gold">${money(monthlySales)}</h3>
      <p class="text-gray-500 mt-2">Approved challenge fees this month</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Monthly Payouts</p>
      <h3 class="text-4xl font-black text-red-400">${money(monthlyPayouts)}</h3>
      <p class="text-gray-500 mt-2">Paid payouts this month</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Monthly Revenue</p>
      <h3 class="text-4xl font-black ${monthlyRevenue >= 0 ? "text-green-400" : "text-red-400"}">${money(monthlyRevenue)}</h3>
      <p class="text-gray-500 mt-2">Sales minus paid payouts</p>
    </div>
  </div>

  <div class="grid md:grid-cols-3 gap-5 mb-8">
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Yearly Sales</p>
      <h3 class="text-4xl font-black gold">${money(yearlySales)}</h3>
      <p class="text-gray-500 mt-2">Approved challenge fees this year</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Yearly Payouts</p>
      <h3 class="text-4xl font-black text-red-400">${money(yearlyPayouts)}</h3>
      <p class="text-gray-500 mt-2">Paid payouts this year</p>
    </div>
    <div class="vault p-6 rounded-3xl">
      <p class="text-gray-400 text-sm">Yearly Revenue</p>
      <h3 class="text-4xl font-black ${yearlyRevenue >= 0 ? "text-green-400" : "text-red-400"}">${money(yearlyRevenue)}</h3>
      <p class="text-gray-500 mt-2">Sales minus paid payouts</p>
    </div>
  </div>

  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Gross Revenue",money(salesAll),"Approved challenge fees")}
    ${stat("Approved Payouts",money(approvedLiability),"Approved but not paid")}
    ${stat("Pending Payouts",money(pendingLiability),"Requested but not approved")}
    ${stat("Net Revenue",money(netRevenue),"Gross revenue minus paid payouts")}
    ${stat("Active Traders",activeTraders,"Production active accounts")}
    ${stat("Funded Traders",fundedTraders,"Production funded/live accounts")}
    ${stat("Conversion Rate",conversionRate,"Approved purchases / traders")}
    ${stat("Mode",productionMode().toUpperCase(),isLiveMode()?"Test data hidden":"Testing records visible")}
    ${stat("Pending Sales",money(pendingSales),"Awaiting verification")}
    ${stat("Rejected Sales",money(rejectedSales),"Declined purchases")}
    ${stat("Paid Payouts",money(paidAll),"Total cash outflow")}
    ${stat("Approved Purchases",countedPurchases,"Backend-counted paid challenge buyers")}
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <h3 class="text-2xl font-black gold mb-4">Revenue Debug Summary</h3>
    <div class="grid md:grid-cols-4 lg:grid-cols-9 gap-3">
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Purchases Loaded</p><b>${debugSummary.totalPurchasesLoaded}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Payouts Loaded</p><b>${debugSummary.totalPayoutsLoaded}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Purchases Counted</p><b>${debugSummary.purchasesCounted}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Payouts Counted</p><b>${debugSummary.payoutsCounted}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Excluded Purchases</p><b>${debugSummary.excludedPurchases}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Supabase Mode</p><b>${debugSummary.supabaseProductionMode}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Supabase Launch</p><b>${debugSummary.supabaseRevenueLaunchDate}</b></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Launch Date Used</p><b>${debugSummary.launchDateUsed}</b><p class="text-gray-500 text-xs mt-1">${debugSummary.launchDateDisplay}</p></div>
      <div class="card2 p-4 rounded-2xl"><p class="text-gray-500 text-xs">Current Mode</p><b>${debugSummary.currentMode}</b></div>
    </div>
  </div>

  <div class="grid lg:grid-cols-2 gap-6">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-3xl font-black gold mb-5">Revenue by Plan</h3>
      <div class="grid gap-4">
        ${
          planRows.map(row=>`
          <div class="card2 p-5 rounded-2xl">
            <div class="flex justify-between gap-4">
              <div>
                <h3 class="text-xl font-black">${row.name}</h3>
                <p class="text-gray-500">${money(row.account_size)} account • ${row.count} sales</p>
              </div>
              <h3 class="text-2xl font-black gold">${money(row.fee)}</h3>
            </div>
          </div>
          `).join("") || `<p class="text-gray-400">No approved sales yet.</p>`
        }
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-3xl font-black gold mb-5">Monthly Sales History</h3>
      <div class="grid gap-4">
        ${
          monthRows.map(row=>`
          <div class="card2 p-5 rounded-2xl">
            <div class="flex justify-between gap-4">
              <div>
                <h3 class="text-xl font-black">${row.month}</h3>
                <p class="text-gray-500">${row.count} approved challenge sales</p>
              </div>
              <h3 class="text-2xl font-black gold">${money(row.sales)}</h3>
            </div>
          </div>
          `).join("") || `<p class="text-gray-400">No monthly sales history yet.</p>`
        }
      </div>
    </div>
  </div>`;
}



function fundedModule(){
  const s = q();

  const fundedTraders = traders.filter(t=>{
    const status = String(t.status||"").toLowerCase();
    const phase = String(t.phase||"").toLowerCase();
    const breached = status === "breached";
    return !breached && (status==="funded" || status==="live" || phase==="funded" || phase==="live");
  });

  const breachedFunded = traders.filter(t=>{
    const status = String(t.status||"").toLowerCase();
    const phase = String(t.phase||"").toLowerCase();
    return status==="breached" && (phase==="funded" || phase==="live" || t.funded_at);
  });

  const list = fundedTraders.filter(t =>
    (t.name||"").toLowerCase().includes(s) ||
    (t.email||"").toLowerCase().includes(s) ||
    (t.phone||"").toLowerCase().includes(s) ||
    (t.mt5_login||"").toLowerCase().includes(s) ||
    (t.selected_plan||"").toLowerCase().includes(s) ||
    (t.admin_note||"").toLowerCase().includes(s)
  );

  const live = fundedTraders.filter(t=>String(t.status||"").toLowerCase()==="live").length;
  const funded = fundedTraders.filter(t=>String(t.status||"").toLowerCase()==="funded" || String(t.phase||"").toLowerCase()==="funded").length;
  const eligible = fundedTraders.filter(t=>payoutEligible(t)).length;
  const totalFundedCapital = fundedTraders.reduce((a,t)=>a+Number(t.account_size||t.balance||0),0);
  const totalProfit = fundedTraders.reduce((a,t)=>a+Number(t.profit||0),0);
  const totalPayouts = payouts
    .filter(p=>p.status==="paid")
    .reduce((a,p)=>a+Number(p.amount||0),0);

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">Funded Trader Engine</span>
        <h3 class="text-4xl font-black gold mt-3">Funded / Live Account Command</h3>
        <p class="text-gray-400 mt-2">
          Only active funded/live traders appear here. Breached accounts are removed from this command center.
        </p>
      </div>

      <div class="card2 p-5 rounded-2xl min-w-[250px]">
        <p class="text-gray-500 text-sm">Clean Funded Capital</p>
        <h2 class="text-4xl font-black gold">${money(totalFundedCapital)}</h2>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-3 xl:grid-cols-7 gap-4 mb-8">
    ${stat("Funded / Live",fundedTraders.length,"Excludes breached")}
    ${stat("Live Accounts",live,"Currently live")}
    ${stat("Funded Stage",funded,"Funded not breached")}
    ${stat("Payout Eligible",eligible,"Ready for payout review")}
    ${stat("Funded Profit",money(totalProfit),"Clean funded profit")}
    ${stat("Paid Payouts",money(totalPayouts),"All paid payouts")}
    ${stat("Removed Breached",breachedFunded.length,"Moved to breach control")}
  </div>

  <div class="grid lg:grid-cols-3 gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Clean Funded View</h3>
      <p class="text-gray-400">
        Breached traders no longer pollute the funded section. This page is now only for real funded/live traders.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Rule</p>
        <p class="text-green-400 font-bold">Funded/Live + Not Breached</p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Payout Control</h3>
      <p class="text-gray-400">
        Payout eligibility is shown only for traders still in good standing.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Eligibility</p>
        <p class="text-gray-300">Live/Funded + Profit above ₦0 + Not breached</p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Breach Separation</h3>
      <p class="text-gray-400">
        Breached funded accounts are now counted separately and managed through Account Management.
      </p>
      <div class="card2 p-4 rounded-2xl mt-5">
        <p class="text-gray-500 text-sm">Removed breached accounts</p>
        <p class="text-red-400 font-bold">${breachedFunded.length}</p>
      </div>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-4 mb-5">
      <div>
        <h3 class="text-3xl font-black gold">Funded Traders List</h3>
        <p class="text-gray-400">Clean table-style command center for funded/live traders only.</p>
      </div>
      <span class="badge">${list.length} visible</span>
    </div>

    ${
      list.length
      ? `<div class="overflow-x-auto">
          <table class="w-full text-left border-separate border-spacing-y-3">
            <thead>
              <tr class="text-gray-500 text-sm">
                <th class="p-3">Trader</th>
                <th class="p-3">Status</th>
                <th class="p-3">Capital</th>
                <th class="p-3">Profit</th>
                <th class="p-3">Drawdown</th>
                <th class="p-3">Payout</th>
                <th class="p-3">Actions</th>
              </tr>
            </thead>
            <tbody>
              ${list.map(fundedTraderRow).join("")}
            </tbody>
          </table>
        </div>`
      : empty("No clean funded/live traders yet. Promote traders from Account Management.")
    }
  </div>`;
}

function fundedTraderRow(t){
  const eligible = payoutEligible(t);
  const status = String(t.status||"").toLowerCase();
  const isLive = status==="live";

  return `
  <tr class="card2 rounded-2xl overflow-hidden">
    <td class="p-4 rounded-l-2xl">
      <h3 class="text-xl font-black gold">${t.name || "Unnamed Trader"}</h3>
      <p class="text-gray-400 text-sm">${t.email || ""}</p>
      <p class="text-gray-500 text-xs">${t.phone || ""}</p>
    </td>

    <td class="p-4">
      <span class="badge ${isLive ? "text-green-400" : "gold"}">${isLive ? "LIVE" : "FUNDED"}</span>
      <p class="text-gray-500 text-sm mt-2">${t.phase || "funded"}</p>
    </td>

    <td class="p-4">
      <h3 class="text-2xl font-black gold">${money(t.account_size || t.balance || 0)}</h3>
      <p class="text-gray-500 text-xs">${t.selected_plan || "No plan"}</p>
    </td>

    <td class="p-4">
      <h3 class="text-2xl font-black text-green-400">${money(t.profit || 0)}</h3>
      <p class="text-green-400 text-sm">${pct(t.profit_percent || 0)}</p>
    </td>

    <td class="p-4">
      <h3 class="text-2xl font-black text-red-400">${pct(t.drawdown_percent || 0)}</h3>
      <p class="text-gray-500 text-xs">Risk usage</p>
    </td>

    <td class="p-4">
      <span class="badge ${eligible ? "text-green-400" : "text-yellow-400"}">${eligible ? "ELIGIBLE" : "NOT READY"}</span>
      <p class="text-gray-500 text-xs mt-2">${eligible ? "Profit visible" : "No payout yet"}</p>
    </td>

    <td class="p-4 rounded-r-2xl">
      <div class="flex flex-wrap gap-2">
        <button class="btn btn-green" onclick="markLive('${t.id}')">Live</button>
        <button class="btn btn-gold" onclick="openPayoutReview('${t.id}')">Review</button>
        <button class="btn btn-red" onclick="fundedBreach('${t.id}')">Breach</button>
      </div>
    </td>
  </tr>`;
}



function payoutEligible(t){
  const status = String(t.status||"").toLowerCase();
  const phase = String(t.phase||"").toLowerCase();
  const isFunded = status==="funded" || status==="live" || phase==="funded" || phase==="live";
  const notBreached = status !== "breached";
  const profit = Number(t.profit||0);
  return isFunded && notBreached && profit > 0;
}

function fundedTraderCard(t){
  const eligible = payoutEligible(t);
  const status = String(t.status||"").toLowerCase();
  const isLive = status==="live";

  return `
  <div class="vault p-6 rounded-3xl">
    <div class="flex flex-wrap justify-between gap-4 mb-6">
      <div>
        <div class="flex flex-wrap gap-2 mb-3">
          <span class="badge ${isLive ? "text-green-400" : "gold"}">${isLive ? "LIVE ACCOUNT" : "FUNDED ACCOUNT"}</span>
          <span class="badge">${t.phase || "funded"}</span>
          <span class="badge ${eligible ? "text-green-400" : "text-yellow-400"}">${eligible ? "PAYOUT ELIGIBLE" : "NOT ELIGIBLE YET"}</span>
        </div>
        <h3 class="text-3xl font-black gold">${t.name || "Unnamed Trader"}</h3>
        <p class="text-gray-400">${t.email || ""} • ${t.phone || ""}</p>
        <p class="text-gray-500 text-sm mt-1">Reference: ${t.account_reference || "No reference"}</p>
      </div>

      <div class="text-right">
        <p class="text-gray-500 text-sm">Funded Date</p>
        <b>${formatDate(t.funded_at || t.updated_at || t.approved_at)}</b>
        <p class="text-gray-500 text-sm mt-2">Status</p>
        <b class="${isLive ? "text-green-400" : "gold"}">${t.status || "funded"}</b>
      </div>
    </div>

    <div class="grid md:grid-cols-5 gap-4 mb-6">
      <div class="card2 p-5 rounded-2xl md:col-span-2">
        <p class="text-gray-500 text-sm">Funded Account Size</p>
        <h3 class="text-4xl font-black gold">${money(t.account_size || t.balance || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Profit</p>
        <h3 class="text-3xl font-black text-green-400">${money(t.profit || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Profit %</p>
        <h3 class="text-3xl font-black text-green-400">${pct(t.profit_percent || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Drawdown</p>
        <h3 class="text-3xl font-black text-red-400">${pct(t.drawdown_percent || 0)}</h3>
      </div>
    </div>

    <div class="grid md:grid-cols-4 gap-4 mb-6">
      <div><p class="text-gray-500 text-sm">MT5 Login</p><b>${t.mt5_login || "Not assigned"}</b></div>
      <div><p class="text-gray-500 text-sm">Server</p><b>${t.mt5_server || "Not assigned"}</b></div>
      <div><p class="text-gray-500 text-sm">Plan</p><b>${t.selected_plan || "No plan"}</b></div>
      <div><p class="text-gray-500 text-sm">Trading Days Left</p><b>${t.trading_days_left ?? 30}</b></div>
    </div>

    <div class="card2 p-5 rounded-2xl mb-6">
      <p class="text-gray-500 text-sm">Funded Admin Note</p>
      <p class="text-gray-300 mt-2">${t.admin_note || "No funded note yet."}</p>
    </div>

    <div class="grid md:grid-cols-2 xl:grid-cols-5 gap-3">
      <button class="btn btn-green" onclick="markLive('${t.id}')">Mark Live</button>
      <button class="btn btn-gold" onclick="openPayoutReview('${t.id}')">Payout Review</button>
      <button class="btn btn-red" onclick="fundedBreach('${t.id}')">Mark Breached</button>
      <button class="btn btn-dark" onclick="fundedReset('${t.id}')">Return to Funded</button>
      <button class="btn btn-dark" onclick="archiveTrader('${t.id}')">Archive</button>
    </div>
  </div>`;
}

function markLive(id){
  const note = prompt("Admin note:", "Funded trader moved to live account status.") || "";
  lifecycleUpdate({
    id,
    status:"live",
    phase:"live",
    admin_note:note
  },"Trader marked as live.");
}

function openPayoutReview(id){
  const t = traders.find(x=>String(x.id)===String(id));
  if(!t){alert("Trader not found");return;}
  const eligible = payoutEligible(t);

  alert(
    `Payout Review\\n\\nTrader: ${t.name || "Unknown"}\\nAccount: ${money(t.account_size || t.balance || 0)}\\nProfit: ${money(t.profit || 0)}\\nStatus: ${t.status}\\nEligible: ${eligible ? "YES" : "NO"}\\n\\nUse Payouts module to approve or pay actual payout request.`
  );
}

function fundedBreach(id){
  const reason = prompt("Reason for funded breach:", "Funded/live account breached due to drawdown or rule violation.");
  if(reason === null) return;

  lifecycleUpdate({
    id,
    status:"breached",
    admin_note:reason
  },"Funded trader marked as breached.");
}

function fundedReset(id){
  const note = prompt("Admin note:", "Trader returned to funded status after review.") || "";
  lifecycleUpdate({
    id,
    status:"funded",
    phase:"funded",
    admin_note:note
  },"Trader returned to funded status.");
}



function accountsModule(){
  const s = q();

  const list = filteredTraders().filter(t =>
    (t.status||"").toLowerCase().includes(s) ||
    (t.phase||"").toLowerCase().includes(s) ||
    (t.admin_note||"").toLowerCase().includes(s) ||
    (t.selected_plan||"").toLowerCase().includes(s)
  );

  const active = traders.filter(t=>t.status==="active").length;
  const breached = traders.filter(t=>t.status==="breached").length;
  const phase1 = traders.filter(t=>t.phase==="phase1").length;
  const phase2 = traders.filter(t=>t.phase==="phase2").length;
  const funded = traders.filter(t=>t.phase==="funded" || t.status==="funded" || t.status==="live").length;
  const inactive = traders.filter(t=>t.status==="inactive" || t.status==="archived").length;

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">Account Lifecycle Engine</span>
        <h3 class="text-4xl font-black gold mt-3">Breach, Pass, Funded & Reset Control</h3>
        <p class="text-gray-400 mt-2">
          Move traders through the NairaPips lifecycle. Mark breaches, promote phases, activate funded accounts and manage retries.
        </p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[230px]">
        <p class="text-gray-500 text-sm">Breached Accounts</p>
        <h2 class="text-5xl font-black text-red-400">${breached}</h2>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
    ${stat("Active",active,"Currently trading")}
    ${stat("Phase 1",phase1,"Evaluation stage")}
    ${stat("Phase 2",phase2,"Verification stage")}
    ${stat("Funded / Live",funded,"Advanced accounts")}
    ${stat("Breached",breached,"Rule violation")}
    ${stat("Inactive",inactive,"Archived or paused")}
  </div>

  <div class="grid lg:grid-cols-3 gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Breach Command</h3>
      <p class="text-gray-400">
        Use this when a trader breaks maximum drawdown, fails challenge rules, violates trading behaviour, or fails evaluation.
      </p>
      <div class="mt-5 card2 p-4 rounded-2xl">
        <p class="text-gray-500 text-sm">Recommended Admin Note</p>
        <p class="text-gray-300 mt-2">Breached due to max drawdown / rule violation. Account closed pending review.</p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Phase Promotion</h3>
      <p class="text-gray-400">
        Move successful traders from Phase 1 to Phase 2, then to Funded/Live after verification.
      </p>
      <div class="mt-5 card2 p-4 rounded-2xl">
        <p class="text-gray-500 text-sm">Promotion Path</p>
        <p class="text-gray-300 mt-2">Phase 1 → Phase 2 → Funded → Live → Payout</p>
      </div>
    </div>

    <div class="vault p-6 rounded-3xl">
      <h3 class="text-2xl font-black gold mb-4">Retry / Reset</h3>
      <p class="text-gray-400">
        Reset account status for retry offers, corrections, support decisions or admin overrides.
      </p>
      <div class="mt-5 card2 p-4 rounded-2xl">
        <p class="text-gray-500 text-sm">Reset Result</p>
        <p class="text-gray-300 mt-2">Status becomes active, phase returns to Phase 1, drawdown/profit reset to zero.</p>
      </div>
    </div>
  </div>

  <div class="grid gap-5">
    ${list.map(accountLifecycleCard).join("") || empty("No trader record found.")}
  </div>`;
}

function lifecycleBadge(value){
  const v = String(value||"").toLowerCase();
  if(v==="breached") return `<span class="badge text-red-400">BREACHED</span>`;
  if(v==="funded" || v==="live") return `<span class="badge text-green-400">${String(value).toUpperCase()}</span>`;
  if(v==="active") return `<span class="badge text-green-400">ACTIVE</span>`;
  if(v==="inactive" || v==="archived") return `<span class="badge text-gray-400">${String(value).toUpperCase()}</span>`;
  return `<span class="badge">${value||"PROCESSING"}</span>`;
}

function accountLifecycleCard(t){
  const isBreached = t.status === "breached";
  const isFunded = t.status === "funded" || t.phase === "funded" || t.status === "live";

  return `
  <div class="vault p-6 rounded-3xl ${isBreached ? "danger" : ""}">
    <div class="flex flex-wrap justify-between gap-4 mb-6">
      <div>
        <div class="flex flex-wrap gap-2 mb-3">
          ${lifecycleBadge(t.status)}
          <span class="badge">${t.phase || "no_phase"}</span>
          <span class="badge">${t.payment_status || "payment_unknown"}</span>
        </div>
        <h3 class="text-3xl font-black gold">${t.name || "Unnamed Trader"}</h3>
        <p class="text-gray-400">${t.email || ""} • ${t.phone || ""}</p>
        <p class="text-gray-500 text-sm mt-1">Reference: ${t.account_reference || "No reference"}</p>
      </div>

      <div class="text-right">
        <p class="text-gray-500 text-sm">Joined</p>
        <b>${formatDate(t.created_at)}</b>
        <p class="text-gray-500 text-sm mt-2">Approved</p>
        <b>${formatDate(t.approved_at)}</b>
      </div>
    </div>

    <div class="grid md:grid-cols-5 gap-4 mb-6">
      <div class="card2 p-5 rounded-2xl md:col-span-2">
        <p class="text-gray-500 text-sm">Account Size</p>
        <h3 class="text-4xl font-black gold">${money(t.account_size || t.balance || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Profit %</p>
        <h3 class="text-3xl font-black text-green-400">${pct(t.profit_percent || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Drawdown %</p>
        <h3 class="text-3xl font-black text-red-400">${pct(t.drawdown_percent || 0)}</h3>
      </div>
      <div class="card2 p-5 rounded-2xl">
        <p class="text-gray-500 text-sm">Days Left</p>
        <h3 class="text-3xl font-black">${t.trading_days_left ?? 30}</h3>
      </div>
    </div>

    <div class="grid md:grid-cols-4 gap-4 mb-6">
      <div><p class="text-gray-500 text-sm">MT5 Login</p><b>${t.mt5_login || "Not assigned"}</b></div>
      <div><p class="text-gray-500 text-sm">Server</p><b>${t.mt5_server || "Not assigned"}</b></div>
      <div><p class="text-gray-500 text-sm">Plan</p><b>${t.selected_plan || "No plan"}</b></div>
      <div><p class="text-gray-500 text-sm">Last Login</p><b>${formatDate(t.last_login_at)}</b></div>
    </div>

    <div class="card2 p-5 rounded-2xl mb-6">
      <p class="text-gray-500 text-sm">Admin Note / Breach Reason</p>
      <p class="${isBreached ? "text-red-400" : "text-gray-300"} mt-2">${t.admin_note || "No lifecycle note yet."}</p>
    </div>

    <div class="grid md:grid-cols-2 xl:grid-cols-5 gap-3">
      <button class="btn btn-green" onclick="promotePhase2('${t.id}')">Pass Phase 1 → Phase 2</button>
      <button class="btn btn-gold" onclick="markFunded('${t.id}')">Mark Funded</button>
      <button class="btn btn-red" onclick="markBreached('${t.id}')">Mark Breached</button>
      <button class="btn btn-dark" onclick="resetTraderLifecycle('${t.id}')">Reset / Retry</button>
      <button class="btn btn-dark" onclick="archiveTrader('${t.id}')">Archive</button>
    </div>
  </div>`;
}

async function lifecycleUpdate(payload,successMessage){
  const res = await fetch(`${API_URL}/update_status`,{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify(payload)
  });

  const data = await res.json();

  if(data.success){
    alert(successMessage);
    loadData();
  }else{
    alert(data.error || "Lifecycle update failed");
  }
}

function markBreached(id){
  const reason = prompt(
    "Reason for breach:",
    "Breached due to max drawdown / rule violation."
  );

  if(reason === null) return;

  lifecycleUpdate({
    id,
    status:"breached",
    admin_note:reason
  },"Trader marked as breached.");
}

function promotePhase2(id){
  const note = prompt(
    "Admin note:",
    "Phase 1 passed. Trader promoted to Phase 2."
  );

  if(note === null) return;

  lifecycleUpdate({
    id,
    status:"active",
    phase:"phase2",
    admin_note:note,
    profit:0,
    drawdown:0,
    profit_percent:0,
    drawdown_percent:0,
    trading_days_left:30
  },"Trader promoted to Phase 2.");
}

function markFunded(id){
  const note = prompt(
    "Admin note:",
    "Trader passed verification and is now funded/live."
  );

  if(note === null) return;

  lifecycleUpdate({
    id,
    status:"funded",
    phase:"funded",
    admin_note:note,
    trading_days_left:30
  },"Trader marked as funded.");
}

function resetTraderLifecycle(id){
  if(!confirm("Reset this trader for retry? Profit/drawdown will return to zero and phase will return to Phase 1.")) return;

  lifecycleUpdate({
    id,
    status:"active",
    phase:"phase1",
    profit:0,
    drawdown:0,
    profit_percent:0,
    drawdown_percent:0,
    trading_days_left:30,
    admin_note:"Account reset for retry / admin override."
  },"Trader reset for retry.");
}

function archiveTrader(id){
  if(!confirm("Archive this trader account?")) return;

  lifecycleUpdate({
    id,
    status:"archived",
    admin_note:"Account archived by admin."
  },"Trader archived.");
}




function monitoringModule(){
  const s = q();
  const monitored = traders.filter(t =>
    (t.payment_status==="approved" || ["active","funded","live","breached"].includes(String(t.status||"").toLowerCase())) &&
    (t.mt5_login || t.account_size || t.balance)
  );
  const list = monitored.filter(t =>
    (t.name||"").toLowerCase().includes(s) ||
    (t.email||"").toLowerCase().includes(s) ||
    (t.phone||"").toLowerCase().includes(s) ||
    (t.mt5_login||"").toLowerCase().includes(s) ||
    (t.status||"").toLowerCase().includes(s) ||
    (t.phase||"").toLowerCase().includes(s)
  );
  const safe = monitored.filter(t=>monitorZone(t).zone==="safe").length;
  const warning = monitored.filter(t=>monitorZone(t).zone==="warning").length;
  const danger = monitored.filter(t=>monitorZone(t).zone==="danger").length;
  const breached = monitored.filter(t=>monitorZone(t).zone==="breached").length;
  const totalCapital = monitored.reduce((a,t)=>a+Number(t.account_size||t.balance||0),0);
  const totalEquity = monitored.reduce((a,t)=>a+Number(t.equity||t.balance||0),0);
  const totalProfit = monitored.reduce((a,t)=>a+Number(t.profit||0),0);

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">MT5 Monitoring Command</span>
        <h3 class="text-4xl font-black gold mt-3">Maximum Drawdown Monitoring</h3>
        <p class="text-gray-400 mt-2">NairaPips operates with maximum drawdown only. No daily drawdown logic is used here.</p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[260px] monitor-card">
        <p class="text-gray-500 text-sm">Total Monitored Capital</p>
        <h2 class="monitor-text font-black gold">${money(totalCapital)}</h2>
      </div>
    </div>
  </div>
  <div class="grid md:grid-cols-3 xl:grid-cols-7 gap-4 mb-8">
    ${stat("Monitored",monitored.length,"Approved/active MT5 accounts")}
    ${stat("Safe",safe,"0% - 50% DD used")}
    ${stat("Warning",warning,"51% - 75% DD used")}
    ${stat("Danger",danger,"76% - 99% DD used")}
    ${stat("Breached",breached,"100% max DD used")}
    ${monitorStat("Total Equity",money(totalEquity),"Equity snapshot")}
    ${monitorStat("Total Profit",money(totalProfit),"Visible profit")}
  </div>
  <div class="grid lg:grid-cols-3 gap-6 mb-8">
    <div class="vault p-6 rounded-3xl"><h3 class="text-2xl font-black gold mb-4">Rule Model</h3><p class="text-gray-400">This engine tracks only overall maximum drawdown.</p><div class="card2 p-4 rounded-2xl mt-5"><p class="text-gray-500 text-sm">NairaPips Rule</p><p class="text-green-400 font-bold">No Daily Drawdown. Maximum Drawdown Only.</p></div></div>
    <div class="vault p-6 rounded-3xl"><h3 class="text-2xl font-black gold mb-4">Auto-Breach Foundation</h3><p class="text-gray-400">When MT5 live sync is connected, these statuses can update automatically.</p><div class="card2 p-4 rounded-2xl mt-5"><p class="text-gray-500 text-sm">Auto rule</p><p class="text-red-400 font-bold">If max DD used ≥ 100% → Breached</p></div></div>
    <div class="vault p-6 rounded-3xl"><h3 class="text-2xl font-black gold mb-4">Profit Target Intelligence</h3><p class="text-gray-400">Profit target progress can later auto-detect phase pass.</p><div class="card2 p-4 rounded-2xl mt-5"><p class="text-gray-500 text-sm">Pass logic</p><p class="text-green-400 font-bold">Phase 1: 10% • Phase 2: 8%</p></div></div>
  </div>
  <div class="vault p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-4 mb-5"><div><h3 class="text-3xl font-black gold">Monitoring Table</h3><p class="text-gray-400">Simulation-ready now. Later this receives live MT5 values.</p></div><span class="badge">${list.length} visible</span></div>
    <div class="grid gap-5">${list.map(monitorTraderCard).join("") || empty("No monitored account yet. Approved traders with MT5 will appear here.")}</div>
  </div>`;
}
function monitorStat(label,value,extra=""){
  return `<div class="vault p-5 rounded-2xl overflow-hidden monitor-card"><p class="text-gray-400 text-sm">${label}</p><h3 class="monitor-text font-black gold">${value}</h3><p class="text-gray-500 text-xs mt-1">${extra}</p></div>`;
}
function maxDrawdownLimit(t){ return 20; }
function targetForPhase(t){ return String(t.phase||"").toLowerCase()==="phase2" ? 8 : 10; }
function monitorZone(t){
  if(String(t.status||"").toLowerCase()==="breached") return {zone:"breached",label:"BREACHED",cls:"monitor-breached",text:"text-red-400",usage:100};
  const maxDD = maxDrawdownLimit(t);
  const used = Math.abs(Number(t.drawdown_percent||0));
  const usage = maxDD > 0 ? Math.min(100,(used/maxDD)*100) : 0;
  if(usage >= 100) return {zone:"breached",label:"BREACH LEVEL",cls:"monitor-breached",text:"text-red-400",usage};
  if(usage >= 76) return {zone:"danger",label:"DANGER",cls:"monitor-danger",text:"text-red-400",usage};
  if(usage >= 51) return {zone:"warning",label:"WARNING",cls:"monitor-warning",text:"text-yellow-400",usage};
  return {zone:"safe",label:"SAFE",cls:"monitor-safe",text:"text-green-400",usage};
}
function monitorTraderCard(t){
  const z = monitorZone(t);
  const maxDD = maxDrawdownLimit(t);
  const ddUsed = Math.abs(Number(t.drawdown_percent||0));
  const ddUsage = z.usage ?? (maxDD > 0 ? Math.min(100,(ddUsed/maxDD)*100) : 0);
  const target = targetForPhase(t);
  const profitPct = Number(t.profit_percent||0);
  const targetProgress = target > 0 ? Math.min(100,(profitPct/target)*100) : 0;
  return `
  <div class="vault p-6 rounded-3xl ${z.zone==="breached" ? "danger" : ""}">
    <div class="flex flex-wrap justify-between gap-4 mb-6">
      <div><div class="flex flex-wrap gap-2 mb-3"><span class="badge ${z.text}">${z.label}</span><span class="badge">${t.phase || "phase1"}</span><span class="badge">${t.status || "processing"}</span></div><h3 class="text-3xl font-black gold">${t.name || "Unnamed Trader"}</h3><p class="text-gray-400">${t.email || ""} • ${t.phone || ""}</p><p class="text-gray-500 text-sm mt-1">MT5: ${t.mt5_login || "Not assigned"} • ${t.mt5_server || "No server"}</p></div>
      <div class="text-right min-w-[230px]"><p class="text-gray-500 text-sm">Account Size</p><h3 class="monitor-text font-black gold">${money(t.account_size || t.balance || 0)}</h3></div>
    </div>
    <div class="grid md:grid-cols-5 gap-4 mb-6">
      <div class="card2 p-5 rounded-2xl monitor-card"><p class="text-gray-500 text-sm">Balance</p><h3 class="payout-field-value font-black">${money(t.balance || 0)}</h3></div>
      <div class="card2 p-5 rounded-2xl monitor-card"><p class="text-gray-500 text-sm">Equity</p><h3 class="payout-field-value font-black text-green-400">${money(t.equity || t.balance || 0)}</h3></div>
      <div class="card2 p-5 rounded-2xl monitor-card"><p class="text-gray-500 text-sm">Profit</p><h3 class="payout-field-value font-black text-green-400">${money(t.profit || 0)}</h3></div>
      <div class="card2 p-5 rounded-2xl monitor-card"><p class="text-gray-500 text-sm">Profit %</p><h3 class="payout-field-value font-black text-green-400">${pct(t.profit_percent || 0)}</h3></div>
      <div class="card2 p-5 rounded-2xl monitor-card"><p class="text-gray-500 text-sm">Max DD Used</p><h3 class="payout-field-value font-black ${z.text}">${ddUsage.toFixed(1)}%</h3></div>
    </div>
    <div class="grid lg:grid-cols-2 gap-5 mb-6">
      <div class="card2 p-5 rounded-2xl"><div class="flex justify-between gap-4 mb-3"><p class="text-gray-400">Maximum Drawdown Usage</p><b class="${z.text}">${ddUsed.toFixed(2)}% / ${maxDD}%</b></div><div class="monitor-meter"><div class="${z.cls}" style="height:100%;width:${ddUsage}%"></div></div><p class="text-gray-500 text-sm mt-3">Safe 0-50% • Warning 51-75% • Danger 76-99% • Breached 100%</p></div>
      <div class="card2 p-5 rounded-2xl"><div class="flex justify-between gap-4 mb-3"><p class="text-gray-400">Profit Target Progress</p><b class="text-green-400">${profitPct.toFixed(2)}% / ${target}%</b></div><div class="monitor-meter"><div class="monitor-safe" style="height:100%;width:${targetProgress}%"></div></div><p class="text-gray-500 text-sm mt-3">Target: Phase 1 = 10%, Phase 2 = 8%</p></div>
    </div>
    <div class="grid md:grid-cols-2 xl:grid-cols-5 gap-3"><button class="btn btn-green" onclick="simulateProfit('${t.id}')">Sim Profit</button><button class="btn btn-gold" onclick="simulateDrawdown('${t.id}')">Sim DD</button><button class="btn btn-red" onclick="monitorBreach('${t.id}')">Mark Breached</button><button class="btn btn-dark" onclick="monitorReset('${t.id}')">Reset Metrics</button><button class="btn btn-dark" onclick="openMonitoringEvidence('${t.id}')">Evidence</button></div>
  </div>`;
}
function simulateProfit(id){
  const t = traders.find(x=>String(x.id)===String(id)); if(!t){alert("Trader not found");return;}
  const add = Number(prompt("Add profit percent for simulation:", "1") || 0);
  const newPct = Number(t.profit_percent||0) + add;
  const size = Number(t.account_size||t.balance||0);
  const newProfit = size * (newPct/100);
  lifecycleUpdate({id,profit_percent:newPct,profit:newProfit,equity:size+newProfit,admin_note:"Monitoring simulation: profit updated."},"Profit simulation updated.");
}
function simulateDrawdown(id){
  const t = traders.find(x=>String(x.id)===String(id)); if(!t){alert("Trader not found");return;}
  const dd = Number(prompt("Set total maximum drawdown percent used:", "5") || 0);
  const size = Number(t.account_size||t.balance||0);
  const drawdownAmount = size * (dd/100);
  const shouldBreach = dd >= maxDrawdownLimit(t);
  lifecycleUpdate({id,drawdown_percent:dd,drawdown:drawdownAmount,equity:size-drawdownAmount,status:shouldBreach?"breached":t.status,admin_note:shouldBreach?"Auto simulation: breached due to maximum drawdown.":"Monitoring simulation: drawdown updated."},shouldBreach?"Trader breached by max drawdown simulation.":"Drawdown simulation updated.");
}
function monitorBreach(id){
  const reason = prompt("Breach reason:", "Breached due to maximum drawdown rule.");
  if(reason === null) return;
  lifecycleUpdate({id,status:"breached",admin_note:reason},"Trader marked as breached.");
}
function monitorReset(id){
  if(!confirm("Reset monitoring metrics for this trader?")) return;
  const t = traders.find(x=>String(x.id)===String(id));
  const size = Number(t?.account_size||t?.balance||0);
  lifecycleUpdate({id,profit:0,drawdown:0,profit_percent:0,drawdown_percent:0,equity:size,admin_note:"Monitoring metrics reset by admin."},"Monitoring metrics reset.");
}



function zoneClass(zone){
  zone = String(zone||"safe").toLowerCase();
  if(zone==="warning") return "zone-warning";
  if(zone==="danger") return "zone-danger";
  if(zone==="critical") return "zone-critical";
  if(zone==="breached") return "zone-breached";
  return "zone-safe";
}

async function openMonitoringEvidence(traderId){
  try{
    const res = await fetch(`${API_URL}/breach_evidence/${traderId}`);
    const data = await res.json();
    if(!data.success){alert(data.error || "Evidence not found");return;}

    const ev = data.data;
    const events = ev.events || [];
    const snapshots = ev.snapshots || [];

    document.getElementById("content").innerHTML = `
    <div class="vault p-7 rounded-3xl mb-8">
      <button class="btn btn-dark mb-5" onclick="setModule('monitoring')">← Back to Monitoring</button>
      <span class="badge">Monitoring Evidence</span>
      <h3 class="text-4xl font-black gold mt-3">Breach Evidence & Equity Memory</h3>
      <p class="text-gray-400 mt-2">System-recorded proof from NairaPips monitoring engine.</p>
    </div>

    <div class="grid evidence-grid gap-4 mb-8">
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Status</p><h3 class="evidence-value font-black ${zoneClass(ev.status)}">${ev.status||"unknown"}</h3></div>
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Risk Zone</p><h3 class="evidence-value font-black ${zoneClass(ev.risk_zone)}">${ev.risk_zone||"safe"}</h3></div>
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Highest Equity</p><h3 class="evidence-value font-black gold">${money(ev.highest_equity||0)}</h3></div>
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Lowest Equity</p><h3 class="evidence-value font-black text-red-400">${money(ev.lowest_equity||0)}</h3></div>
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Max DD Used</p><h3 class="evidence-value font-black ${zoneClass(ev.risk_zone)}">${Number(ev.max_drawdown_used||0).toFixed(1)}%</h3></div>
      <div class="vault p-5 rounded-2xl"><p class="text-gray-500 text-sm">Breach Time</p><h3 class="text-xl font-black">${formatDate(ev.breach_time)}</h3></div>
    </div>

    <div class="grid lg:grid-cols-2 gap-6">
      <div class="vault p-6 rounded-3xl">
        <h3 class="text-3xl font-black gold mb-5">Monitoring Timeline</h3>
        <div class="grid gap-4">
          ${
            events.length ? events.map(e=>`
              <div class="flex gap-4">
                <div>
                  <div class="timeline-dot"></div>
                  <div class="timeline-line h-14"></div>
                </div>
                <div class="card2 p-4 rounded-2xl flex-1">
                  <div class="flex flex-wrap justify-between gap-3">
                    <b class="${zoneClass(e.risk_zone)}">${e.event_type||"event"}</b>
                    <span class="text-gray-500 text-sm">${formatDate(e.created_at)}</span>
                  </div>
                  <p class="text-gray-300 mt-2">${e.message||""}</p>
                  <p class="text-gray-500 text-xs mt-2">Equity: ${money(e.equity||0)} • Max DD Used: ${Number(e.max_drawdown_used||0).toFixed(1)}%</p>
                </div>
              </div>
            `).join("") : `<p class="text-gray-400">No monitoring events recorded yet.</p>`
          }
        </div>
      </div>

      <div class="vault p-6 rounded-3xl">
        <h3 class="text-3xl font-black gold mb-5">Recent Equity Snapshots</h3>
        <div class="grid gap-4">
          ${
            snapshots.length ? snapshots.map(s=>`
              <div class="card2 p-4 rounded-2xl">
                <div class="flex flex-wrap justify-between gap-3">
                  <b class="${zoneClass(s.risk_zone)}">${s.risk_zone||"safe"}</b>
                  <span class="text-gray-500 text-sm">${formatDate(s.created_at)}</span>
                </div>
                <div class="grid grid-cols-2 gap-3 mt-3 text-sm">
                  <p>Balance: <b>${money(s.balance||0)}</b></p>
                  <p>Equity: <b>${money(s.equity||0)}</b></p>
                  <p>Profit: <b class="text-green-400">${money(s.profit||0)}</b></p>
                  <p>DD Used: <b class="${zoneClass(s.risk_zone)}">${Number(s.max_drawdown_used||0).toFixed(1)}%</b></p>
                </div>
              </div>
            `).join("") : `<p class="text-gray-400">No equity snapshots recorded yet.</p>`
          }
        </div>
      </div>
    </div>`;
  }catch(e){
    alert("Could not load evidence: " + e.message);
  }
}



function marketingDeletedIds(){
  return (marketingDeletedIdCache || []).map(String);
}

async function saveMarketingDeletedIds(ids){
  const unique = [...new Set((ids || []).map(String))];
  try{
    await postJSON(`${API_URL}/marketing_deleted_contacts/save`, {contact_ids: unique});
    marketingDeletedIdCache = unique;
  }catch(e){
    alert("Marketing contacts could not update in Supabase: " + e.message);
    throw e;
  }
}

function normalizeNairaPhone(phone){
  const d = normalizeNairaPhoneDigits(phone);
  return d ? "+" + d : "";
}

function marketingUsers(){
  const deleted = marketingDeletedIds();
  const seen = new Set();
  const rows = (traders || []).map(t => {
    const phone = normalizeNairaPhone(t.phone || t.whatsapp || t.whatsapp_number || "");
    const email = String(t.email || "").trim();
    const id = String(t.id || email || phone || Math.random());
    return {
      id,
      name: t.name || t.full_name || t.trader_name || "Unknown",
      email,
      phone,
      created_at: t.created_at || t.joined_at || t.approved_at || "",
      deleted: deleted.includes(id)
    };
  }).filter(u => {
    const key = `${u.email}|${u.phone}`.toLowerCase();
    if(seen.has(key)) return false;
    seen.add(key);
    return (u.name || u.email || u.phone);
  });

  return rows;
}

function selectedMarketingIds(){
  return [...document.querySelectorAll('.marketing-select:checked')].map(x=>x.value);
}

function visibleMarketingIds(){
  return [...document.querySelectorAll('.marketing-select')].map(x=>x.value);
}

function toggleMarketingSelection(master){
  document.querySelectorAll('.marketing-select').forEach(cb=>cb.checked = master.checked);
}


function marketingDateValue(u){
  const raw = u.created_at || u.joined_at || u.approved_at || "";
  const d = raw ? new Date(raw) : null;
  return d && !isNaN(d.getTime()) ? d : null;
}

function marketingDateStart(unit){
  const now = new Date();
  if(unit === "today") return new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if(unit === "yesterday") return new Date(now.getFullYear(), now.getMonth(), now.getDate()-1);
  if(unit === "week"){
    const d = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const day = d.getDay() || 7;
    d.setDate(d.getDate() - day + 1);
    return d;
  }
  if(unit === "month") return new Date(now.getFullYear(), now.getMonth(), 1);
  if(unit === "last30") return new Date(now.getFullYear(), now.getMonth(), now.getDate()-30);
  if(unit === "last90") return new Date(now.getFullYear(), now.getMonth(), now.getDate()-90);
  return null;
}

function marketingMatchesDateRange(u, range){
  if(!range || range === "alltime") return true;
  const d = marketingDateValue(u);
  if(!d) return false;
  const now = new Date();

  if(range === "today") return d >= marketingDateStart("today");
  if(range === "yesterday"){
    const start = marketingDateStart("yesterday");
    const end = marketingDateStart("today");
    return d >= start && d < end;
  }
  if(range === "thisweek") return d >= marketingDateStart("week");
  if(range === "thismonth") return d >= marketingDateStart("month");
  if(range === "last30") return d >= marketingDateStart("last30") && d <= now;
  if(range === "last90") return d >= marketingDateStart("last90") && d <= now;
  if(range === "nodate") return !d;
  return true;
}

function marketingDateLabel(range){
  const labels = {
    alltime:"All Time",
    today:"Today",
    yesterday:"Yesterday",
    thisweek:"This Week",
    thismonth:"This Month",
    last30:"Last 30 Days",
    last90:"Last 90 Days",
    nodate:"No Date"
  };
  return labels[range] || "All Time";
}

function databaseModule(){
  const s = q();
  const filter = document.getElementById("marketingFilter")?.value || "active";
  const dateRange = document.getElementById("marketingDateRange")?.value || "alltime";
  const sort = document.getElementById("marketingSort")?.value || "newest";

  let list = marketingUsers().filter(u => {
    const hay = `${u.name} ${u.email} ${u.phone}`.toLowerCase();
    const matchSearch = hay.includes(s);
    const matchFilter = filter === "all" ||
      (filter === "active" && !u.deleted) ||
      (filter === "deleted" && u.deleted) ||
      (filter === "email" && !u.deleted && u.email) ||
      (filter === "whatsapp" && !u.deleted && u.phone);
    const matchDate = marketingMatchesDateRange(u, dateRange);
    return matchSearch && matchFilter && matchDate;
  });

  list.sort((a,b)=>{
    const da = marketingDateValue(a)?.getTime() || 0;
    const db = marketingDateValue(b)?.getTime() || 0;
    if(sort === "name") return String(a.name).localeCompare(String(b.name));
    if(sort === "email") return String(a.email).localeCompare(String(b.email));
    if(sort === "phone") return String(a.phone).localeCompare(String(b.phone));
    if(sort === "oldest") return da - db;
    if(sort === "newest") return db - da;
    if(sort === "deleted") return Number(b.deleted) - Number(a.deleted) || db - da;
    return db - da;
  });

  const all = marketingUsers();
  const active = all.filter(u=>!u.deleted);
  const removed = all.filter(u=>u.deleted);
  const withEmail = active.filter(u=>u.email);
  const withPhone = active.filter(u=>u.phone);
  const today = active.filter(u=>marketingMatchesDateRange(u,"today"));
  const week = active.filter(u=>marketingMatchesDateRange(u,"thisweek"));
  const month = active.filter(u=>marketingMatchesDateRange(u,"thismonth"));

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5 items-start">
      <div>
        <span class="badge">MARKETING CRM</span>
        <h3 class="text-5xl font-black gold mt-3">Users Database</h3>
        <p class="text-gray-400 mt-3 max-w-3xl">Pure NairaPips marketing contact system. No MT5 monitoring here — only name, email and WhatsApp contacts formatted for outreach, with date filters for campaigns.</p>
      </div>
      <div class="flex flex-wrap gap-3">
        <button onclick="copyMarketingEmails()" class="btn btn-gold">Copy All Emails</button>
        <button onclick="copyMarketingPhones()" class="btn btn-dark">Copy All WhatsApp</button>
        <button onclick="exportMarketingCSV()" class="btn btn-dark">Export All CSV</button>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-4 xl:grid-cols-8 gap-4 mb-6">
    ${stat("Active Contacts", active.length, "Marketing ready")}
    ${stat("Emails", withEmail.length, "Copy / export ready")}
    ${stat("WhatsApp", withPhone.length, "+234 normalized")}
    ${stat("Today", today.length, "New today")}
    ${stat("This Week", week.length, "Recent leads")}
    ${stat("This Month", month.length, "Monthly growth")}
    ${stat("Deleted", removed.length, "Can be restored")}
    ${stat("Total Records", all.length, "All contacts")}
  </div>

  <div class="crm-toolbar p-5 rounded-3xl mb-6">
    <div class="grid xl:grid-cols-[1fr_1fr_1fr_auto] gap-4 items-center">
      <select id="marketingFilter" onchange="databaseModule()">
        <option value="active" ${filter==="active"?"selected":""}>Active Contacts</option>
        <option value="deleted" ${filter==="deleted"?"selected":""}>Deleted Contacts</option>
        <option value="email" ${filter==="email"?"selected":""}>Email Ready</option>
        <option value="whatsapp" ${filter==="whatsapp"?"selected":""}>WhatsApp Ready</option>
        <option value="all" ${filter==="all"?"selected":""}>All Contacts</option>
      </select>
      <select id="marketingDateRange" onchange="databaseModule()">
        <option value="alltime" ${dateRange==="alltime"?"selected":""}>All Time</option>
        <option value="today" ${dateRange==="today"?"selected":""}>Today</option>
        <option value="yesterday" ${dateRange==="yesterday"?"selected":""}>Yesterday</option>
        <option value="thisweek" ${dateRange==="thisweek"?"selected":""}>This Week</option>
        <option value="thismonth" ${dateRange==="thismonth"?"selected":""}>This Month</option>
        <option value="last30" ${dateRange==="last30"?"selected":""}>Last 30 Days</option>
        <option value="last90" ${dateRange==="last90"?"selected":""}>Last 90 Days</option>
        <option value="nodate" ${dateRange==="nodate"?"selected":""}>No Date Records</option>
      </select>
      <select id="marketingSort" onchange="databaseModule()">
        <option value="newest" ${sort==="newest"?"selected":""}>Newest First</option>
        <option value="oldest" ${sort==="oldest"?"selected":""}>Oldest First</option>
        <option value="name" ${sort==="name"?"selected":""}>Sort By Name</option>
        <option value="email" ${sort==="email"?"selected":""}>Sort By Email</option>
        <option value="phone" ${sort==="phone"?"selected":""}>Sort By WhatsApp</option>
        <option value="deleted" ${sort==="deleted"?"selected":""}>Deleted First</option>
      </select>
      <div class="flex flex-wrap gap-2">
        <button onclick="copySelectedMarketingEmails()" class="btn btn-dark">Copy Selected Emails</button>
        <button onclick="copySelectedMarketingPhones()" class="btn btn-dark">Copy Selected WhatsApp</button>
        <button onclick="exportSelectedMarketingCSV()" class="btn btn-gold">Export Selected</button>
        <button onclick="deleteSelectedMarketingUsers()" class="btn btn-red">Delete Selected</button>
        <button onclick="restoreSelectedMarketingUsers()" class="btn btn-green">Restore Selected</button>
        <button onclick="deleteAllMarketingUsers()" class="btn btn-red">Delete All Contacts</button>
        <button onclick="restoreAllMarketingUsers()" class="btn btn-green">Restore All Contacts</button>
      </div>
    </div>
    <div class="mt-4 flex flex-wrap gap-2">
      <button onclick="setMarketingDateQuick('today')" class="btn btn-dark">Today</button>
      <button onclick="setMarketingDateQuick('thisweek')" class="btn btn-dark">This Week</button>
      <button onclick="setMarketingDateQuick('thismonth')" class="btn btn-dark">This Month</button>
      <button onclick="setMarketingDateQuick('last30')" class="btn btn-dark">Last 30 Days</button>
      <button onclick="setMarketingDateQuick('alltime')" class="btn btn-gold">All Time</button>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl">
    <div class="flex flex-wrap justify-between gap-4 mb-5">
      <div>
        <h3 class="text-3xl font-black gold">Contact List</h3>
        <p class="text-gray-400">${list.length} contact(s) visible • ${marketingDateLabel(dateRange)} • ${sort === "newest" ? "Newest first" : sort === "oldest" ? "Oldest first" : "Sorted by " + sort}. Select contacts for bulk actions.</p>
      </div>
      <label class="badge cursor-pointer"><input type="checkbox" class="crm-check mr-2" onchange="toggleMarketingSelection(this)"> Select Visible</label>
    </div>

    <div class="grid gap-4">
      ${list.map(marketingUserCard).join("") || `<div class="card p-10 rounded-3xl text-center text-gray-400">No contacts found in this date/filter segment.</div>`}
    </div>
  </div>`;
}

function setMarketingDateQuick(range){
  const el = document.getElementById("marketingDateRange");
  if(el) el.value = range;
  databaseModule();
}

function marketingUserCard(u){
  const wa = normalizeNairaPhoneDigits(u.phone);
  return `
  <div class="crm-contact-card p-5 rounded-2xl ${u.deleted ? "opacity-55" : ""}">
    <div class="grid lg:grid-cols-[auto_1.2fr_1.4fr_1.2fr_auto] gap-4 items-center">
      <input type="checkbox" class="marketing-select crm-check" value="${escapeQuotes(u.id)}">
      <div>
        <p class="text-gray-500 text-xs">Name</p>
        <h3 class="text-xl font-black">${escapeHtml(u.name)}</h3>
      </div>
      <div>
        <p class="text-gray-500 text-xs">Email</p>
        ${u.email ? `<a class="gold break-all" href="mailto:${escapeHtml(u.email)}">${escapeHtml(u.email)}</a>` : `<span class="text-gray-500">No email</span>`}
      </div>
      <div>
        <p class="text-gray-500 text-xs">WhatsApp</p>
        ${u.phone ? `<a class="gold font-bold" href="https://wa.me/${wa}" target="_blank">${escapeHtml(u.phone)}</a>` : `<span class="text-gray-500">No number</span>`}
        <p class="text-gray-600 text-xs mt-1">Joined: ${formatDate(u.created_at)}</p>
      </div>
      <div class="flex flex-wrap gap-2 justify-start lg:justify-end">
        <span class="badge">${u.deleted ? "DELETED" : "ACTIVE"}</span>
        ${u.email ? `<button class="btn btn-dark" onclick="copyText('${escapeQuotes(u.email)}','Email copied')">Copy Email</button>` : ""}
        ${u.phone ? `<button class="btn btn-dark" onclick="copyText('${escapeQuotes(u.phone)}','Phone copied')">Copy Phone</button>` : ""}
        ${u.deleted ? `<button class="btn btn-gold" onclick="restoreMarketingUser('${escapeQuotes(u.id)}')">Restore</button>` : `<button class="btn btn-red" onclick="deleteMarketingUser('${escapeQuotes(u.id)}')">Delete</button>`}
      </div>
    </div>
  </div>`;
}

function marketingByIds(ids){
  const set = new Set(ids.map(String));
  return marketingUsers().filter(u=>set.has(String(u.id)));
}

async function deleteMarketingUser(id){
  if(!confirm("Delete this contact from marketing database view? You can restore later.")) return;
  const ids = marketingDeletedIds();
  ids.push(String(id));
  await saveMarketingDeletedIds(ids);
  databaseModule();
}

async function restoreMarketingUser(id){
  const ids = marketingDeletedIds().filter(x => String(x) !== String(id));
  await saveMarketingDeletedIds(ids);
  databaseModule();
}

async function deleteSelectedMarketingUsers(){
  const selected = selectedMarketingIds();
  if(!selected.length){alert("Select contacts first.");return;}
  if(!confirm(`Delete ${selected.length} selected contact(s)? You can restore later.`)) return;
  await saveMarketingDeletedIds([...marketingDeletedIds(), ...selected]);
  databaseModule();
}

async function restoreSelectedMarketingUsers(){
  const selected = selectedMarketingIds();
  if(!selected.length){alert("Select contacts first.");return;}
  const keep = marketingDeletedIds().filter(id=>!selected.includes(String(id)));
  await saveMarketingDeletedIds(keep);
  databaseModule();
}

async function deleteAllMarketingUsers(){
  const active = marketingUsers().filter(u=>!u.deleted).map(u=>u.id);
  if(!active.length){alert("No active contacts to delete.");return;}
  if(!confirm(`Delete all ${active.length} active marketing contacts? You can restore them later.`)) return;
  await saveMarketingDeletedIds([...marketingDeletedIds(), ...active]);
  databaseModule();
}

async function restoreAllMarketingUsers(){
  if(!confirm("Restore all deleted marketing contacts?")) return;
  await saveMarketingDeletedIds([]);
  databaseModule();
}

function activeMarketingUsers(){
  return marketingUsers().filter(u=>!u.deleted);
}

function copyMarketingEmails(){
  const emails = activeMarketingUsers().map(u=>u.email).filter(Boolean);
  copyText(emails.join(", "), `${emails.length} emails copied`);
}

function copyMarketingPhones(){
  const phones = activeMarketingUsers().map(u=>u.phone).filter(Boolean);
  copyText(phones.join("\n"), `${phones.length} WhatsApp numbers copied`);
}

function copySelectedMarketingEmails(){
  const emails = marketingByIds(selectedMarketingIds()).map(u=>u.email).filter(Boolean);
  copyText(emails.join(", "), `${emails.length} selected emails copied`);
}

function copySelectedMarketingPhones(){
  const phones = marketingByIds(selectedMarketingIds()).map(u=>u.phone).filter(Boolean);
  copyText(phones.join("\n"), `${phones.length} selected WhatsApp numbers copied`);
}

function marketingCSV(rows){
  return [["Name","Email","WhatsApp","Status","Joined"].join(","), ...rows.map(u => [u.name,u.email,u.phone,u.deleted?"deleted":"active",formatDate(u.created_at)].map(csvCell).join(","))].join("\n");
}

function downloadMarketingCSV(rows, name){
  const blob = new Blob([marketingCSV(rows)], {type:"text/csv;charset=utf-8"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportMarketingCSV(){
  downloadMarketingCSV(activeMarketingUsers(), `nairapips-marketing-users-${new Date().toISOString().slice(0,10)}.csv`);
}

function exportSelectedMarketingCSV(){
  const rows = marketingByIds(selectedMarketingIds());
  if(!rows.length){alert("Select contacts first.");return;}
  downloadMarketingCSV(rows, `nairapips-selected-marketing-users-${new Date().toISOString().slice(0,10)}.csv`);
}

function csvCell(v){
  return `"${String(v||"").replace(/"/g,'""')}"`;
}

async function copyText(text,msg="Copied"){
  if(!text){ alert("Nothing to copy."); return; }
  try{
    await navigator.clipboard.writeText(text);
    alert(msg);
  }catch(e){
    const ta=document.createElement("textarea");
    ta.value=text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    ta.remove();
    alert(msg);
  }
}

function cleanPhone(phone){
  return normalizeNairaPhoneDigits(phone);
}

function escapeHtml(v){
  return String(v||"")
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;")
    .replace(/'/g,"&#039;");
}

function escapeQuotes(v){
  return String(v || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/* REFERRAL MARKETING */
function referralDefaults(){
  return {
    programName:"NairaPips Referral Program",
    baseUrl:"https://nairapips.com",
    defaultCode:"NAIRAPIPS",
    rebateType:"percent",
    rebateValue:"10",
    customerBonus:"0",
    cookieDays:"30",
    minPayout:"5000",
    status:"active",
    payoutRule:"Rebate is approved only after a referred trader pays and passes payment verification.",
    publicMessage:"Refer a trader to NairaPips and earn rebate when they buy a challenge."
  };
}

function getReferralSettings(){
  return {...referralDefaults(), ...(referralSettingsCache || {})};
}

async function setReferralSettings(data){
  const merged = {...getReferralSettings(), ...data};
  const saved = await postJSON(`${API_URL}/referral_settings`, merged);
  referralSettingsCache = saved.data || merged;
}

function cleanReferralCode(v){
  const x = String(v||"").trim().toUpperCase().replace(/[^A-Z0-9_-]/g,"");
  return x || "NAIRAPIPS";
}

function referralLink(code){
  const r = getReferralSettings();
  const base = String(r.baseUrl||"https://nairapips.com").replace(/\/$/,"");
  return `${base}?ref=${encodeURIComponent(cleanReferralCode(code || r.defaultCode))}`;
}

function referralRebateText(){
  const r = getReferralSettings();
  return r.rebateType === "fixed" ? money(r.rebateValue) : `${Number(r.rebateValue||0)}%`;
}

function referralEstimate(amount){
  const r = getReferralSettings();
  const n = Number(amount||0);
  if(r.rebateType === "fixed") return Number(r.rebateValue||0);
  return Math.round(n * (Number(r.rebateValue||0) / 100));
}

function referralGlobalBar(){
  const r = getReferralSettings();
  if(r.status !== "active") return `
    <div class="referral-bar rounded-3xl p-4 flex flex-wrap justify-between items-center gap-3">
      <div><span class="referral-chip">REFERRAL PAUSED</span><p class="text-gray-400 text-sm mt-2">Referral visibility is currently paused from the Referrals module.</p></div>
      <button class="btn btn-gold" onclick="setModule('referrals',document.querySelectorAll('.sidebar-btn')[16])">Open Referrals</button>
    </div>`;
  return `
  <div class="referral-bar rounded-3xl p-4 flex flex-wrap justify-between items-center gap-4">
    <div class="min-w-0">
      <div class="flex flex-wrap gap-2 mb-2">
        <span class="referral-chip">MARKETING REFERRAL LIVE</span>
        <span class="referral-chip">REBATE: ${referralRebateText()}</span>
        <span class="referral-chip">COOKIE: ${r.cookieDays} DAYS</span>
      </div>
      <p class="text-gray-300 text-sm break-words">${escapeHtml(r.publicMessage)} <b class="gold">${referralLink(r.defaultCode)}</b></p>
    </div>
    <div class="flex flex-wrap gap-2">
      <button class="btn btn-gold" onclick="copyReferralLink()">Copy Link</button>
      <button class="btn btn-dark" onclick="setModule('referrals',document.querySelectorAll('.sidebar-btn')[16])">Edit Rebates</button>
    </div>
  </div>`;
}

async function copyTextValue(text,msg="Copied."){
  try{ await navigator.clipboard.writeText(text); alert(msg); }
  catch(e){
    const ta=document.createElement("textarea"); ta.value=text; document.body.appendChild(ta); ta.select(); document.execCommand("copy"); ta.remove(); alert(msg);
  }
}

function copyReferralLink(code){ copyTextValue(referralLink(code),"Referral link copied."); }

function copyReferralMessage(){
  const r=getReferralSettings();
  const msg=`${r.publicMessage}\n\nReferral Link: ${referralLink(r.defaultCode)}\nRebate: ${referralRebateText()}\nRule: ${r.payoutRule}`;
  copyTextValue(msg,"Referral marketing message copied.");
}

async function saveReferralSettings(){
  const data={
    programName:document.getElementById("ref_program")?.value || "NairaPips Referral Program",
    baseUrl:document.getElementById("ref_base")?.value || "https://nairapips.com",
    defaultCode:cleanReferralCode(document.getElementById("ref_code")?.value || "NAIRAPIPS"),
    rebateType:document.getElementById("ref_type")?.value || "percent",
    rebateValue:document.getElementById("ref_value")?.value || "10",
    customerBonus:document.getElementById("ref_customer_bonus")?.value || "0",
    cookieDays:document.getElementById("ref_cookie")?.value || "30",
    minPayout:document.getElementById("ref_min_payout")?.value || "5000",
    status:document.getElementById("ref_status")?.value || "active",
    publicMessage:document.getElementById("ref_message")?.value || "Refer a trader to NairaPips and earn rebate when they buy a challenge.",
    payoutRule:document.getElementById("ref_rule")?.value || "Rebate is approved only after a referred trader pays and passes payment verification."
  };
  try{
    await setReferralSettings(data);
    alert("Referral settings saved to NairaPips Supabase. The referral banner has been updated across the admin pages.");
  }catch(e){
    alert("Referral settings could not save to Supabase: " + e.message);
    return;
  }
  referralsModule();
}

async function resetReferralSettings(){
  if(!confirm("Reset referral settings to NairaPips default?")) return;
  try{
    const saved = await postJSON(`${API_URL}/referral_settings/reset`, {});
    referralSettingsCache = saved.data || referralDefaults();
    referralsModule();
  }catch(e){
    alert("Referral settings could not reset in Supabase: " + e.message);
  }
}

function referralKnownCode(row){
  return cleanReferralCode(row.referral_code || row.ref_code || row.affiliate_code || row.referred_by || row.source_ref || "");
}


function referralSelfReferralRisk(p){
  const ref = referralKnownCode(p);
  if(!ref) return false;
  const email = String(p.email||"").toLowerCase();
  const phone = normalizeNairaPhoneDigits(p.phone||"");
  return traders.some(t => referralKnownCode(t) === ref && (String(t.email||"").toLowerCase() === email || normalizeNairaPhoneDigits(t.phone||"") === phone));
}

function referralsModule(){
  const r = getReferralSettings();
  const referredPurchases = purchases.filter(p => referralKnownCode(p));
  const referredTraders = traders.filter(t => referralKnownCode(t));
  const approvedRefPurchases = referredPurchases.filter(p => p.payment_status === "approved" || p.status === "approved");
  const approvedRevenue = approvedRefPurchases.reduce((a,p)=>a+Number(p.fee||0),0);
  const estimatedRebate = approvedRefPurchases.reduce((a,p)=>a+referralEstimate(p.fee||0),0);
  const baseLink = referralLink(r.defaultCode);

  document.getElementById("content").innerHTML = `
  <div class="referral-hero p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">REFERRAL MARKETING ENGINE</span>
        <h3 class="text-4xl font-black gold mt-3">Referral Links & Rebate Control</h3>
        <p class="text-gray-300 mt-2 max-w-4xl">Create one public referral link, adjust rebates from admin, copy campaign messages, and keep the offer visible across every admin page. This is built for marketing growth without touching MT5 monitoring.</p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[240px]">
        <p class="text-gray-500 text-sm">Current Rebate</p>
        <h2 class="text-5xl font-black gold">${referralRebateText()}</h2>
        <p class="text-gray-400 text-sm mt-2">Status: <b class="${r.status==='active'?'text-green-400':'text-red-400'}">${String(r.status).toUpperCase()}</b></p>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Referral Purchases",referredPurchases.length,"Detected with referral codes")}
    ${stat("Approved Referral Sales",approvedRefPurchases.length,"Eligible for rebate review")}
    ${stat("Referral Revenue",money(approvedRevenue),"Approved referred fees")}
    ${stat("Estimated Rebates",money(estimatedRebate),"Based on current rule")}
    ${stat("Self-Referral Risk",referredPurchases.filter(referralSelfReferralRisk).length,"Blocked/flagged for review")}
  </div>

  <div class="grid lg:grid-cols-[430px_1fr] gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-3xl font-black gold mb-2">Edit Referral Rules</h3>
      <p class="text-gray-400 mb-5">Adjust rebate, campaign link, payout rule and visibility. Saved permanently in NairaPips Supabase through the backend API.</p>

      <label class="text-gray-400 text-sm">Program Name</label>
      <input id="ref_program" value="${escapeHtml(r.programName)}" class="mb-3">

      <label class="text-gray-400 text-sm">Website / Landing Base URL</label>
      <input id="ref_base" value="${escapeHtml(r.baseUrl)}" class="mb-3">

      <label class="text-gray-400 text-sm">Main Referral Code</label>
      <input id="ref_code" value="${escapeHtml(r.defaultCode)}" class="mb-3">

      <div class="grid md:grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-gray-400 text-sm">Rebate Type</label>
          <select id="ref_type">
            <option value="percent" ${r.rebateType==='percent'?'selected':''}>Percentage %</option>
            <option value="fixed" ${r.rebateType==='fixed'?'selected':''}>Fixed Naira</option>
          </select>
        </div>
        <div>
          <label class="text-gray-400 text-sm">Rebate Value</label>
          <input id="ref_value" value="${escapeHtml(r.rebateValue)}" placeholder="10 or 5000">
        </div>
      </div>

      <div class="grid md:grid-cols-3 gap-3 mb-3">
        <div><label class="text-gray-400 text-sm">Customer Bonus</label><input id="ref_customer_bonus" value="${escapeHtml(r.customerBonus)}"></div>
        <div><label class="text-gray-400 text-sm">Cookie Days</label><input id="ref_cookie" value="${escapeHtml(r.cookieDays)}"></div>
        <div><label class="text-gray-400 text-sm">Min Payout</label><input id="ref_min_payout" value="${escapeHtml(r.minPayout)}"></div>
      </div>

      <label class="text-gray-400 text-sm">Visibility</label>
      <select id="ref_status" class="mb-3">
        <option value="active" ${r.status==='active'?'selected':''}>Active - show across pages</option>
        <option value="paused" ${r.status==='paused'?'selected':''}>Paused - show paused notice</option>
      </select>

      <label class="text-gray-400 text-sm">Public Marketing Message</label>
      <textarea id="ref_message" rows="3" class="mb-3">${escapeHtml(r.publicMessage)}</textarea>

      <label class="text-gray-400 text-sm">Admin Payout Rule</label>
      <textarea id="ref_rule" rows="3" class="mb-4">${escapeHtml(r.payoutRule)}</textarea>

      <div class="grid md:grid-cols-2 gap-3">
        <button onclick="saveReferralSettings()" class="btn btn-gold">Save Referral Settings</button>
        <button onclick="resetReferralSettings()" class="btn btn-dark">Reset Default</button>
      </div>
    </div>

    <div class="grid gap-6">
      <div class="vault p-6 rounded-3xl">
        <div class="flex flex-wrap justify-between gap-4 mb-4">
          <div>
            <h3 class="text-3xl font-black gold">Live Referral Link</h3>
            <p class="text-gray-400">Use this in WhatsApp, TikTok bio, landing page buttons, trader dashboard banners and email campaigns.</p>
          </div>
          <span class="badge">${r.cookieDays} day tracking window</span>
        </div>
        <div class="referral-link-box mb-4">${baseLink}</div>
        <div class="flex flex-wrap gap-3">
          <button onclick="copyReferralLink()" class="btn btn-gold">Copy Referral Link</button>
          <button onclick="copyReferralMessage()" class="btn btn-dark">Copy Marketing Message</button>
        </div>
      </div>

      <div class="vault p-6 rounded-3xl">
        <h3 class="text-3xl font-black gold mb-4">Plan Rebate Preview</h3>
        <div class="grid referral-grid gap-4">
          ${plans.map(p=>`
            <div class="referral-plan-card p-5 rounded-2xl">
              <p class="text-gray-500 text-sm">${escapeHtml(p.name||'Challenge Plan')}</p>
              <h3 class="text-3xl font-black gold mt-1">${money(p.fee)}</h3>
              <p class="text-gray-400 mt-2">Estimated rebate per approved sale:</p>
              <b class="text-2xl text-green-400">${money(referralEstimate(p.fee))}</b>
            </div>`).join("") || `<div class="card2 p-5 rounded-2xl text-gray-400">No challenge plans found yet.</div>`}
        </div>
      </div>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-4 mb-5">
      <div>
        <h3 class="text-3xl font-black gold">Referral Tracking Table</h3>
        <p class="text-gray-400">This reads referral fields if your backend supplies ref_code, referral_code, affiliate_code, source_ref or referred_by.</p>
      </div>
      <span class="badge">Soft admin tracking</span>
    </div>
    <div class="tableWrap">
      <table>
        <thead><tr><th>Referral Code</th><th>Trader</th><th>Email</th><th>Plan</th><th>Fee</th><th>Status</th><th>Estimated Rebate</th><th>Date</th></tr></thead>
        <tbody>
          ${referredPurchases.map(p=>`
            <tr>
              <td><b class="gold">${referralKnownCode(p)}</b>${referralSelfReferralRisk(p)?`<br><span class="badge test-badge">SELF-REFERRAL RISK</span>`:""}</td>
              <td>${escapeHtml(p.trader_name||p.name||'Trader')}</td>
              <td class="text-gray-400">${escapeHtml(p.email||'-')}</td>
              <td>${escapeHtml(p.plan_name||p.selected_plan||'-')}</td>
              <td>${money(p.fee)}</td>
              <td><span class="badge">${escapeHtml(p.payment_status||p.status||'pending')}</span></td>
              <td class="text-green-400 font-black">${money(referralEstimate(p.fee))}</td>
              <td class="text-gray-400">${formatDate(p.created_at)}</td>
            </tr>`).join("") || `<tr><td colspan="8" class="text-center text-gray-400 py-10">No referred purchases detected yet. Add referral capture to landing/trader pages using ?ref=CODE and store it with purchases.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl">
    <h3 class="text-3xl font-black gold mb-4">How to Show It On All NairaPips Pages</h3>
    <div class="grid md:grid-cols-3 gap-4">
      <div class="card2 p-5 rounded-2xl"><b class="gold">Landing Page</b><p class="text-gray-400 mt-2">Put the referral offer near pricing and checkout CTA. Track ?ref=CODE before payment.</p></div>
      <div class="card2 p-5 rounded-2xl"><b class="gold">Trader Dashboard</b><p class="text-gray-400 mt-2">Show each trader their personal referral link and rebate rule.</p></div>
      <div class="card2 p-5 rounded-2xl"><b class="gold">Admin Pages</b><p class="text-gray-400 mt-2">The global referral banner above is now visible across every admin module.</p></div>
    </div>
  </div>`;
}



/* STAFF RBAC */
function roleLabel(role){
  if(role === "super_admin") return "Super Admin";
  return role || "Custom Staff";
}

function staffStatusBadge(status){
  const s = String(status||"active").toLowerCase();
  const cls = s === "active" ? "text-green-400" : s === "suspended" ? "text-red-400" : "text-yellow-400";
  return `<span class="badge ${cls}">${s.toUpperCase()}</span>`;
}

function permissionCount(perms, role){
  const p = normalizePermissions(perms, role);
  if(p === "all") return "All access";
  return Object.keys(p).filter(m=>p[m]?.view).length + " module(s)";
}

function staffModule(){
  if(!canDo("staff","view")) return document.getElementById("content").innerHTML = empty("You do not have access to Staff Management.");
  const activeStaff = staffMembers.filter(s=>String(s.status||"active").toLowerCase()==="active").length;
  const suspendedStaff = staffMembers.filter(s=>String(s.status||"").toLowerCase()==="suspended").length;

  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <div class="flex flex-wrap justify-between gap-5">
      <div>
        <span class="badge">SUPER ADMIN MANUAL ACCESS CONTROL</span>
        <h3 class="text-4xl font-black gold mt-3">Staff Command Center</h3>
        <p class="text-gray-400 mt-2">Create workers and personally choose exactly what each one can see, edit, delete, approve, export or reveal.</p>
      </div>
      <div class="card2 p-5 rounded-2xl min-w-[260px]">
        <p class="text-gray-500 text-sm">Logged in as</p>
        <h3 class="text-2xl font-black gold">${currentAdmin.name || currentAdmin.username || "Super Admin"}</h3>
        <p class="text-gray-400">${roleLabel(currentAdmin.role)}</p>
      </div>
    </div>
  </div>

  <div class="grid md:grid-cols-4 gap-4 mb-8">
    ${stat("Staff Accounts",staffMembers.length,"Workers created")}
    ${stat("Active",activeStaff,"Can login")}
    ${stat("Suspended",suspendedStaff,"Blocked access")}
    ${stat("Audit Logs",auditLogs.length,"Recent actions")}
  </div>

  <div class="grid xl:grid-cols-[420px_1fr] gap-6 mb-8">
    <div class="vault p-6 rounded-3xl">
      <h3 class="text-3xl font-black gold mb-2">Create Staff</h3>
      <p class="text-gray-400 mb-5">You are the Super Admin. Give the worker a title, then tick exactly what they can see or do. No prepared role controls them.</p>
      <input id="staff_name" placeholder="Full name" class="mb-3">
      <input id="staff_email" placeholder="Email" class="mb-3">
      <input id="staff_username" placeholder="Username" class="mb-3">
      <input id="staff_password" placeholder="Temporary password" class="mb-3">
      <input id="staff_role" placeholder="Custom role title e.g My Finance Boy / Support Girl" class="mb-4">

      <div class="card2 p-4 rounded-2xl mb-4 border border-yellow-900/30">
        <div class="flex flex-wrap justify-between gap-3 items-center mb-4">
          <div>
            <h4 class="text-xl font-black gold">Manual Permission Control</h4>
            <p class="text-gray-500 text-sm">Tick what this staff can access. Unticked modules will be hidden from their sidebar.</p>
          </div>
          <div class="flex flex-wrap gap-2">
            <button type="button" onclick="selectCommonStaffPreset('viewonly')" class="btn btn-dark">View Only</button>
            <button type="button" onclick="selectCommonStaffPreset('support')" class="btn btn-dark">Support Helper</button>
            <button type="button" onclick="selectCommonStaffPreset('marketing')" class="btn btn-dark">Marketing Helper</button>
            <button type="button" onclick="selectCommonStaffPreset('clear')" class="btn btn-red">Clear All</button>
          </div>
        </div>
        <div id="createPermissionGrid" class="permission-grid"></div>
      </div>
      <button onclick="createStaff()" class="btn btn-gold w-full">Create Staff With These Permissions</button>
    </div>

    <div class="vault p-6 rounded-3xl">
      <div class="flex flex-wrap justify-between gap-4 mb-5">
        <div>
          <h3 class="text-3xl font-black gold">Staff Members</h3>
          <p class="text-gray-400">Control who can enter NairaPips admin and what they can touch.</p>
        </div>
        <button onclick="loadData()" class="btn btn-dark">Refresh Staff</button>
      </div>
      <div class="grid gap-5">
        ${staffMembers.map(staffCard).join("") || empty("No staff accounts yet. Create your first worker account.")}
      </div>
    </div>
  </div>

  <div class="vault p-6 rounded-3xl">
    <h3 class="text-3xl font-black gold mb-4">Audit Logs</h3>
    <div class="tableWrap">
      <table>
        <tr><th>Staff</th><th>Action</th><th>Module</th><th>Details</th><th>Date</th></tr>
        ${(auditLogs||[]).slice(0,50).map(a=>`
          <tr>
            <td>${a.staff_name || a.username || "System"}</td>
            <td><b>${a.action || "activity"}</b></td>
            <td>${a.module || "admin"}</td>
            <td class="text-gray-400">${a.details || "—"}</td>
            <td>${formatDate(a.created_at)}</td>
          </tr>`).join("") || `<tr><td colspan="5" class="text-center text-gray-400 py-8">No audit logs yet.</td></tr>`}
      </table>
    </div>
  </div>`;
  setTimeout(renderCreatePermissionGrid, 50);
}

function renderCreatePermissionGrid(){
  const box = document.getElementById("createPermissionGrid");
  if(!box) return;
  if(!staffCreatePermissionDraft || Object.keys(staffCreatePermissionDraft).length === 0){
    staffCreatePermissionDraft = emptyPermissions();
  }
  box.innerHTML = STAFF_MODULES.map(([module,label])=>createPermissionModuleCard(module,label)).join("");
}

function createPermissionModuleCard(module,label){
  const p = staffCreatePermissionDraft[module] || {};
  return `<div class="staff-permission-card">
    <div class="flex justify-between items-center gap-3 mb-2">
      <b class="gold">${label}</b>
      <label class="staff-switch"><input type="checkbox" ${p.view?"checked":""} onchange="toggleCreatePerm('${module}','view',this.checked)"> View</label>
    </div>
    <div class="grid grid-cols-2 gap-x-3">
      ${["create","edit","delete","approve","export","reveal_passwords"].map(a=>`
        <label class="staff-switch"><input type="checkbox" ${p[a]?"checked":""} onchange="toggleCreatePerm('${module}','${a}',this.checked)"> ${a.replace("_"," ")}</label>
      `).join("")}
    </div>
  </div>`;
}

function toggleCreatePerm(module, action, checked){
  if(!staffCreatePermissionDraft[module]) staffCreatePermissionDraft[module] = {};
  staffCreatePermissionDraft[module][action] = checked;
  if(action !== "view" && checked) staffCreatePermissionDraft[module].view = true;
  renderCreatePermissionGrid();
}

function selectCommonStaffPreset(type){
  staffCreatePermissionDraft = emptyPermissions();
  const allow = (mods, actions=["view"])=>{
    mods.forEach(m=>{
      if(!staffCreatePermissionDraft[m]) staffCreatePermissionDraft[m] = {};
      actions.forEach(a=>staffCreatePermissionDraft[m][a]=true);
      staffCreatePermissionDraft[m].view = true;
    });
  };
  if(type === "viewonly") allow(STAFF_MODULES.map(x=>x[0]), ["view"]);
  if(type === "support") allow(["overview","traders","support","announcements","database"], ["view","create","edit"]);
  if(type === "marketing") allow(["overview","database","referrals","announcements","revenue"], ["view","create","edit","export"]);
  if(type === "clear") staffCreatePermissionDraft = emptyPermissions();
  renderCreatePermissionGrid();
}

function staffCard(s){
  const perms = normalizePermissions(s.permissions, s.role);
  const visibleModules = perms === "all" ? STAFF_MODULES.map(x=>x[0]) : Object.keys(perms).filter(m=>perms[m]?.view);
  return `<div class="card2 p-5 rounded-2xl">
    <div class="flex flex-wrap justify-between gap-4 mb-4">
      <div>
        ${staffStatusBadge(s.status)}
        <h3 class="text-2xl font-black gold mt-3">${s.name || "Staff Member"}</h3>
        <p class="text-gray-400">${s.email || ""} • @${s.username || ""}</p>
      </div>
      <div class="text-right">
        <p class="text-gray-500 text-sm">Role</p>
        <b>${roleLabel(s.role)}</b>
        <p class="text-gray-500 text-xs mt-1">${permissionCount(s.permissions, s.role)}</p>
      </div>
    </div>
    <div class="flex flex-wrap gap-2 mb-4">
      ${visibleModules.slice(0,8).map(m=>`<span class="staff-pill">${STAFF_MODULES.find(x=>x[0]===m)?.[1] || m}</span>`).join("")}
      ${visibleModules.length>8 ? `<span class="staff-pill">+${visibleModules.length-8} more</span>` : ""}
    </div>
    <div class="grid md:grid-cols-4 gap-3">
      <button onclick="openPermissionEditor('${s.id}')" class="btn btn-gold">Edit Access</button>
      <button onclick="resetStaffPassword('${s.id}')" class="btn btn-dark">Reset Password</button>
      <button onclick="toggleStaffStatus('${s.id}','${String(s.status||"active").toLowerCase()==="active" ? "suspended" : "active"}')" class="btn ${String(s.status||"active").toLowerCase()==="active" ? "btn-red" : "btn-green"}">${String(s.status||"active").toLowerCase()==="active" ? "Suspend" : "Activate"}</button>
      <button onclick="deleteStaff('${s.id}')" class="btn btn-red">Delete</button>
    </div>
  </div>`;
}

function openPermissionEditor(id){
  const s = staffMembers.find(x=>String(x.id)===String(id));
  if(!s) return;
  staffPermissionDraft = normalizePermissions(s.permissions, s.role);
  document.getElementById("content").innerHTML = `
  <div class="vault p-7 rounded-3xl mb-8">
    <button onclick="staffModule()" class="btn btn-dark mb-5">← Back to Staff</button>
    <span class="badge">Permission Matrix</span>
    <h3 class="text-4xl font-black gold mt-3">${s.name || "Staff"}</h3>
    <p class="text-gray-400 mt-2">Tick only what this worker should see or do. Unticked modules disappear from their sidebar.</p>
  </div>
  <div class="vault p-6 rounded-3xl mb-8">
    <div class="grid md:grid-cols-3 gap-4 mb-6">
      <input id="edit_staff_name" value="${s.name || ""}" placeholder="Name">
      <input id="edit_staff_email" value="${s.email || ""}" placeholder="Email">
      <input id="edit_staff_role" value="${s.role || ""}" placeholder="Custom role title">
    </div>
    <div class="permission-grid">
      ${STAFF_MODULES.map(([module,label])=>permissionModuleCard(module,label)).join("")}
    </div>
    <div class="flex flex-wrap gap-3 mt-6">
      <button onclick="saveStaffPermissions('${s.id}')" class="btn btn-gold">Save Staff Permissions</button>
      <button onclick="staffModule()" class="btn btn-dark">Cancel</button>
    </div>
  </div>`;
}

function permissionModuleCard(module,label){
  const p = staffPermissionDraft[module] || {};
  return `<div class="staff-permission-card">
    <div class="flex justify-between items-center gap-3 mb-2">
      <b class="gold">${label}</b>
      <label class="staff-switch"><input type="checkbox" ${p.view?"checked":""} onchange="togglePerm('${module}','view',this.checked)"> View</label>
    </div>
    <div class="grid grid-cols-2 gap-x-3">
      ${["create","edit","delete","approve","export","reveal_passwords"].map(a=>`
        <label class="staff-switch"><input type="checkbox" ${p[a]?"checked":""} onchange="togglePerm('${module}','${a}',this.checked)"> ${a.replace("_"," ")}</label>
      `).join("")}
    </div>
  </div>`;
}

function togglePerm(module, action, checked){
  if(!staffPermissionDraft[module]) staffPermissionDraft[module] = {};
  staffPermissionDraft[module][action] = checked;
  if(action !== "view" && checked) staffPermissionDraft[module].view = true;
}

async function createStaff(){
  const role = document.getElementById("staff_role").value || "Custom Staff";
  const permissions = staffCreatePermissionDraft && Object.keys(staffCreatePermissionDraft).length ? staffCreatePermissionDraft : emptyPermissions();
  const payload = {
    name:document.getElementById("staff_name").value,
    email:document.getElementById("staff_email").value,
    username:document.getElementById("staff_username").value,
    password:document.getElementById("staff_password").value,
    role,
    permissions,
    status:"active"
  };
  if(!payload.name || !payload.username || !payload.password){alert("Name, username and password are required.");return;}
  const visibleCount = Object.keys(permissions).filter(m=>permissions[m]?.view).length;
  if(visibleCount === 0 && !confirm("This staff has no module access. Create anyway?")) return;
  try{ await postJSON(`${API_URL}/staff_members`, payload); alert("Staff account created with your custom permissions."); staffCreatePermissionDraft = emptyPermissions(); loadData(); }
  catch(e){ alert(e.message || "Could not create staff."); }
}

async function saveStaffPermissions(id){
  const payload = {id, name:document.getElementById("edit_staff_name").value, email:document.getElementById("edit_staff_email").value, role:document.getElementById("edit_staff_role").value, permissions:staffPermissionDraft};
  try{ await postJSON(`${API_URL}/staff_members/update`, payload); alert("Permissions saved."); loadData(); }
  catch(e){ alert(e.message || "Could not save permissions."); }
}

async function toggleStaffStatus(id,status){
  try{ await postJSON(`${API_URL}/staff_members/status`, {id,status}); alert("Staff status updated."); loadData(); }
  catch(e){ alert(e.message || "Could not update staff."); }
}

async function resetStaffPassword(id){
  const password = prompt("New temporary password:");
  if(!password) return;
  try{ await postJSON(`${API_URL}/staff_members/password`, {id,password}); alert("Password reset."); }
  catch(e){ alert(e.message || "Could not reset password."); }
}

async function deleteStaff(id){
  if(!confirm("Delete this staff account?")) return;
  try{ await postJSON(`${API_URL}/staff_members/delete`, {id}); alert("Staff deleted."); loadData(); }
  catch(e){ alert(e.message || "Could not delete staff."); }
}


function placeholder(title,msg){
  document.getElementById("content").innerHTML = `<div class="vault p-8 rounded-3xl"><h3 class="text-3xl font-black gold mb-4">${title}</h3><p class="text-gray-400 text-lg">${msg}</p></div>`;
}
</script>

<script>

// Safe MT5 Password Generator Patch
window.generateMT5Password = window.generateMT5Password || function(inputId, length = 14){
  const upper = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const lower = "abcdefghijkmnopqrstuvwxyz";
  const numbers = "23456789";
  const symbols = "@#$%&*!?";
  const all = upper + lower + numbers + symbols;
  let password = "";
  password += upper[Math.floor(Math.random() * upper.length)];
  password += lower[Math.floor(Math.random() * lower.length)];
  password += numbers[Math.floor(Math.random() * numbers.length)];
  password += symbols[Math.floor(Math.random() * symbols.length)];
  for(let i=password.length; i<length; i++){
    password += all[Math.floor(Math.random() * all.length)];
  }
  password = password.split("").sort(() => Math.random() - 0.5).join("");
  const field = document.getElementById(inputId);
  if(field) field.value = password;
};

window.copyMT5Field = window.copyMT5Field || async function(inputId){
  const field = document.getElementById(inputId);
  if(!field || !field.value){ alert("Nothing to copy yet."); return; }
  try{
    await navigator.clipboard.writeText(field.value);
    alert("Password copied.");
  }catch(e){
    field.select();
    document.execCommand("copy");
    alert("Password copied.");
  }
};

window.clearMT5Passwords = window.clearMT5Passwords || function(){
  const master = document.getElementById("mt5_master");
  const investor = document.getElementById("mt5_investor");
  if(master) master.value = "";
  if(investor) investor.value = "";
};

window.previewMT5PasswordRules = window.previewMT5PasswordRules || function(){
  alert("Password rules:\\n\\n• 14 characters\\n• Uppercase letters\\n• Lowercase letters\\n• Numbers\\n• Symbols: @ # $ % & * ! ?\\n\\nUse different Master and Investor passwords for every MT5 account.");
};

</script>

<script>
document.addEventListener("DOMContentLoaded", function(){
  const pass = document.getElementById("adminPass");
  const user = document.getElementById("adminUser");
  [user, pass].forEach(el=>{
    if(el){
      el.addEventListener("keydown", function(e){
        if(e.key === "Enter") adminLogin();
      });
    }
  });
});
</script>

</body>
</html>
