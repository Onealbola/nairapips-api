/* Security review is staff-only; shared IPs never trigger an automatic restriction. */
function npAccessReviewNav(){return ['super_admin','admin','admin_manager'].includes(String(SESSION.admin?.role||'').toLowerCase())?'<button class="nav" onclick="npOpenAccessReview()">🔎 Account Links</button>':'';}
window.npOpenAccessReview=async function(){
 const state={sequence:0,search:0};window.NP_ACCESS_REVIEW=state;
 modal('ACCOUNT LINKS',`<div style="max-width:1100px"><h2>Trader access review</h2><p class="text-sm text-gray-400">Shared Wi-Fi, mobile networks and VPNs can produce IP matches. Review the evidence before taking action.</p><div id="np-access-status" class="card my-3">Checking security history…</div><div class="flex flex-wrap gap-2 my-3"><input id="np-access-query" class="input" placeholder="Search trader name or email" maxlength="100"><button class="btn btn-gold" onclick="npSearchAccessTraders()">Search</button><select id="np-access-days" class="select" onchange="npLoadAccessAlerts();if(window.NP_ACCESS_REVIEW?.traderId)npLoadAccessReview(window.NP_ACCESS_REVIEW.traderId)"><option value="7">Last 7 days</option><option value="30" selected>Last 30 days</option><option value="90">Last 90 days</option></select></div><div id="np-access-alerts"></div><div id="np-access-search"></div><div id="np-access-report"></div></div>`);
 try{const r=await api('/admin/trader_access/status');if(window.NP_ACCESS_REVIEW!==state)return;const el=document.getElementById('np-access-status');if(!el)return;
  el.textContent=!r.ready?'Database setup required. Run the trader access security SQL setup before relying on this screen.':r.warning?'Logging warning: '+r.warning:'Security history is enabled. Records are retained for 90 days. Times below are Nigeria time (WAT).';
  state.ready=!!r.ready;if(state.ready)npLoadAccessAlerts();
  if(r.unrecorded_events)el.textContent+=' '+r.unrecorded_events+' access event(s) could not be saved; some history is missing.';
 }catch(e){const el=document.getElementById('np-access-status');if(el)el.textContent=e.message||'Security status unavailable';}
};
window.npSearchAccessTraders=async function(){
 const state=window.NP_ACCESS_REVIEW,query=document.getElementById('np-access-query')?.value.trim(),box=document.getElementById('np-access-search');if(!state||!box)return;
 if(!query||query.length<2){box.textContent='Enter at least two characters.';return;}
 const seq=++state.search;box.textContent='Searching…';
 try{const r=await api('/admin/trader_access/search?q='+encodeURIComponent(query));if(window.NP_ACCESS_REVIEW!==state||seq!==state.search||!box.isConnected)return;
  box.innerHTML=(r.traders||[]).map(t=>`<button class="btn btn-ghost m-1" onclick='npLoadAccessReview(${esc(JSON.stringify(String(t.id)))})'>${esc(t.name||'Trader')} · ${esc(t.email||'')}</button>`).join('')||'No matching traders.';
 }catch(e){if(seq===state.search)box.textContent=e.message||'Search unavailable';}
};
function npAccessTime(value){return value?new Date(value).toLocaleString('en-NG',{timeZone:'Africa/Lagos'}):'Not recorded';}
window.npLoadAccessReview=async function(id){
 const state=window.NP_ACCESS_REVIEW,box=document.getElementById('np-access-report');if(!state||!box)return;
 state.traderId=id;const seq=++state.sequence,days=document.getElementById('np-access-days').value;box.textContent='Loading access history…';
 try{const r=await api('/admin/trader_access/review?'+new URLSearchParams({trader_id:id,days}));if(window.NP_ACCESS_REVIEW!==state||seq!==state.sequence||!box.isConnected)return;
  if(!r.success)throw Error(r.error||'Review unavailable');const d=r.data,m=d.matches||[],events=d.events||[];
  box.innerHTML=`<section class="card my-3"><h2>${esc(d.trader.name||'Trader')}</h2><p>${esc(d.trader.email||'')}</p><strong style="color:${m.length?'#fca5a5':'#ffda5c'}">${m.length?'POSSIBLE LINK TO BANNED ACCOUNT — REVIEW REQUIRED':'No banned-account IP match found in recorded history'}</strong><p class="text-xs text-gray-400">${esc(d.matches_total||0)} matching evidence record(s). A missing match does not prove that accounts are unrelated. Older registration IPs are identified separately because their proxy provenance is unverified.</p><button class="btn btn-ghost" onclick="openNairaPipsBlackBook()">Open Black Book</button></section><div class="card my-3" style="overflow:auto"><h3>Matching banned accounts</h3><table class="compact"><thead><tr><th>Account</th><th>Shared IP</th><th>This trader last seen</th><th>Banned account last seen</th><th>Evidence</th></tr></thead><tbody>${m.map(x=>`<tr><td>${esc(x.banned_name||'Trader')}<div class="text-xs">${esc(x.banned_email||'')}</div></td><td class="mono">${esc(x.ip)}</td><td>${esc(npAccessTime(x.last_seen))}</td><td>${esc(npAccessTime(x.banned_last_seen))}</td><td>${esc(x.evidence)}</td></tr>`).join('')||'<tr><td colspan="5">No matching evidence recorded for this period.</td></tr>'}</tbody></table></div><div class="card my-3" style="overflow:auto"><h3>Recent successful registrations and logins</h3><p class="text-xs text-gray-400">Showing ${events.length} of ${esc(d.events_total||0)} recorded events. New logging starts after setup; earlier login history cannot be reconstructed.</p><table class="compact"><thead><tr><th>Time (WAT)</th><th>Event</th><th>IP address</th><th>IP source</th><th>Browser / device details</th></tr></thead><tbody>${events.map(x=>`<tr><td>${esc(npAccessTime(x.observed_at))}</td><td>${esc(x.event_type)}</td><td class="mono">${esc(x.ip)}</td><td>${esc(x.ip_source)}</td><td style="max-width:360px;overflow-wrap:anywhere">${esc(x.user_agent||'Not supplied')}</td></tr>`).join('')||'<tr><td colspan="5">No successful access events recorded in this period.</td></tr>'}</tbody></table></div>`;
 }catch(e){if(seq===state.sequence&&box.isConnected)box.textContent=e.message||'Security history unavailable';}
};

window.npLoadAccessAlerts=async function(){
 const state=window.NP_ACCESS_REVIEW,box=document.getElementById('np-access-alerts');if(!state?.ready||!box)return;
 const seq=(state.alertSequence||0)+1;state.alertSequence=seq;
 try{const r=await api('/admin/trader_access/alerts?days='+document.getElementById('np-access-days').value);if(window.NP_ACCESS_REVIEW!==state||seq!==state.alertSequence||!box.isConnected)return;
  if(!r.success)throw Error(r.error||'Review list unavailable');const d=r.data;
  box.innerHTML=`<section class="card my-3"><h3>Possible linked accounts — review required</h3><p class="text-sm text-gray-400">${esc(d.total)} account(s) share a recorded IP with a restricted trader. This list refreshes every 30 seconds while open. These are clues, not confirmed identities.</p>${(d.alerts||[]).map(t=>`<button class="btn btn-ghost m-1" onclick='npLoadAccessReview(${esc(JSON.stringify(String(t.trader_id)))})'>${esc(t.name||'Trader')} · ${esc(t.email||'')} · ${esc(t.matched_banned_accounts)} restricted account match(es)</button>`).join('')||'<p>No potential links in the recorded history for this period.</p>'}</section>`;
 }catch(e){if(seq===state.alertSequence&&box.isConnected)box.textContent=e.message||'Review list unavailable';}
};
setInterval(()=>{if(!document.hidden&&document.getElementById('np-access-alerts')&&window.NP_ACCESS_REVIEW?.ready)npLoadAccessAlerts();},30000);
