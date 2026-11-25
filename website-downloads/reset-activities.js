/* Compact dashboard entry point; original reset eligibility/payment actions stay intact. */
(function(){
'use strict';
if(window.NP_RESET_ACTIVITIES_RELEASE||typeof npRenderPreviousResetPanel!=='function')return;
window.NP_RESET_ACTIVITIES_RELEASE='reset-activities-20261125-v1';
let owner='',expanded=false;
function currentOwner(){return String(SESSION.data?.trader?.id||SESSION.trader?.id||'')}
const style=document.createElement('style');style.textContent=`
.np-reset-activities{border:1px solid #d4af3745;border-radius:16px;background:linear-gradient(135deg,#b18c2010,#101010);margin-bottom:18px;min-width:0;overflow:hidden}
.np-reset-activities>summary{display:flex;align-items:center;justify-content:space-between;gap:12px;list-style:none;cursor:pointer;padding:18px 20px;min-height:76px;box-sizing:border-box}
.np-reset-activities>summary::-webkit-details-marker{display:none}.np-reset-activities>summary:focus-visible{outline:2px solid #ffda5c;outline-offset:-4px;border-radius:15px}
.np-reset-activities-title{display:block;font-size:16px;font-weight:850;color:#fff}.np-reset-activities-sub{display:block;font-size:12px;color:#a6a6a6;margin-top:4px}
.np-reset-activities-meta{display:flex;gap:12px;align-items:center;flex-shrink:0}.np-reset-activities-count{border:1px solid #d4af3750;border-radius:20px;padding:5px 10px;color:#ffda5c;font-size:11px;font-weight:800;white-space:nowrap}
.np-reset-activities-arrow{color:#ffda5c;display:block;transition:transform .15s}.np-reset-activities[open]>summary .np-reset-activities-arrow{transform:rotate(180deg)}
.np-reset-activities-content{padding:0 20px 18px;min-width:0}.np-reset-activities-content .mono{overflow-wrap:anywhere}
@media(max-width:480px){.np-reset-activities>summary{padding:15px}.np-reset-activities-content{padding:0 14px 15px}.np-reset-activities-title{font-size:15px}.np-reset-activities-sub{font-size:11px}.np-reset-activities-meta{gap:8px}.np-reset-activities-content button{max-width:100%;white-space:normal}}
`;document.head.appendChild(style);
const render=npRenderPreviousResetPanel;
window.npRenderPreviousResetPanel=function(...args){
 const html=render.apply(this,args);if(!html)return html;
 const nextOwner=currentOwner();if(owner!==nextOwner){owner=nextOwner;expanded=false}
 const template=document.createElement('template');template.innerHTML=html;
 const panel=template.content.querySelector('section');const head=panel?.querySelector('.np-smart-panel-head');
 if(!panel||!head)return html;
 const count=Number.parseInt(head.querySelector('.pill')?.textContent||'',10);
 if(!Number.isFinite(count))return html;
 head.remove();
 // Internal authority markers are not useful to the trader.
 for(const span of panel.querySelectorAll('span'))if(/Authority:/.test(span.textContent))span.remove();
 return '<details class="np-reset-activities" data-reset-activities'+(expanded?' open':'')+'><summary aria-label="Activities: view resets"><span><span class="np-reset-activities-title">Activities</span><span class="np-reset-activities-sub">View your resets and payment updates</span></span><span class="np-reset-activities-meta"><span class="np-reset-activities-count">'+count+' reset'+(count===1?'':'s')+'</span><span class="np-reset-activities-arrow" aria-hidden="true">⌄</span></span></summary><div class="np-reset-activities-content">'+panel.innerHTML+'</div></details>';
};
document.addEventListener('toggle',event=>{const panel=event.target;if(panel.matches?.('[data-reset-activities]')&&panel.isConnected){owner=currentOwner();expanded=panel.open}},true);
})();
