/* Mobile presentation only: existing navigation and financial handlers stay authoritative. */
(function(){
 'use strict';
 if(window.NP_MOBILE_PANELS)return;
 window.NP_MOBILE_PANELS='2026-11-19-card-fit';
 const mq=window.matchMedia('(max-width:900px)');
 let drawer=null,opener=null,oldOverflow='',frame=0;
 const inerted=new Map();
 const svg=path=>'<svg viewBox="0 0 24 24" aria-hidden="true">'+path+'</svg>';
 const icons={home:svg('<path d="m3 10 9-7 9 7v10H3Z"/><path d="M9 20v-7h6v7"/>'),accounts:svg('<rect x="4" y="3" width="16" height="18" rx="3"/><path d="M8 8h8M8 12h8M8 16h5"/>'),purchase:svg('<path d="M5 7h14l1 14H4L5 7Z"/><path d="M8 8V6a4 4 0 0 1 8 0v2"/>'),payouts:svg('<rect x="3" y="5" width="18" height="15" rx="3"/><path d="M3 10h18M15 15h3"/>'),menu:svg('<path d="M4 6h16M4 12h16M4 18h16"/>')};
 function closeMenu(restore=true){
  if(!drawer)return;
  const previous=drawer;drawer=null;
  previous.classList.remove('np-trader-drawer-open');previous.removeAttribute('role');previous.removeAttribute('aria-modal');
  document.querySelector('.np-trader-backdrop')?.setAttribute('hidden','');
  document.querySelectorAll('[data-np-trader-menu]').forEach(b=>b.setAttribute('aria-expanded','false'));
  document.body.style.overflow=oldOverflow;
  inerted.forEach((value,node)=>{node.inert=value;});inerted.clear();
  filterMenu('');
  if(restore&&opener?.isConnected)opener.focus({preventScroll:true});
 }
 function openMenu(button){
  if(!mq.matches||drawer)return;
  drawer=document.querySelector('.app>.sidebar');if(!drawer)return;
  opener=button;oldOverflow=document.body.style.overflow;
  drawer.classList.add('np-trader-drawer-open');drawer.setAttribute('role','dialog');drawer.setAttribute('aria-modal','true');drawer.setAttribute('aria-label','Trader menu');
  document.querySelector('.np-trader-backdrop').hidden=false;
  document.querySelectorAll('[data-np-trader-menu]').forEach(b=>b.setAttribute('aria-expanded','true'));
  document.querySelectorAll('.app>main,.np-mobile-header,.np-trader-dock').forEach(node=>{inerted.set(node,node.inert);node.inert=true;});
  document.body.style.overflow='hidden';
  const search=drawer.querySelector('[data-np-menu-search]');search.value='';filterMenu('');search.focus();
 }
 function filterMenu(query){
  const aside=document.querySelector('.app>.sidebar');if(!aside)return;
  let count=0;const q=query.trim().toLowerCase();
  aside.querySelectorAll('.nav-item').forEach(b=>{const visible=!q||b.textContent.toLowerCase().includes(q);b.dataset.npFiltered=String(!visible);if(visible)count++;});
  aside.querySelectorAll('.nav-section').forEach(section=>{
   let node=section.nextElementSibling,visible=false;
   while(node&&!node.classList.contains('nav-section')){if(node.matches('.nav-item')&&node.dataset.npFiltered!=='true')visible=true;node=node.nextElementSibling;}
   section.dataset.npFiltered=String(!visible);
  });
  const empty=aside.querySelector('.np-mobile-menu-empty');if(empty)empty.hidden=count>0;
 }
 function installTraderShell(app,aside){
  if(!aside.id)aside.id='np-trader-sidebar';
  if(!aside.querySelector('.np-trader-drawer-head')){
   aside.insertAdjacentHTML('afterbegin','<div class="np-trader-drawer-head"><div class="np-trader-drawer-heading"><span>Trader workspace</span><button type="button" data-np-menu-close aria-label="Close trader menu">×</button></div><input type="search" data-np-menu-search placeholder="Find a tool…" aria-label="Find a trader tool" autocomplete="off"></div>');
   aside.querySelector('.nav')?.insertAdjacentHTML('beforeend','<p class="np-mobile-menu-empty" role="status" hidden>No tools match your search.</p>');
  }
  if(!app.querySelector('.np-mobile-header')){
   const menu='<button type="button" class="np-mobile-menu-button" data-np-trader-menu aria-controls="'+aside.id+'" aria-expanded="false"><span aria-hidden="true">☰</span> Menu</button>';
   app.insertAdjacentHTML('afterbegin','<header class="np-mobile-header"><div><div class="np-mobile-wordmark">NAIRA<span>PIPS</span></div><div class="np-mobile-caption">Trader workspace</div></div>'+menu+'</header>');
   app.insertAdjacentHTML('beforeend','<button type="button" class="np-trader-backdrop" data-np-menu-close aria-label="Close trader menu" hidden></button><nav class="np-trader-dock" aria-label="Trader quick navigation">'+[['dashboard','Home','home'],['accounts','Accounts','accounts'],['purchase','Buy a plan','purchase'],['payouts','Payouts','payouts']].map(([tab,label,icon])=>'<button type="button" data-np-go="'+tab+'"><span aria-hidden="true">'+icons[icon]+'</span><span>'+label+'</span></button>').join('')+'<button type="button" data-np-trader-menu aria-controls="'+aside.id+'" aria-expanded="false"><span aria-hidden="true">'+icons.menu+'</span><span>More</span></button></nav>');
  }
  const tab=app.querySelector('main[data-module]')?.dataset.module;
  app.querySelectorAll('[data-np-go]').forEach(b=>{
   const active=b.dataset.npGo===tab;b.classList.toggle('active',active);
   if(active)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');
  });
  const more=app.querySelector('.np-trader-dock [data-np-trader-menu]');
  more.classList.toggle('active',!!tab&&!['dashboard','accounts','purchase','payouts'].includes(tab));
  if(more.classList.contains('active'))more.setAttribute('aria-current','page');else more.removeAttribute('aria-current');
  aside.querySelectorAll('.nav-item').forEach(b=>{if(b.classList.contains('active'))b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});
  // Preserve unread notices in the compact navigation as well as in the full menu.
  const unread=aside.querySelector('.np-announcement-blink');
  const badge=more.querySelector('.np-mobile-nav-badge');
  if(unread&&!badge){const node=document.createElement('span');node.className='np-mobile-nav-badge';node.textContent=unread.textContent;more.append(node);}
  else if(!unread&&badge)badge.remove();
 }
 function enhanceTables(){
  document.querySelectorAll('.app main table,.app .main table,.modal-card table').forEach(table=>{
   // Keep small two-column key/value and plan comparison tables as designed.
   const row=table.tHead?.rows[0]||table.rows[0];
   if(!row||row.cells.length<3)return;
   if(!table.parentElement.classList.contains('np-table-scroll')){
    const wrap=document.createElement('div');wrap.className='np-table-scroll';
    wrap.tabIndex=0;wrap.setAttribute('role','region');
    const heading=table.closest('section,.card,.vault')?.querySelector('h2,h3,.card-title');
    wrap.setAttribute('aria-label',(heading?.textContent.trim()||'Data table')+' — scroll horizontally');
    table.before(wrap);wrap.append(table);
    const hint=document.createElement('div');hint.className='np-table-hint';hint.setAttribute('aria-hidden','true');
    hint.innerHTML='<span>Swipe to see all columns</span><span>↔</span>';wrap.before(hint);
   }
   const wrap=table.parentElement,hint=wrap.previousElementSibling;
   if(hint?.classList.contains('np-table-hint'))hint.hidden=!mq.matches||wrap.scrollWidth<=wrap.clientWidth+1;
  });
 }
 function refresh(){
  frame=0;
  if(drawer&&!drawer.isConnected)closeMenu(false);
  const app=document.querySelector('.app'),aside=app?.querySelector(':scope>.sidebar');
  if(aside)installTraderShell(app,aside);
  document.querySelectorAll('.mobile-nav [data-mod],#np-admin-sidebar [data-mod]').forEach(b=>{if(b.classList.contains('active'))b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});
  const search=document.querySelector('#topbar .search-trigger');
  if(search&&!search.hasAttribute('role')){search.setAttribute('role','button');search.tabIndex=0;search.setAttribute('aria-label','Search traders and accounts');}
  enhanceTables();
 }
 function schedule(){if(!frame)frame=requestAnimationFrame(refresh);}
 function boot(){
  document.addEventListener('click',e=>{
   const button=e.target.closest('button');if(!button)return;
   if(button.hasAttribute('data-np-trader-menu')){openMenu(button);return;}
   if(button.hasAttribute('data-np-menu-close')){closeMenu();return;}
   if(button.hasAttribute('data-np-go')){
    closeMenu(false);if(typeof setTab==='function')setTab(button.dataset.npGo);
    window.scrollTo({top:0,behavior:'instant'});schedule();return;
   }
   if(drawer&&drawer.contains(button)&&button.matches('.nav-item,.btn-signout,.np-mobile-security-shortcut')){closeMenu(false);window.scrollTo({top:0,behavior:'instant'});}
  },true);
  document.addEventListener('input',e=>{if(e.target.matches('[data-np-menu-search]'))filterMenu(e.target.value);});
  document.addEventListener('keydown',e=>{
   if(e.target.matches('#topbar .search-trigger')&&(e.key==='Enter'||e.key===' ')){e.preventDefault();e.target.click();}
   if(!drawer)return;
   if(e.key==='Escape'){e.preventDefault();closeMenu();return;}
   if(e.key!=='Tab')return;
   const nodes=[...drawer.querySelectorAll('button,input,a[href],[tabindex="0"]')].filter(n=>!n.disabled&&n.getClientRects().length);
   const first=nodes[0],last=nodes[nodes.length-1];
   if(e.shiftKey&&document.activeElement===first){e.preventDefault();last?.focus();}
   else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first?.focus();}
  });
  window.addEventListener('resize',()=>{if(!mq.matches)closeMenu(false);schedule();});
  // Include dialogs and their asynchronously loaded content outside #root.
  new MutationObserver(schedule).observe(document.body,{childList:true,subtree:true});
  refresh();
 }
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
