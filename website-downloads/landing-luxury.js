/* Approved NairaPips luxury landing skin. No financial writes or checkout changes. */
(function(){
'use strict';
const VERSION='luxury-landing-20261108-v1';
if(window.NPLuxuryLanding?.version===VERSION)return;
function install(){
 if(!document.getElementById('challenge-plans'))return;
 document.documentElement.classList.add('np-luxury-landing');
 let style=document.getElementById('np-luxury-landing-style');
 if(!style){style=document.createElement('style');style.id='np-luxury-landing-style';document.head.appendChild(style)}
 style.textContent=window.NPLuxuryThemeCSS||'';
 document.title='NairaPips | Trade with distinction';
 const description=document.querySelector('meta[name="description"]');
 if(description)description.content='Explore NairaPips trading accounts. Compare account sizes, prices, drawdown rules and payout terms, and choose the plan that suits your ambition.';
 const nav=document.getElementById('nav');
 if(nav&&!nav.querySelector('.np-lux-nav-inner'))nav.innerHTML='<div class="container np-lux-nav-inner"><a class="np-lux-brand" href="#" aria-label="NairaPips home"><svg class="np-lux-monogram" viewBox="0 0 100 80" aria-hidden="true"><defs><linearGradient id="npLogoGold" x2="1" y2="1"><stop stop-color="#fff3bd"/><stop offset=".48" stop-color="#deb056"/><stop offset="1" stop-color="#a16d19"/></linearGradient></defs><text x="5" y="61" font-family="Georgia,serif" font-size="69" letter-spacing="-16" fill="url(#npLogoGold)">NP</text><path d="M1 72 97 24" stroke="#e8be69" stroke-width="1.4"/></svg><span class="np-lux-wordmark">NAIRAPIPS</span></a><div class="np-lux-nav-links"><a href="#challenge-plans">Accounts</a><a href="#journey">How it works</a><a href="#connect">Support</a></div><div class="np-lux-auth"><a class="btn btn-ghost" href="/dashboard/">Sign in</a><a class="btn btn-primary np-lux-explore" href="#challenge-plans">Explore accounts</a></div></div>';
 if(!document.getElementById('np-luxury-hero')){
  const hero=document.createElement('section');hero.id='np-luxury-hero';hero.className='np-lux-hero';
  hero.innerHTML='<div class="container"><div class="np-lux-kicker">THE NAIRAPIPS COLLECTION</div><h1>Trade with <span>distinction.</span></h1><p>Choose the account and trading rules that suit your ambition.</p></div>';
  const existing=document.querySelector('.np-sales-hero');
  if(existing)existing.before(hero);else document.getElementById('challenge-plans').before(hero);
 }
 const golden=document.getElementById('golden-ticket-option');
 if(golden&&!golden.querySelector('.np-lux-golden'))golden.innerHTML='<div class="container np-lux-golden"><div><div class="np-lux-kicker">GOLDEN TICKET</div><h2>Another way to begin.</h2><p>Explore the Golden Ticket programme and review its current entry rules.</p></div><a class="btn btn-ghost" href="/capital-selection.html#register">Explore Golden Ticket ↗</a></div>';
 const final=document.querySelector('section.final');
 if(final&&!final.querySelector('.np-lux-final'))final.innerHTML='<div class="container np-lux-final"><div class="np-lux-kicker">YOUR NEXT CHAPTER</div><h2>Make your next move.</h2><p>Find the account that fits your ambition.</p><div class="np-lux-final-actions"><a class="btn btn-primary" href="#challenge-plans">Explore accounts</a><a class="btn btn-ghost" href="/dashboard/">Sign in</a></div></div>';
 const journey=document.getElementById('journey');
 if(journey&&!journey.querySelector('.np-lux-journey'))journey.innerHTML='<div class="container np-lux-journey"><div class="np-lux-kicker">A CLEAR PATH FORWARD</div><h2>Your journey, thoughtfully defined.</h2><div class="np-lux-steps"><article><small>01</small><h3>Find your account.</h3><p>Compare the available account sizes and packages.</p></article><article><small>02</small><h3>Know your terms.</h3><p>Review your selected plan’s drawdown, targets and payout rules.</p></article><article><small>03</small><h3>Make it yours.</h3><p>Create your account and follow the purchase and approval steps.</p></article><article><small>04</small><h3>Trade your plan.</h3><p>Follow your account rules and request payouts when eligible.</p></article></div></div>';
}
window.NPLuxuryLanding={version:VERSION,install};
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',install,{once:true});else install();
})();
