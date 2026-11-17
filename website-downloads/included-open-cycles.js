/* Owner-configured unlimited plans include their cycles in the base price. */
(function(){
 if(typeof npBuyerRender==='function'){
  const render=npBuyerRender;
  npBuyerRender=function(catalog,...args){
   const result=render(catalog,...args);
   if(Number(catalog?.default_cycles)===0&&catalog?.cycle_options?.length===1){
    const toggle=document.getElementById('pur-cycle-toggle');
    if(toggle){toggle.checked=false;toggle.disabled=true;const label=toggle.closest('label');if(label){label.hidden=true;label.style.display='none';}}
   }
   return result;
  };
 }
})();
