const fs=require('fs'),vm=require('vm'),assert=require('assert');
const label={hidden:false,style:{}},toggle={checked:true,disabled:false,closest:()=>label};
const ctx={document:{getElementById:()=>toggle},npBuyerRender:()=>42};vm.createContext(ctx);vm.runInContext(fs.readFileSync('website-downloads/included-open-cycles.js','utf8'),ctx);
assert.equal(ctx.npBuyerRender({default_cycles:0,cycle_options:[{cycles:0}]}),42);assert(toggle.disabled);assert(!toggle.checked);assert(label.hidden);
label.hidden=false;toggle.disabled=false;ctx.npBuyerRender({default_cycles:3,cycle_options:[{cycles:3},{cycles:0}]});assert(!toggle.disabled);assert(!label.hidden);
console.log('Included unlimited hides paid-upgrade controls; finite plans keep their existing choices.');
