const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync('website-downloads/trader_entry_redirect.html','utf8');const script=[...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)][0][1];
for(const[search,hash,path]of [['','','/dashboard/trader_clean.html'],['?ticket=ABC&email=a%40b.com','','/dashboard/trader_clean.html'],['?mode=register&ref=ABC','','/dashboard/register.html'],['','#signup','/dashboard/register.html'],['?setup_password=1','','/dashboard/trader_clean.html']]){
 let dest;vm.runInNewContext(script,{URLSearchParams,location:{search,hash,replace:x=>dest=x}});assert(dest.startsWith(path));assert(!dest.includes('onrender.com'));if(search.includes('ref='))assert(dest.includes('ref=ABC'));if(search.includes('ticket='))assert(dest.includes('ticket=ABC'));
}
console.log('Entry always opens current same-origin dashboard; signup and ticket parameters preserved');
