// Preserve every other cPanel function while syncing these four plan editors.
const fs=require('fs'),path=require('path'),acorn=require('acorn');
const root=path.resolve(__dirname,'..','website-downloads');
const names=new Set(['showCreatePlan','createPlan','editPlan','savePlan']);
function functions(html){
 const scripts=[...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
 for(const script of scripts){
  if(!script[1].includes('pl-payout-frequency'))continue;
  const source=script[1],nodes=acorn.parse(source,{ecmaVersion:'latest'}).body;
  const found=nodes.filter(n=>n.type==='ExpressionStatement'&&n.expression.type==='AssignmentExpression'&&n.expression.left.type==='MemberExpression'&&n.expression.left.object.name==='window'&&names.has(n.expression.left.property.name));
  if(found.length!==4)throw Error('Expected exactly four plan-editor functions');
  return {script,source,found};
 }
 throw Error('Admin plan script not found');
}
const original=fs.readFileSync(path.join(root,'admin_clean.html'),'utf8');
const targetPath=path.join(root,'admin_clean_cpanel.html'),target=fs.readFileSync(targetPath,'utf8');
const from=functions(original),to=functions(target),replacements=new Map(from.found.map(n=>[n.expression.left.property.name,from.source.slice(n.start,n.end)]));
let code=to.source;
for(const n of [...to.found].sort((a,b)=>b.start-a.start))code=code.slice(0,n.start)+replacements.get(n.expression.left.property.name)+code.slice(n.end);
acorn.parse(code,{ecmaVersion:'latest'});
fs.writeFileSync(targetPath,target.replace(to.source,code));
console.log('Synced four plan-editor functions; other cPanel code preserved.');
