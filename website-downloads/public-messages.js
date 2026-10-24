/* Public messages hide server internals; API authorization remains server-side. */
(function(){
  const technical=/onrender\.com|supabase\.(?:co|com)|\[Errno\s*\d+\]|Traceback|Resource temporarily unavailable|SQLSTATE|psycopg|PostgREST|\/home\/|\/workspace\/|stack trace/i;
  function message(value){return typeof value==='string'&&technical.test(value)?'This service is temporarily unavailable. Please try again shortly.':value;}
  window.npPublicMessage=message;
  const original=window.fetch;
  window.fetch=async function(input,options){
    const response=await original.call(this,input,options);
    let url;try{url=new URL(typeof input==='string'?input:input.url,location.href);}catch(_){return response;}
    if(url.origin!=='https://nairapips-api.onrender.com')return response;
    const json=response.json.bind(response);
    response.json=async function(){
      const data=await json();
      if(data&&typeof data==='object'&&!Array.isArray(data)&&(!response.ok||data.success===false||data.error)){
        for(const key of ['error','message','details','hint'])if(typeof data[key]==='string')data[key]=message(data[key]);
      }
      return data;
    };
    return response;
  };
})();
