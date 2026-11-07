var P="/svc/";
var K=null;

async function key(){
  if(K)return K;
  var c=await caches.open("uv-k");
  var hit=await c.match("/__key");
  if(hit){K=await hit.text();return K}
  var r=await fetch("/api/key",{cache:"no-store"});
  K=await r.text();
  await c.put("/__key",new Response(K));
  return K;
}
function dec(code,k){
  var s=atob(code.replace(/-/g,"+").replace(/_/g,"/")),b=new Uint8Array(s.length),i;
  for(i=0;i<s.length;i++)b[i]=s.charCodeAt(i)^k.charCodeAt(i%k.length);
  return new TextDecoder().decode(b);
}

function fr(t,id,p){
  p=p||new Uint8Array(0);
  var b=new Uint8Array(5+p.length),dv=new DataView(b.buffer);
  dv.setUint8(0,t);dv.setUint32(1,id);b.set(p,5);
  return b.buffer;
}

var mux={
  ws:null,ready:null,id:0,p:new Map(),
  conn:function(){
    if(this.ready)return this.ready;
    var self=this;
    this.ready=new Promise(function(res,rej){
      var ws=new WebSocket(location.origin.replace(/^http/,"ws")+"/ws");
      ws.binaryType="arraybuffer";
      ws.onopen=function(){self.ws=ws;res()};
      ws.onmessage=function(e){self.onMsg(e.data)};
      ws.onclose=ws.onerror=function(){
        self.ready=null;self.ws=null;
        self.p.forEach(function(st){if(st.rej)st.rej(new Error("ws"))});
        self.p.clear();
        rej(new Error("ws"));
      };
    });
    return this.ready;
  },
  onMsg:function(buf){
    if(typeof buf==="string")return;
    var dv=new DataView(buf),t=dv.getUint8(0),id=dv.getUint32(1),p=new Uint8Array(buf,5);
    var st=this.p.get(id);if(!st)return;
    if(t===2){st.res(JSON.parse(new TextDecoder().decode(p)))}
    else if(t===3){st.push(p)}
    else if(t===4){st.close();this.p.delete(id)}
    else if(t===5){st.err(new Error(new TextDecoder().decode(p)));this.p.delete(id)}
  },
  req:async function(method,url,headers,body){
    await this.conn();
    var id=++this.id,self=this;
    var st={};
    var stream=new ReadableStream({
      start:function(c){st.push=function(v){c.enqueue(v)};st.close=function(){c.close()};st.err=function(e){c.error(e)}},
      cancel:function(){try{self.ws.send(fr(6,id))}catch(e){}}
    });
    var head=new Promise(function(res,rej){st.res=res;st.rej=rej});
    this.p.set(id,st);
    this.ws.send(fr(1,id,new TextEncoder().encode(JSON.stringify({method:method,url:url,headers:headers}))));
    if(body){
      var rd=body.getReader();
      for(;;){var r=await rd.read();if(r.done)break;this.ws.send(fr(3,id,r.value))}
    }
    this.ws.send(fr(4,id));
    var meta=await head;
    return {status:meta.status,headers:meta.headers,stream:stream};
  }
};

async function unframe(res){
  var rd=res.body.getReader(),buf=new Uint8Array(0);
  async function need(n){while(buf.length<n){var r=await rd.read();if(r.done)throw 0;
    var t=new Uint8Array(buf.length+r.value.length);t.set(buf);t.set(r.value,buf.length);buf=t}}
  await need(4);
  var len=new DataView(buf.buffer,buf.byteOffset,4).getUint32(0);
  await need(4+len);
  var hd=JSON.parse(new TextDecoder().decode(buf.subarray(4,4+len)));
  var rest=buf.subarray(4+len);
  return {status:hd.status,headers:hd.headers,stream:new ReadableStream({
    start:function(c){if(rest.length)c.enqueue(rest)},
    pull:async function(c){var r=await rd.read();if(r.done)c.close();else c.enqueue(r.value)},
    cancel:function(x){rd.cancel(x)}
  })};
}

async function viaHttp(method,url,headers,body){
  var h=new Headers(headers||{});
  h.set("x-uv-target",encodeURIComponent(url));
  var init={method:method,headers:h,redirect:"manual"};
  if(body)init.body=body;
  var res=await fetch("/api/fetch",init);
  return unframe(res);
}

function build(status,headers,stream){
  var h=new Headers();
  for(var k in headers){if(k==="set-cookie"){headers[k].forEach(function(c){h.append(k,c)});continue}h.set(k,headers[k])}
  return new Response([204,205,304].indexOf(status)>-1?null:stream,{status:status,headers:h});
}

self.addEventListener("install",function(){self.skipWaiting()});
self.addEventListener("activate",function(e){e.waitUntil(self.clients.claim())});

self.addEventListener("fetch",function(e){
  var u=new URL(e.request.url);
  if(u.origin!==location.origin||u.pathname.indexOf(P)!==0)return;
  e.respondWith((async function(){
    var k=await key();
    var t;
    try{t=dec(u.pathname.slice(P.length),k)}
    catch(err){K=null;await caches.delete("uv-k");k=await key();t=dec(u.pathname.slice(P.length),k)}
    var h={};
    e.request.headers.forEach(function(v,kk){if(["host","accept-encoding","x-uv-target"].indexOf(kk)<0)h[kk]=v});
    var body=null;
    if(e.request.method!=="GET"&&e.request.method!=="HEAD")body=await e.request.arrayBuffer();
    try{
      var r=await mux.req(e.request.method,t,h,body?new Blob([body]).stream():null);
      return build(r.status,r.headers,r.stream);
    }catch(err){
      var f=await viaHttp(e.request.method,t,h,body);
      return build(f.status,f.headers,f.stream);
    }
  })());
});
