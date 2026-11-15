const net = require("net");
const WebSocket = require("ws");

const WSPORT = 8765;
const wss = new WebSocket.Server({ port: WSPORT });
console.log(`MH OBD Bridge running on ws://127.0.0.1:${WSPORT}`);

const sleep = ms => new Promise(r => setTimeout(r, ms));

function cleanHex(s){
  return s.replace(/[\r\n\s>]/g,'').toUpperCase();
}
function findBytes(raw){
  const h=cleanHex(raw);
  const out=[];
  for(let i=0;i<h.length-1;i+=2){
    const b=parseInt(h.slice(i,i+2),16);
    if(Number.isFinite(b)) out.push(b);
  }
  return out;
}
function parseDTC(raw){
  let h=cleanHex(raw);
  // Mode 03 responses commonly begin with 43.
  const idx=h.indexOf("43");
  if(idx<0)return [];
  h=h.slice(idx+2);
  const codes=[];
  for(let i=0;i+3<h.length;i+=4){
    const b1=parseInt(h.slice(i,i+2),16), b2=parseInt(h.slice(i+2,i+4),16);
    if((b1===0 && b2===0) || !Number.isFinite(b1)||!Number.isFinite(b2))continue;
    const types=['P','C','B','U'];
    const type=types[(b1>>6)&3];
    const digit=(b1>>4)&3;
    const tail=((b1&0x0f)<<8)|b2;
    const code=type+digit+tail.toString(16).toUpperCase().padStart(3,'0');
    codes.push({code,description:describeDTC(code),raw:"Mode 03"});
  }
  return codes;
}
function describeDTC(code){
  const d={
    P0300:"خلل في الاشتعال العشوائي / متعدد الأسطوانات",
    P0301:"خلل في الاشتعال — الأسطوانة 1",
    P0302:"خلل في الاشتعال — الأسطوانة 2",
    P0303:"خلل في الاشتعال — الأسطوانة 3",
    P0304:"خلل في الاشتعال — الأسطوانة 4",
    P0171:"خليط فقير — النظام 1",
    P0420:"كفاءة نظام المحول الحفاز أقل من الحد المطلوب"
  };
  return d[code] || "كود أعطال OBD-II — يلزم فحص وصف الشركة المصنعة";
}

function parsePID(pid, raw){
  const h=cleanHex(raw);
  const key=pid.toUpperCase();
  const responses={};
  const pos=h.indexOf("41"+key);
  if(pos<0)return null;
  const bytes=findBytes(h.slice(pos+4));
  if(key==="0C" && bytes.length>=2)return {pid:"rpm",value:((bytes[0]*256+bytes[1])/4),unit:"rpm"};
  if(key==="0D" && bytes.length>=1)return {pid:"speed",value:bytes[0],unit:"km/h"};
  if(key==="05" && bytes.length>=1)return {pid:"temp",value:bytes[0]-40,unit:"°C"};
  if(key==="11" && bytes.length>=1)return {pid:"throttle",value:+(bytes[0]*100/255).toFixed(1),unit:"%"};
  if(key==="10" && bytes.length>=2)return {pid:"maf",value:+((bytes[0]*256+bytes[1])/100).toFixed(2),unit:"g/s"};
  if(key==="0B" && bytes.length>=1)return {pid:"map",value:bytes[0],unit:"kPa"};
  if(key==="42" && bytes.length>=2)return {pid:"volt",value:+((bytes[0]*256+bytes[1])/1000).toFixed(3),unit:"V"};
  return null;
}

wss.on("connection", ws=>{
  let socket=null;
  let queue=Promise.resolve();
  const send=o=>{try{ws.send(JSON.stringify(o))}catch{}};

  function enqueue(fn){queue=queue.then(fn).catch(e=>send({type:"error",message:e.message}));return queue;}

  function writeELM(cmd){
    return new Promise((resolve,reject)=>{
      if(!socket || socket.destroyed)return reject(new Error("OBD غير متصل"));
      let buf="";
      const timer=setTimeout(()=>{socket.removeListener("data",onData);reject(new Error("Timeout: "+cmd))},3000);
      const onData=d=>{
        buf+=d.toString("ascii");
        if(buf.includes(">")){
          clearTimeout(timer);socket.removeListener("data",onData);resolve(buf);
        }
      };
      socket.on("data",onData);
      socket.write(cmd+"\r");
    });
  }

  async function initialize(){
    const init=["ATZ","ATE0","ATL0","ATS0","ATH0","ATSP0"];
    for(const c of init){try{await writeELM(c);await sleep(150)}catch(e){}}
    send({type:"status",message:"تم تهيئة ELM327 وبدء الاتصال بالسيارة"});
  }

  ws.on("message",msg=>{
    let m;try{m=JSON.parse(msg.toString())}catch{return}
    if(m.type==="connect"){
      enqueue(async()=>{
        if(socket)try{socket.destroy()}catch{}
        socket=new net.Socket();
        socket.setTimeout(6000);
        socket.on("error",e=>send({type:"error",message:e.message}));
        socket.on("close",()=>send({type:"status",message:"تم فصل قطعة OBD"}));
        await new Promise((resolve,reject)=>{
          socket.once("connect",resolve);socket.once("error",reject);
          socket.connect(Number(m.port||35000),String(m.host||"192.168.0.10"));
        });
        send({type:"status",message:"تم الاتصال بقطعة KINGBOLEN"});
        await initialize();
      });
    } else if(m.type==="command"){
      enqueue(async()=>{
        const cmd=String(m.cmd||"").trim().toUpperCase();
        const raw=await writeELM(cmd);
        send({type:"data",raw});
        if(cmd==="03"){
          send({type:"dtc",codes:parseDTC(raw)});
        }else if(/^01[0-9A-F]{2}$/.test(cmd)){
          const parsed=parsePID(cmd.slice(2),raw);
          if(parsed)send({type:"pid",...parsed});
        }
      });
    } else if(m.type==="disconnect"){
      enqueue(async()=>{if(socket)socket.end()});
    }
  });
});
