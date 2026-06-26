#!/usr/bin/env python3
import asyncio, json, time, threading, subprocess, os
from collections import deque
from flask import Flask, jsonify, render_template_string
import paho.mqtt.client as mqtt
from aiocoap import resource, Message, Code
import aiocoap

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import PROTOCOL_OVERHEAD, TX_POWER, RX_POWER, IDLE_POWER, PROCESSING, ACK_SIZE

app = Flask(__name__)
lock = threading.Lock()
nodes, history, switch_log = {}, {}, []
stats = {"start": time.time(), "rx": 0, "sw": 0}

CONF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mosquitto.conf')
if not os.path.exists(CONF_FILE):
    with open(CONF_FILE, 'w') as f:
        f.write("listener 1883 0.0.0.0\nallow_anonymous true\n")

print(">>> Starting Mosquitto Broker automatically on 0.0.0.0:1883...")
subprocess.Popen(['mosquitto', '-c', CONF_FILE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)

def calc_energy(psize, proto, fct, retries):
    ovh = PROTOCOL_OVERHEAD[proto]
    bits = (psize + ovh) * 8
    ack_bits = ACK_SIZE * 8
    tx = TX_POWER * bits * (1 + retries)
    rx = RX_POWER * ack_bits * (1 + retries)
    idle = IDLE_POWER * fct
    proc = PROCESSING * (1.2 if proto == "CoAP" else 1.0)
    return {"tx": tx, "rx": rx, "idle": idle, "proc": proc, "total": tx+rx+idle+proc, "ovh": ovh}

def process_data(proto, raw_payload):
    try:
        data = json.loads(raw_payload.decode())
        nid = str(data["node_id"])
        fct = time.time() - data["timestamp"]
        psize = len(raw_payload)
        en = calc_energy(psize, proto, fct, data.get("retransmissions", 0))
        eff = (psize / (psize + en["ovh"]) * 100) if psize > 0 else 0
        
        with lock:
            stats["rx"] += 1
            if nid in nodes and nodes[nid]["protocol"] != proto:
                stats["sw"] += 1
                switch_log.append({"t": time.strftime("%H:%M:%S"), "id": nid, "from": nodes[nid]["protocol"], "to": proto, "lat": data["latency"], "loss": data["packet_loss"]})
                if len(switch_log) > 50: switch_log.pop(0)
                
            nodes[nid] = {
                "protocol": proto, "latency": data["latency"], "jitter": data["jitter"], "loss": data["packet_loss"],
                "fct": fct, "psize": psize, "ovh": en["ovh"], "eff": eff,
                "retries": data.get("retransmissions",0), "total_retries": data.get("total_retries",0),
                "switches": data.get("switches",0),
                "energy": en["total"], "e_tx": en["tx"], "e_rx": en["rx"], "e_idle": en["idle"], "e_proc": en["proc"],
                "temp": data["temperature"], "soil": data["soil_moisture"], "hum": data["humidity"],
                "ph": data["ph"], "water": data["water_level"], "light": data["light"]
            }
            if nid not in history: history[nid] = {k: deque(maxlen=50) for k in ["lat","loss","fct","ene"]}
            h = history[nid]
            h["lat"].append(data["latency"]); h["loss"].append(data["packet_loss"])
            h["fct"].append(fct); h["ene"].append(en["total"])
    except Exception as e:
        print(f"[SERVER DATA ERROR] {e}")

def on_mqtt(c, u, m): 
    process_data("MQTT", m.payload)

def start_mqtt():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    c.on_message = on_mqtt
    while True:
        try: 
            c.connect("127.0.0.1", 1883, 60); break
        except: time.sleep(1)
    c.subscribe("agri/data"); c.loop_forever()

class CoapRes(resource.Resource):
    async def render_post(self, req): 
        process_data("CoAP", req.payload)
        return Message(code=Code.CREATED, payload=b"ACK")

async def start_coap():
    root = resource.Site(); root.add_resource(('agri','data'), CoapRes())
    await aiocoap.Context.create_server_context(root, bind=('0.0.0.0', 5683))
    await asyncio.get_running_loop().create_future()

HTML = """<!DOCTYPE html><html><head><title>Agri IoT</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{font-family:Arial;background:#f4f6f9;margin:0;padding:20px}
.hdr{text-align:center;color:#2c3e50}.st{text-align:center;color:#7f8c8d;margin-bottom:20px;font-size:1.2em;}
.sum{background:#fff;padding:15px;border-radius:8px;display:flex;justify-content:space-around;margin-bottom:20px;box-shadow:0 2px 5px #0001}
.sum div{text-align:center}.sum b{display:block;font-size:1.5em;color:#2c3e50}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:20px;margin-bottom:20px}
.card{background:#fff;padding:20px;border-radius:8px;box-shadow:0 2px 5px #0001;border-top:4px solid #bdc3c7}
.card.mqtt{border-color:#3498db}.card.coap{border-color:#e74c3c}
.top{display:flex;justify-content:space-between;margin-bottom:15px;padding-bottom:10px;border-bottom:1px solid #eee}
.badge{padding:4px 10px;border-radius:12px;font-weight:bold;font-size:0.85em}
.bm{background:#ebf8ff;color:#3498db}.bc{background:#ffeaea;color:#e74c3c}
.r{display:flex;justify-content:space-between;padding:6px 0;font-size:0.9em;border-bottom:1px solid #f9f9f9}
.r span:last-child{font-weight:bold}.rd{color:#e74c3c}.rg{color:#27ae60}
.charts{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
.ch{background:#fff;padding:15px;border-radius:8px;box-shadow:0 2px 5px #0001}
.log{background:#fff;padding:15px;border-radius:8px;box-shadow:0 2px 5px #0001;max-height:150px;overflow-y:auto;font-family:monospace;font-size:0.85em}
</style></head><body>
<div class="hdr"><h1>🌾 Precision Agriculture IoT Framework</h1></div>
<div class="st" id="st">Initializing...</div>
<div class="sum" id="sm"></div>
<div class="grid" id="cd"></div>
<div class="charts">
    <div class="ch"><canvas id="c1"></canvas></div>
    <div class="ch"><canvas id="c2"></canvas></div>
    <div class="ch"><canvas id="c3"></canvas></div>
    <div class="ch"><canvas id="c4"></canvas></div>
</div>
<div class="log" id="lg">Waiting for events...</div>
<script>
let c1,c2,c3,c4;
const C=['#3498db','#27ae60','#f1c40f'];

function init(){
    if(typeof Chart === 'undefined'){
        document.getElementById('st').innerText = "Graphs disabled (Offline Mode), waiting for nodes...";
        return; 
    }
    
    // Base options without titles
    const base={responsive:true,animation:{duration:0},scales:{x:{display:false},y:{beginAtZero:true}},elements:{point:{radius:0},line:{tension:0.4}},plugins:{legend:{position:'bottom'}}};

    // Add specific titles to each graph
    c1=new Chart(document.getElementById('c1'),{type:'line',data:{labels:[],datasets:[]},options:{...base, plugins:{...base.plugins, title:{display:true, text:'📊 Latency Over Time (ms)', font:{size:14}}}}});
    
    c2=new Chart(document.getElementById('c2'),{type:'line',data:{labels:[],datasets:[]},options:{...base, scales:{...base.scales, y:{...base.scales.y, max:100, title:{display:true, text:'Percentage (%)'}}}, plugins:{...base.plugins, title:{display:true, text:'📉 Packet Loss Over Time', font:{size:14}}}}});
    
    c3=new Chart(document.getElementById('c3'),{type:'line',data:{labels:[],datasets:[]},options:{...base, scales:{...base.scales, y:{...base.scales.y, title:{display:true, text:'Energy (mJ)'}}}, plugins:{...base.plugins, title:{display:true, text:'⚡ Cumulative Energy Consumption', font:{size:14}}}}});
    
    c4=new Chart(document.getElementById('c4'),{type:'line',data:{labels:[],datasets:[]},options:{...base, scales:{...base.scales, y:{...base.scales.y, title:{display:true, text:'Time (s)'}}}, plugins:{...base.plugins, title:{display:true, text:'⏱️ Flow Completion Time (FCT)', font:{size:14}}}}});
}

function uC(h){
    if(typeof Chart === 'undefined') return;
    const n=Object.keys(h);if(!n.length)return;
    const ml=Math.max(...n.map(x=>h[x].lat.length));
    const lb=Array.from({length:ml},(_,i)=>i);
    const mk=k=>n.map((id,i)=>({label:'Node '+id,data:Array.from(h[id][k]),borderColor:C[i%3],backgroundColor:C[i%3]+'22',fill:true}));
    c1.data.labels=lb;c1.data.datasets=mk('lat');c1.update();
    c2.data.labels=lb;c2.data.datasets=mk('loss');c2.update();
    c3.data.labels=lb;c3.data.datasets=mk('ene');c3.update();
    c4.data.labels=lb;c4.data.datasets=mk('fct');c4.update();
}

function u(){
    fetch('/api/d').then(r=>r.json()).then(d=>{
        fetch('/api/s').then(r=>r.json()).then(s=>{
            fetch('/api/h').then(r=>r.json()).then(h=>{
                fetch('/api/w').then(r=>r.json()).then(w=>{
                    try {
                        document.getElementById('st').innerText=Object.keys(d).length?"System Online - Live Telemetry":"Waiting for nodes...";
                        document.getElementById('sm').innerHTML='<div><b>'+s.rx+'</b>Packets Rx</div><div><b>'+s.sw+'</b>Switches</div><div><b>'+Math.floor(s.up)+'s</b>Uptime</div>';
                        let html='';
                        for(let id in d){
                            let m=d[id],c=m.protocol==='MQTT'?'mqtt':'coap',bc=m.protocol==='MQTT'?'bm':'bc',rc=(m.loss>5||m.latency>150)?'rd':'rg';
                            html+='<div class="card '+c+'"><div class="top"><h3>Node '+id+'</h3><span class="badge '+bc+'">'+m.protocol+'</span></div>';
                            html+='<div class="r"><span>Latency / Jitter</span><span class="'+rc+'">'+m.latency.toFixed(1)+' ms / '+m.jitter.toFixed(1)+' ms</span></div>';
                            html+='<div class="r"><span>Packet Loss</span><span class="'+rc+'">'+m.loss.toFixed(1)+' %</span></div>';
                            html+='<div class="r"><span>FCT / Payload</span><span>'+m.fct.toFixed(4)+'s / '+m.psize+'B</span></div>';
                            html+='<div class="r"><span>Overhead / Efficiency</span><span>'+m.ovh+'B / '+m.eff.toFixed(1)+'%</span></div>';
                            html+='<div class="r"><span>Retries (Total)</span><span class="'+(m.retries>0?'rd':'rg')+'">'+m.retries+' ('+m.total_retries+')</span></div>';
                            html+='<div class="r"><span>Total Energy</span><span>'+m.energy.toFixed(2)+' mJ</span></div>';
                            html+='<div class="r"><span>Temp / Soil</span><span>'+m.temp+'°C / '+m.soil+'%</span></div>';
                            html+='<div class="r"><span>Humidity / pH</span><span>'+m.hum+'% / '+m.ph+'</span></div>';
                            html+='<div class="r"><span>Water / Light</span><span>'+m.water+'cm / '+m.light+' lux</span></div></div>';
                        }
                        document.getElementById('cd').innerHTML=html;
                        uC(h);
                        let lg='';if(!w.length)lg='No switches yet.';else for(let i=w.length-1;i>=0;i--){let x=w[i];lg+=x.t+' | Node '+x.id+': '+x.from+' -> '+x.to+'<br>';}document.getElementById('lg').innerHTML=lg;
                    } catch(err) {
                        document.getElementById('st').innerText = "UI Error: " + err.message;
                    }
                }).catch(e => console.error(e));
            }).catch(e => console.error(e));
        }).catch(e => console.error(e));
    }).catch(e => console.error(e));
}

init(); u(); setInterval(u, 2000);
</script></body></html>"""

@app.route('/')
def idx(): return render_template_string(HTML)
@app.route('/api/d')
def apid():
    with lock: return jsonify(nodes)
@app.route('/api/s')
def apis():
    with lock: return jsonify({"rx":stats["rx"],"sw":stats["sw"],"up":time.time()-stats["start"]})
@app.route('/api/h')
def apih():
    with lock: return jsonify({k:{kk:list(vv) for kk,vv in v.items()} for k,v in history.items()})
@app.route('/api/w')
def apiw():
    with lock: return jsonify(switch_log)

if __name__ == '__main__':
    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=lambda: asyncio.run(start_coap()), daemon=True).start()
    print(">>> FLASK SERVER RUNNING AT http://localhost:5000 <<<")
    app.run(host='0.0.0.0', port=5000, use_reloader=False, threaded=True)