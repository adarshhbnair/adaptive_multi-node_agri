import asyncio
import json
import time
import threading
from flask import Flask, jsonify, render_template_string
import paho.mqtt.client as mqtt
from aiocoap import resource, Message, Code
import aiocoap

app = Flask(__name__)
metrics_lock = threading.Lock()
node_metrics = {}

BASE_ENERGY = 10.0
MQTT_OVERHEAD = 20
COAP_OVERHEAD = 4
TX_POWER = 0.005

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>IoT Dashboard</title>
    <style>
        body { font-family: Arial; background: #ffffff; color: #333; margin: 0; padding: 30px; display: flex; flex-direction: column; align-items: center; }
        h1 { color: #2c3e50; }
        #status { color: #7f8c8d; margin-bottom: 25px; font-size: 1.1em; }
        .dashboard { display: flex; flex-wrap: wrap; gap: 25px; justify-content: center; }
        .card { background: #f8f9fa; padding: 25px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); width: 300px; border-left: 6px solid #bdc3c7; }
        .card.mqtt { border-left-color: #3498db; }
        .card.coap { border-left-color: #e74c3c; }
        .proto-mqtt { color: #3498db; font-weight: bold; font-size: 1.2em; }
        .proto-coap { color: #e74c3c; font-weight: bold; font-size: 1.2em; }
        .stat { margin: 12px 0; font-size: 0.95em; display: flex; justify-content: space-between; border-bottom: 1px solid #eee; padding-bottom: 5px;}
        .stat-val { font-weight: bold; }
    </style>
</head>
<body>
    <h1>Precision Agriculture IoT Emulation</h1>
    <p id="status">Initializing system...</p>
    <div class="dashboard" id="metrics"></div>

    <script>
        function update() {
            fetch('/api/metrics')
            .then(r => r.json())
            .then(data => {
                let html = '';
                const keys = Object.keys(data);
                if(keys.length === 0) {
                    document.getElementById('status').innerText = "Waiting for sensor nodes to connect...";
                    return;
                }
                document.getElementById('status').innerText = "System Online - Live Telemetry";
                
                for(let id in data) {
                    let m = data[id];
                    let cls = m.protocol === 'MQTT' ? 'mqtt' : 'coap';
                    html += `
                    <div class="card ${cls}">
                        <h3>Sensor Node ${id}</h3>
                        <p>Protocol: <span class="proto-${cls}">${m.protocol}</span></p>
                        <div class="stat"><span>Latency</span> <span class="stat-val">${m.latency.toFixed(2)} ms</span></div>
                        <div class="stat"><span>Packet Loss</span> <span class="stat-val">${m.packet_loss.toFixed(2)} %</span></div>
                        <div class="stat"><span>Avg FCT</span> <span class="stat-val">${m.avg_fct.toFixed(3)} s</span></div>
                        <div class="stat"><span>Energy Used</span> <span class="stat-val">${m.total_energy.toFixed(2)} mJ</span></div>
                        <div class="stat"><span>Payload</span> <span class="stat-val">${m.payload_size} B</span></div>
                    </div>`;
                }
                document.getElementById('metrics').innerHTML = html;
            })
            .catch(() => document.getElementById('status').innerText = "Connecting to server...");
        }
        update();
        setInterval(update, 2000);
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/metrics')
def get_metrics():
    with metrics_lock:
        return jsonify(node_metrics)

# FIX 2: Accept the full data dictionary to extract latency/packet loss
def process_data(node_id, protocol, payload_size, send_time, data):
    fct = time.time() - send_time
    with metrics_lock:
        if node_id not in node_metrics:
            node_metrics[node_id] = {"protocol": protocol, "latency": 0.0, "packet_loss": 0.0, "avg_fct": 0.0, "total_energy": 0.0, "payload_size": payload_size, "fct_history": []}
        node = node_metrics[node_id]
        node["protocol"] = protocol
        node["payload_size"] = payload_size
        
        # Update latency and packet loss from the sensor's network check
        node["latency"] = data.get("latency", 0.0)
        node["packet_loss"] = data.get("packet_loss", 0.0)
        
        overhead = MQTT_OVERHEAD if protocol == "MQTT" else COAP_OVERHEAD
        node["total_energy"] += BASE_ENERGY + (TX_POWER * (payload_size + overhead))
        node["fct_history"].append(fct)
        if len(node["fct_history"]) > 10: node["fct_history"].pop(0)
        node["avg_fct"] = sum(node["fct_history"]) / len(node["fct_history"])

def on_mqtt_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        # Pass 'data' to process_data
        process_data(data["node_id"], "MQTT", data["payload_size"], data["timestamp"], data)
    except: pass

def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_mqtt_message
    client.connect("0.0.0.0", 1883, 60)
    client.subscribe("agri/data")
    client.loop_forever()

class CoapRes(resource.Resource):
    async def render_post(self, request):
        try:
            data = json.loads(request.payload.decode())
            # Pass 'data' to process_data
            process_data(data["node_id"], "CoAP", data["payload_size"], data["timestamp"], data)
            return Message(code=Code.CREATED, payload=b"ACK")
        except: return Message(code=Code.BAD_REQUEST)

async def start_coap():
    root = resource.Site()
    root.add_resource(('agri', 'data'), CoapRes())
    await aiocoap.Context.create_server_context(root, bind=('0.0.0.0', 5683))
    await asyncio.get_running_loop().create_future()

if __name__ == '__main__':
    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=lambda: asyncio.run(start_coap()), daemon=True).start()
    print(">>> SERVER RUNNING. Open http://localhost:5000 in your browser <<<")
    app.run(host='0.0.0.0', port=5000, use_reloader=False)