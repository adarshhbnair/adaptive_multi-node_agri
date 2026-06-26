#!/usr/bin/env python3
import subprocess, json, time, random, sys, os, csv, re, asyncio
import paho.mqtt.client as mqtt
from aiocoap import Message, Code
import aiocoap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import SWITCH_TO_COAP_LATENCY, SWITCH_TO_COAP_LOSS, SWITCH_TO_MQTT_LATENCY, SWITCH_TO_MQTT_LOSS

SERVER_IP = "10.0.0.254"
NODE_ID = sys.argv[1]
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

class SensorNode:
    def __init__(self, nid):
        self.nid = nid
        self.protocol = "MQTT"
        self.latency = self.jitter = self.loss = 0.0
        self.retransmissions = self.total_retries = self.switches = 0
        self.csv_path = os.path.join(DATA_DIR, f"node_{nid}.csv")
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, 'w', newline='') as f:
                csv.writer(f).writerow(['time', 'node', 'proto', 'lat', 'jitter', 'loss', 'retries'])

    def check_network(self):
        try:
            res = subprocess.run(['ping', '-c', '5', '-i', '0.2', SERVER_IP], capture_output=True, text=True, timeout=5)
            out = res.stdout
            lat_m = re.search(r"rtt min/avg/max/mdev = [\d.]+/([\d.]+)/", out)
            jit_m = re.search(r"rtt min/avg/max/mdev = [\d.]+/[\d.]+/[\d.]+/([\d.]+)", out)
            loss_m = re.search(r"(\d+)% packet loss", out)
            self.latency = float(lat_m.group(1)) if lat_m else 500.0
            self.jitter = float(jit_m.group(1)) if jit_m else 0.0
            self.loss = float(loss_m.group(1)) if loss_m else 100.0
        except:
            self.latency, self.jitter, self.loss = 1000.0, 0.0, 100.0

    def decide_protocol(self):
        prev = self.protocol
        if self.protocol == "MQTT":
            if self.loss > SWITCH_TO_COAP_LOSS or self.latency > SWITCH_TO_COAP_LATENCY:
                self.protocol = "CoAP"
        else:
            if self.loss < SWITCH_TO_MQTT_LOSS and self.latency < SWITCH_TO_MQTT_LATENCY:
                self.protocol = "MQTT"
        if prev != self.protocol:
            self.switches += 1
            print(f"\n[Node {self.nid}] ⚡ SWITCH: {prev} -> {self.protocol} (Lat:{self.latency:.1f}ms, Loss:{self.loss:.1f}%)\n")

    def send_mqtt(self, payload):
        self.retransmissions = 0
        for _ in range(3):
            try:
                c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                c.connect(SERVER_IP, 1883, 60)  # FIXED: Removed 'timeout' argument
                r = c.publish("agri/data", payload)
                c.disconnect()
                if r.rc == 0: return True
            except Exception as e:
                print(f"[Node {self.nid}] MQTT ERROR: {e}")
            self.retransmissions += 1
            time.sleep(0.1)
        self.total_retries += self.retransmissions
        return False

    async def send_coap(self, payload):
        self.retransmissions = 0
        for _ in range(3):
            try:
                ctx = await aiocoap.Context.create_client_context()
                req = ctx.request(Message(code=Code.POST, uri=f'coap://{SERVER_IP}/agri/data', payload=payload.encode()))
                res = await asyncio.wait_for(req.response, timeout=5.0)
                await ctx.shutdown()
                if res.code < 128: return True
            except Exception as e:
                print(f"[Node {self.nid}] COAP ERROR: {e}")
            self.retransmissions += 1
            await asyncio.sleep(0.1)
        self.total_retries += self.retransmissions
        return False

    def run(self):
        time.sleep(3)
        print(f"[Node {self.nid}] Online.")
        while True:
            self.check_network()
            self.decide_protocol()
            data = {
                "node_id": self.nid, "timestamp": time.time(), "protocol": self.protocol,
                "latency": self.latency, "jitter": self.jitter, "packet_loss": self.loss,
                "retransmissions": self.retransmissions, "total_retries": self.total_retries,
                "switches": self.switches,
                "temperature": round(random.uniform(20, 40), 2),
                "soil_moisture": round(random.uniform(30, 80), 2),
                "humidity": round(random.uniform(40, 90), 2),
                "ph": round(random.uniform(5.5, 7.5), 2),
                "water_level": round(random.uniform(10, 100), 2),
                "light": round(random.uniform(100, 1000), 2)
            }
            payload = json.dumps(data)
            if self.protocol == "MQTT": self.send_mqtt(payload)
            else: asyncio.run(self.send_coap(payload))
            with open(self.csv_path, 'a', newline='') as f:
                csv.writer(f).writerow([time.time(), self.nid, self.protocol, self.latency, self.jitter, self.loss, self.retransmissions])
            print(f"[Node {self.nid}] {self.protocol:4s} | Lat:{self.latency:6.1f}ms | Loss:{self.loss:4.1f}% | T:{data['temperature']}°C pH:{data['ph']}")
            time.sleep(3)

if __name__ == '__main__':
    SensorNode(NODE_ID).run()