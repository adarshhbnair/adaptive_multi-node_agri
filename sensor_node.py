import subprocess
import time
import json
import random
import sys
import paho.mqtt.client as mqtt
import asyncio
from aiocoap import Message, Code
import aiocoap

SERVER_IP = "10.0.0.254"
NODE_ID = sys.argv[1] if len(sys.argv) > 1 else "1"

class AdaptiveSensorNode:
    def __init__(self, server_ip, node_id):
        self.server_ip = server_ip
        self.node_id = node_id
        self.protocol = "MQTT"
        self.latency = 0.0
        self.packet_loss = 0.0
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        
    def check_network(self):
        try:
            res = subprocess.run(['ping', '-c', '5', '-i', '0.2', self.server_ip], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if "rtt min/avg/max/mdev" in res.stdout:
                self.latency = float(res.stdout.split("rtt min/avg/max/mdev =")[1].split("/")[1].strip())
            else:
                self.latency = 500.0
            if "packet loss" in res.stdout:
                line = [l for l in res.stdout.split('\n') if 'packet loss' in l][0]
                self.packet_loss = float(line.split(',')[2].replace('packet loss', '').replace('%', '').strip())
            else:
                self.packet_loss = 100.0
        except:
            self.latency, self.packet_loss = 1000.0, 100.0

    def switch(self):
        self.protocol = "CoAP" if (self.packet_loss > 5.0 or self.latency > 150.0) else "MQTT"

    def get_payload(self):
        data = {"node_id": self.node_id, "timestamp": time.time(), "protocol": self.protocol, "payload_size": 0, "temp": random.uniform(20, 35), "soil": random.uniform(30, 70)}
        
        # FIX 1: Include latency and packet loss in the data sent to the server!
        data["latency"] = self.latency
        data["packet_loss"] = self.packet_loss
        
        data["raw"] = "X" * random.randint(50, 200)
        js = json.dumps(data)
        data["payload_size"] = len(js)
        return js, data

    def send_mqtt(self, js, d):
        try:
            self.mqtt_client.connect(self.server_ip, 1883, 5)
            self.mqtt_client.publish("agri/data", js)
            self.mqtt_client.disconnect()
        except: pass

    async def send_coap(self, js, d):
        try:
            ctx = await aiocoap.Context.create_client_context()
            await ctx.request(Message(code=Code.POST, uri=f'coap://{self.server_ip}/agri/data', payload=js.encode())).response
        except: pass

    def run(self):
        time.sleep(3)
        while True:
            self.check_network()
            self.switch()
            js, d = self.get_payload()
            print(f"Node {self.node_id} | {self.latency:.1f}ms, {self.packet_loss:.0f}% loss | {self.protocol}")
            if self.protocol == "MQTT": self.send_mqtt(js, d)
            else: asyncio.run(self.send_coap(js, d))
            time.sleep(3)

if __name__ == '__main__':
    AdaptiveSensorNode(SERVER_IP, NODE_ID).run()