#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import Controller, Host
from mininet.cli import CLI
from mininet.log import setLogLevel, info

class RootHost(Host):
    """A host that lives in the Root Namespace (Your actual Fedora OS)"""
    def __init__(self, *args, **kwargs):
        kwargs['inNamespace'] = False
        super(RootHost, self).__init__(*args, **kwargs)

def setup_topology():
    net = Mininet(controller=Controller)
    info('*** Adding controller\n')
    net.addController('c0')
    
    info('*** Adding Gateway & Sensor Nodes\n')
    # This gateway connects Mininet to your Host OS
    gateway = net.addHost('gw', cls=RootHost, ip='10.0.0.254/24')
    
    # These are the isolated IoT sensors
    s1 = net.addHost('h1', ip='10.0.0.1/24')
    s2 = net.addHost('h2', ip='10.0.0.2/24')
    s3 = net.addHost('h3', ip='10.0.0.3/24')
    
    info('*** Adding switch\n')
    sw = net.addSwitch('s1')
    
    info('*** Creating links\n')
    net.addLink(gateway, sw)
    net.addLink(s1, sw)
    net.addLink(s2, sw)
    net.addLink(s3, sw)
    
    info('*** Starting network\n')
    net.start()
    
    # Force standard L2 switching
    sw.cmd('ovs-ofctl add-flow s1 action=NORMAL')
    
    info('*** Verifying connectivity to Gateway\n')
    net.ping([s1, s2, s3, gateway])
    
    info('*** Starting Sensor Nodes\n')
    s1.cmd('python3 sensor_node.py 1 &')
    s2.cmd('python3 sensor_node.py 2 &')
    s3.cmd('python3 sensor_node.py 3 &')
    
    info('\n======================================================\n')
    info('*** Mininet Network is UP!\n')
    info('*** To inject faults: h2 tc qdisc add dev h2-eth0 root netem delay 200ms loss 15%\n')
    info('======================================================\n')
    
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    setup_topology()
