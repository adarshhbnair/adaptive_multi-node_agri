#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import Controller, Host
from mininet.cli import CLI
from mininet.log import setLogLevel, info
import os

class RootHost(Host):
    def __init__(self, *args, **kwargs):
        kwargs['inNamespace'] = False
        super(RootHost, self).__init__(*args, **kwargs)

def setup():
    # ==========================================
    # CRITICAL: Disable Fedora Firewall that blocks TCP 1883
    # ==========================================
    print(">>> Disabling firewalld to allow sensor connections...")
    os.system('systemctl stop firewalld 2>/dev/null')
    os.system('ip link delete gw-eth0 2>/dev/null')
    # ==========================================

    net = Mininet(controller=Controller)
    net.addController('c0')
    
    gw = net.addHost('gw', cls=RootHost, ip='10.0.0.254/24')
    s1 = net.addHost('s1', ip='10.0.0.1/24')
    s2 = net.addHost('s2', ip='10.0.0.2/24')
    s3 = net.addHost('s3', ip='10.0.0.3/24')
    sw = net.addSwitch('sw1')
    
    net.addLink(gw, sw)
    net.addLink(s1, sw)
    net.addLink(s2, sw)
    net.addLink(s3, sw)
    
    net.start()
    sw.cmd('ovs-ofctl add-flow sw1 action=NORMAL')
    
    # Run WITHOUT redirecting output so you see errors directly here!
    s1.cmd('python3 node.py 1 &')
    s2.cmd('python3 node.py 2 &')
    s3.cmd('python3 node.py 3 &')
    
    print("""
    ======================================================
    NETWORK UP! 
    ------------------------------------------------------
    Degrade Node 1:  s1 tc qdisc add dev s1-eth0 root netem delay 200ms loss 15%
    Fix Node 1:      s1 tc qdisc del dev s1-eth0 root
    ======================================================""")
    
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    setup()