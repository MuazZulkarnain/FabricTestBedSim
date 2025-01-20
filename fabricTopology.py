#!/usr/bin/env python3

import sys  # Import sys to use sys.argv[0]
from mininet.net import Mininet
from mininet.node import Controller, OVSKernelSwitch
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import os

def fabricTopology():
    net = Mininet(controller=Controller, switch=OVSKernelSwitch, link=TCLink, autoSetMacs=True)

    info("*** Creating controller\n")
    c0 = net.addController('c0')

    info("*** Creating hosts\n")

    # Orderer Organization
    orderer_org = []
    for i in range(1, 4):
        orderer = net.addHost(f'orderer{i}', ip=f'10.0.1.{i}/24')
        orderer_org.append(orderer)

    # Peer Organizations
    peer_orgs = []
    for org in range(1, 4):
        subnet_num = org + 1
        endorser = net.addHost(f'endorser{org}', ip=f'10.0.{subnet_num}.1/24')
        committer = net.addHost(f'committer{org}', ip=f'10.0.{subnet_num}.2/24')
        peer_orgs.append((endorser, committer))

    info("*** Creating switches\n")
    switches = []
    for i in range(1, 5):
        switch = net.addSwitch(f's{i}')
        switches.append(switch)

    info("*** Creating router\n")
    router = net.addHost('router')

    info("*** Creating links\n")

    # Connect orderer nodes to switch s1
    for orderer in orderer_org:
        net.addLink(orderer, switches[0])

    # Connect peer organization nodes to their respective switches
    for idx, (endorser, committer) in enumerate(peer_orgs):
        net.addLink(endorser, switches[idx+1])
        net.addLink(committer, switches[idx+1])

    # Connect switches to router
    for idx, switch in enumerate(switches):
        net.addLink(switch, router)

    info("*** Building network\n")
    net.build()
    c0.start()
    for switch in switches:
        switch.start([c0])

    info("*** Configuring router\n")

    # Enable IP forwarding on the router
    router.cmd('sysctl -w net.ipv4.ip_forward=1')

    # Assign IP addresses to router interfaces
    for idx, switch in enumerate(switches):
        iface = f'router-eth{idx}'
        subnet_num = idx + 1  # Subnets 10.0.1.0/24 to 10.0.4.0/24
        router_ip = f'10.0.{subnet_num}.254/24'
        router.cmd(f'ifconfig {iface} {router_ip}')

    info("*** Setting up default routes on hosts\n")

    # Configure default routes for orderer nodes
    for orderer in orderer_org:
        orderer.cmd('ip route flush default')
        orderer.cmd('ip route add default via 10.0.1.254')

    # Configure default routes for peer nodes
    for idx, (endorser, committer) in enumerate(peer_orgs):
        subnet_num = idx + 2  # Subnets 10.0.2.0/24 to 10.0.4.0/24
        gw_ip = f'10.0.{subnet_num}.254'
        for host in [endorser, committer]:
            host.cmd('ip route flush default')
            host.cmd(f'ip route add default via {gw_ip}')

    info("*** Creating log directories\n")
    base_log_dir = 'logs'
    if not os.path.exists(base_log_dir):
        os.makedirs(base_log_dir)

    def create_log_dir(node_name):
        node_log_dir = os.path.join(base_log_dir, node_name)
        if not os.path.exists(node_log_dir):
            os.makedirs(node_log_dir)
        return node_log_dir

    # Get the absolute path to the directory containing the scripts
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))  # Use sys.argv[0]

    info("*** Starting node processes\n")

    # Start committer node processes
    for committer in [committer for _, committer in peer_orgs]:
        node_name = committer.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'committer_node.py')
        committer.cmd(f'python3 {script_path} > {log_file} 2>&1 &')

    # Collect committer IPs (assuming one committer node for simplicity)
    committer_ips = [committer.IP() for _, committer in peer_orgs]
    committer_ip = committer_ips[0]  # For simplicity, use the first committer's IP for all orderers

    # Start orderer node processes, providing the committer's IP
    for orderer in orderer_org:
        node_name = orderer.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'orderer_node.py')
        orderer.cmd(f'python3 {script_path} {committer_ip} > {log_file} 2>&1 &')

    # Let the orderers and committers start
    time.sleep(2)

    # Start endorser node processes, providing the IPs of all orderer nodes
    orderer_ips = [orderer.IP() for orderer in orderer_org]
    orderer_ips_str = ','.join(orderer_ips)
    for endorser in [endorser for endorser, _ in peer_orgs]:
        node_name = endorser.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'endorser_node.py')
        endorser.cmd(f'python3 {script_path} {orderer_ips_str} > {log_file} 2>&1 &')

    info("*** All node processes started\n")

    # Let the simulation run for a specified duration
    simulation_time = 60  # Increased to 60 seconds
    info(f"*** Running simulation for {simulation_time} seconds\n")
    time.sleep(simulation_time)

    info("*** Stopping network\n")
    net.stop()

    info("*** Simulation complete. Logs are stored in the 'logs' directory.\n")

if __name__ == '__main__':
        setLogLevel('info')
        fabricTopology()