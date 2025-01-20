#!/usr/bin/env python3

import sys
import argparse  # For command-line argument parsing
from mininet.net import Mininet
from mininet.node import Controller, OVSKernelSwitch
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import time
import os

def fabricTopology(num_clients):
    net = Mininet(controller=Controller, switch=OVSKernelSwitch, link=TCLink, autoSetMacs=True)

    info("*** Creating controller\n")
    c0 = net.addController('c0')

    info("*** Creating hosts and switches for each organization\n")

    # Organization 1
    org1_endorser = net.addHost('endorser1', ip='10.1.1.1/24')
    org1_committer = net.addHost('committer1', ip='10.1.1.2/24')
    org1_switch = net.addSwitch('s1')

    # Organization 2
    org2_endorser = net.addHost('endorser2', ip='10.1.2.1/24')
    org2_committer = net.addHost('committer2', ip='10.1.2.2/24')
    org2_switch = net.addSwitch('s2')

    # Organization 3
    org3_endorser = net.addHost('endorser3', ip='10.1.3.1/24')
    org3_committer = net.addHost('committer3', ip='10.1.3.2/24')
    org3_switch = net.addSwitch('s3')

    # Organization 4 (Orderer Organization)
    orderer1 = net.addHost('orderer1', ip='10.1.4.1/24')
    orderer2 = net.addHost('orderer2', ip='10.1.4.2/24')
    orderer3 = net.addHost('orderer3', ip='10.1.4.3/24')
    org4_switch = net.addSwitch('s4')

    # Client Nodes (External)
    client_nodes = []
    starting_subnet = 10  # Starting subnet number for clients
    for i in range(num_clients):
        subnet_num = starting_subnet + i  # Ensure each client is on its own subnet
        client = net.addHost(f'client{i+1}', ip=f'10.1.{subnet_num}.1/24')
        client_nodes.append(client)

    client_switches = []
    for i in range(num_clients):
        switch = net.addSwitch(f'cs{i+1}')
        client_switches.append(switch)

    # Central Router
    router = net.addHost('router')

    info("*** Creating links within organizations\n")
    # Org1 Links
    net.addLink(org1_endorser, org1_switch)
    net.addLink(org1_committer, org1_switch)

    # Org2 Links
    net.addLink(org2_endorser, org2_switch)
    net.addLink(org2_committer, org2_switch)

    # Org3 Links
    net.addLink(org3_endorser, org3_switch)
    net.addLink(org3_committer, org3_switch)

    # Org4 Links (Orderer Organization)
    net.addLink(orderer1, org4_switch)
    net.addLink(orderer2, org4_switch)
    net.addLink(orderer3, org4_switch)

    info("*** Connecting client nodes to their switches\n")
    for idx, client in enumerate(client_nodes):
        net.addLink(client, client_switches[idx])

    info("*** Connecting organization and client switches to the router\n")
    net.addLink(org1_switch, router, intfName2='router-eth1')
    net.addLink(org2_switch, router, intfName2='router-eth2')
    net.addLink(org3_switch, router, intfName2='router-eth3')
    net.addLink(org4_switch, router, intfName2='router-eth4')

    for idx, cswitch in enumerate(client_switches):
        net.addLink(cswitch, router, intfName2=f'router-eth{5+idx}')

    info("*** Building network\n")
    net.build()
    c0.start()
    org1_switch.start([c0])
    org2_switch.start([c0])
    org3_switch.start([c0])
    org4_switch.start([c0])
    for cswitch in client_switches:
        cswitch.start([c0])

    info("*** Configuring router\n")
    router.cmd('sysctl -w net.ipv4.ip_forward=1')

    # Assign IP addresses to router interfaces
    router.cmd('ifconfig router-eth1 10.1.1.254/24')
    router.cmd('ifconfig router-eth2 10.1.2.254/24')
    router.cmd('ifconfig router-eth3 10.1.3.254/24')
    router.cmd('ifconfig router-eth4 10.1.4.254/24')

    for idx, client in enumerate(client_nodes):
        iface = f'router-eth{5+idx}'
        subnet_num = starting_subnet + idx
        router.cmd(f'ifconfig {iface} 10.1.{subnet_num}.254/24')

    info("*** Setting up default routes on hosts\n")
    # Org1 Hosts
    for host in [org1_endorser, org1_committer]:
        host.cmd('ip route flush default')
        host.cmd('ip route add default via 10.1.1.254')

    # Org2 Hosts
    for host in [org2_endorser, org2_committer]:
        host.cmd('ip route flush default')
        host.cmd('ip route add default via 10.1.2.254')

    # Org3 Hosts
    for host in [org3_endorser, org3_committer]:
        host.cmd('ip route flush default')
        host.cmd('ip route add default via 10.1.3.254')

    # Org4 Hosts (Orderers)
    for host in [orderer1, orderer2, orderer3]:
        host.cmd('ip route flush default')
        host.cmd('ip route add default via 10.1.4.254')

    # Client Nodes
    for idx, client in enumerate(client_nodes):
        subnet_num = starting_subnet + idx
        client.cmd('ip route flush default')
        client.cmd(f'ip route add default via 10.1.{subnet_num}.254')

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
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    info("*** Starting node processes\n")

    # Start committer node processes
    committers = [org1_committer, org2_committer, org3_committer]
    for committer in committers:
        node_name = committer.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'committer_node.py')
        committer.cmd(f'python3 {script_path} > {log_file} 2>&1 &')

    # Collect committer IPs
    committer_ips = [committer.IP() for committer in committers]
    committer_ips_str = ','.join(committer_ips)

    # Start orderer node processes, providing the IPs of all committer nodes
    orderers = [orderer1, orderer2, orderer3]
    for orderer in orderers:
        node_name = orderer.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'orderer_node.py')
        orderer.cmd(f'python3 {script_path} {committer_ips_str} > {log_file} 2>&1 &')

    # Let the orderers and committers start
    time.sleep(2)

    # Start endorser node processes, providing the IPs of all orderer nodes
    orderer_ips = [orderer.IP() for orderer in orderers]
    orderer_ips_str = ','.join(orderer_ips)

    endorsers = [org1_endorser, org2_endorser, org3_endorser]
    for endorser in endorsers:
        node_name = endorser.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'endorser_node.py')
        endorser.cmd(f'python3 {script_path} {orderer_ips_str} > {log_file} 2>&1 &')

    # Start client node processes, providing the IPs of all endorsers
    endorser_ips = [endorser.IP() for endorser in endorsers]
    endorser_ips_str = ','.join(endorser_ips)
    for client in client_nodes:
        node_name = client.name
        node_log_dir = create_log_dir(node_name)
        log_file = os.path.join(node_log_dir, f'{node_name}.log')
        script_path = os.path.join(script_dir, 'client_node.py')
        client.cmd(f'python3 {script_path} {endorser_ips_str} > {log_file} 2>&1 &')

    info("*** All node processes started\n")

    # Let the simulation run for a specified duration
    simulation_time = 60  # Adjust as needed
    info(f"*** Running simulation for {simulation_time} seconds\n")
    time.sleep(simulation_time)

    info("*** Stopping network\n")
    net.stop()

    info("*** Simulation complete. Logs are stored in the 'logs' directory.\n")

if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Mininet Hyperledger Fabric Topology')
    parser.add_argument('-c', '--clients', type=int, default=3,
                        help='Number of client nodes to spawn (default: 3)')
    args = parser.parse_args()

    setLogLevel('info')
    fabricTopology(args.clients)