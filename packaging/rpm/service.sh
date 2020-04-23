#!/usr/bin/env bash

firewallCmd="firewall-cmd --permanent --direct --add-rule ipv4 filter INPUT 0"

# Define function to check whether firewalld is present and started and apply firewall rules for grid nodes
setFirewall ()
{
	if [[ "$(type firewall-cmd &>/dev/null; echo $?)" -eq 0 && "$(systemctl is-active firewalld)" == "active" ]]
	then
	    for port in s d
	    do
	        ${firewallCmd} -p tcp -m multiport --${port}ports 11211:11220,47500:47509,47100:47109 -j ACCEPT &>/dev/null
	        ${firewallCmd} -p udp -m multiport --${port}ports 47400:47409 -j ACCEPT &>/dev/null
	    done
	    ${firewallCmd} -m pkttype --pkt-type multicast -j ACCEPT &>/dev/null

	    systemctl restart firewalld
	fi
}

case $1 in
	start)
		/usr/share/#name#/bin/ignite.sh /etc/#name#/$2 & echo $! >> /var/run/#name#/$2.pid
		;;
	set-firewall)
		setFirewall
		;;
esac
