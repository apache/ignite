%define __jar_repack %{nil}
%define user ignite

#-------------------------------------------------------------------------------
#
# Packages' descriptions
#

Name:		apache-ignite-fabric
Version:	2.4.0
Release:	1%{?dist}
Summary:	Apache Ignite Fabric
Group:		Development/System
License:	ASL 2.0
URL:		https://ignite.apache.org/
Source:		%{name}.zip
Requires:	java-1.8.0, chkconfig
Requires(pre):  shadow-utils
Provides:	%{name}
AutoReq:	no
AutoProv:	no
BuildArch:	noarch
%description
Apache Ignite™ is ...
* the in-memory computing platform
* composed of a strongly consistent distributed database
* with powerful SQL, key-value and processing APIs

#-------------------------------------------------------------------------------
#
# Prepare step: unpack sources
#

%prep
%setup -q -c -n %{name}

#-------------------------------------------------------------------------------
#
# Preinstall scripts
# $1 can be:
#     1 - Initial install 
#     2 - Upgrade
#

#-------------------------------------------------------------------------------
#
# Postinstall scripts
# $1 can be:
#     1 - Initial installation
#     2 - Upgrade
#

%post
case $1 in
	1)
		# Add user for service operation
		useradd -r -d %{_datadir}/%{name} -s /usr/sbin/nologin %{user}
		# Change ownership for work and log directories
		chown -vR %{user}:%{user} %{_sharedstatedir}/%{name} %{_var}/log/%{name}
		# Enable service autostart on boot
		systemctl enable %{name}.service
		# Install alternatives
		update-alternatives --install %{_bindir}/ignitevisorcmd ignitevisorcmd %{_datadir}/%{name}/bin/ignitevisorcmd.sh 0
		update-alternatives --auto ignitevisorcmd
		update-alternatives --display ignitevisorcmd
		;;
	2)
		:
		;;
esac


#-------------------------------------------------------------------------------
#
# Pre-uninstall scripts
# $1 can be:
#     0 - Uninstallation
#     1 - Upgrade
#

%preun
case $1 in
	0)
		# Stop and disable service autostart on boot
		systemctl stop %{name}.service
		systemctl disable %{name}.service
		update-alternatives --remove ignitevisorcmd /usr/share/%{name}/bin/ignitevisorcmd.sh
		update-alternatives --display ignitevisorcmd || true
		;;
	1)
		:
		;;
esac


#-------------------------------------------------------------------------------
#
# Post-uninstall scripts
# $1 can be:
#     0 - Uninstallation
#     1 - Upgrade
#

%postun
case $1 in
	0)
		# Remove user
		userdel %{user}
		# Remove service PID directory
		rm -rfv /var/run/%{name}
		# Remove firewalld rules if firewalld is installed and running
		if [[ "$(type firewall-cmd &>/dev/null; echo $?)" -eq 0 && "$(systemctl is-active firewalld)" == "active" ]]
		then
			for port in s d
			do
				firewall-cmd --permanent --direct --remove-rule ipv4 filter INPUT 0 -p tcp -m multiport --${port}ports 11211:11220,47500:47509,47100:47109 -j ACCEPT &>/dev/null
				firewall-cmd --permanent --direct --remove-rule ipv4 filter INPUT 0 -p udp -m multiport --${port}ports 47400:47409 -j ACCEPT &>/dev/null
			done
			firewall-cmd --permanent --direct --remove-rule ipv4 filter INPUT 0 -m pkttype --pkt-type multicast -j ACCEPT &>/dev/null
			systemctl restart firewalld
		fi
		;;
	1)
		:
		;;
esac

#-------------------------------------------------------------------------------
#
# Prepare packages' layout
#

%install
cd %{name}-*

# Create base directory structure
mkdir -p %{buildroot}%{_datadir}/%{name}
mkdir -p %{buildroot}%{_datadir}/doc/%{name}-%{version}
mkdir -p %{buildroot}%{_var}/log/%{name}
mkdir -p %{buildroot}%{_sharedstatedir}/%{name}
mkdir -p %{buildroot}%{_sysconfdir}/systemd/system
mkdir -p %{buildroot}%{_bindir}

# Copy nessessary files and remove *.bat files
cp -rf benchmarks bin libs platforms %{buildroot}%{_datadir}/%{name}
cp -rf docs/* examples %{buildroot}%{_datadir}/doc/%{name}-%{version}
find %{buildroot}%{_datadir}/%{name}/ -name *.bat -exec rm -rf {} \;

# Setup configuration
cp -rf config %{buildroot}%{_sysconfdir}/%{name}
ln -sf %{_sysconfdir}/%{name} %{buildroot}%{_datadir}/%{name}/config
cat <<EOF > %{buildroot}%{_sysconfdir}/%{name}/default-config.xml
<?xml version="1.0" encoding="UTF-8"?>

<!--
       Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<!--
       Ignite Spring configuration file to startup Ignite cache.

  This file demonstrates how to configure cache using Spring. Provided cache
  will be created on node startup.

  Use this configuration file when running HTTP REST examples (see 'examples/rest' folder).

  When starting a standalone node, you need to execute the following command:
  {IGNITE_HOME}/bin/ignite.{bat|sh} examples/config/example-cache.xml

  When starting Ignite from Java IDE, pass path to this file to Ignition:
  Ignition.start("examples/config/example-cache.xml");
-->

<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
        http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd">

    <bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration">

        <!-- Explicitly configure TCP discovery SPI to provide list of initial nodes. -->
        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
                <property name="ipFinder">
                    <bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder">
                        <property name="multicastGroup" value="228.10.10.157"/>
                    </bean>
                </property>
            </bean>
        </property>

        <!-- Enabling Apache Ignite native persistence. -->
        <property name="dataStorageConfiguration">
            <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
                <property name="defaultDataRegionConfiguration">
                    <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                        <property name="persistenceEnabled" value="true"/>
                    </bean>
                </property>
            </bean>
        </property>

    </bean>
</beans>
EOF

# Setup systemctl services for PE, EE and UE packages
cat <<EOF > %{buildroot}%{_sysconfdir}/systemd/system/%{name}.service
[Unit]
Description=Apache Ignite Fabric Service
After=syslog.target network.target

[Service]
Type=forking
User=%{user}
WorkingDirectory=/usr/share/%{name}/work
PermissionsStartOnly=true
ExecStartPre=-/usr/bin/mkdir /var/run/%{name}
ExecStartPre=-/usr/bin/chown %{user}:%{user} /var/run/%{name}
ExecStartPre=-/usr/bin/env bash /usr/share/%{name}/bin/service.sh set-firewall
EnvironmentFile=%{_sysconfdir}/%{name}/node.cfg
ExecStart=/usr/share/%{name}/bin/service.sh start \${pathToConfig}
PIDFile=/var/run/%{name}/%{name}.pid

[Install]
WantedBy=multi-user.target
EOF
cat <<EOF > %{buildroot}%{_datadir}/%{name}/bin/service.sh
#!/usr/bin/env bash

PID=/var/run/%{name}/%{name}.pid
firewallCmd="firewall-cmd --permanent --direct --add-rule ipv4 filter INPUT 0"

# Define function to check whether firewalld is present and started and apply firewall rules for grid nodes
setFirewall ()
{
	if [[ "\$(type firewall-cmd &>/dev/null; echo \$?)" -eq 0 && "\$(systemctl is-active firewalld)" == "active" ]]
	then
	        for port in s d
	        do
	                \${firewallCmd} -p tcp -m multiport --\${port}ports 11211:11220,47500:47509,47100:47109 -j ACCEPT &>/dev/null
	                \${firewallCmd} -p udp -m multiport --\${port}ports 47400:47409 -j ACCEPT &>/dev/null
	        done
	        \${firewallCmd} -m pkttype --pkt-type multicast -j ACCEPT &>/dev/null

	        systemctl restart firewalld
	fi
}

case \$1 in
	start)
		/usr/share/%{name}/bin/ignite.sh \$2 & echo \$! >> \${PID}
		;;
	set-firewall)
		setFirewall
		;;
esac
EOF
chmod +x %{buildroot}%{_datadir}/%{name}/bin/service.sh
cat <<EOF > %{buildroot}%{_sysconfdir}/%{name}/node.cfg
pathToConfig=""
EOF

# Map work and log directories
ln -sf %{_sharedstatedir}/%{name} %{buildroot}%{_datadir}/%{name}/work
ln -sf %{_var}/log/%{name} %{buildroot}%{_sharedstatedir}/%{name}/log


#-------------------------------------------------------------------------------
#
# Package file list check
#
%files
%dir %{_datadir}/%{name}
%dir %{_sysconfdir}/%{name}
%dir %{_sharedstatedir}/%{name}
%dir %{_var}/log/%{name}

%{_datadir}/%{name}/benchmarks
%{_datadir}/%{name}/bin
%{_datadir}/%{name}/config
%{_datadir}/%{name}/libs
%{_datadir}/%{name}/platforms
%{_datadir}/%{name}/work
%{_sysconfdir}/systemd/system/%{name}.service
%{_sharedstatedir}/%{name}/log

%config(noreplace) %{_sysconfdir}/%{name}/*

%doc %{name}-*/README.txt
%doc %{name}-*/NOTICE
%doc %{name}-*/RELEASE_NOTES.txt
%license %{name}-*/LICENSE

