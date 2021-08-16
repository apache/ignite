#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

%define __jar_repack %{nil}
%define user ignite
%define _libdir /usr/lib
%define _log %{_var}/log
%define _sharedstatedir /var/lib



#-------------------------------------------------------------------------------
#
# Packages' descriptions
#
Name:             apache-ignite
Version:          3.0.0
Release:          1
Summary:          Apache Ignite In-Memory Computing, Database and Caching Platform
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Requires:         java-11 which
Requires(pre):    shadow-utils
Provides:         %{name}
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description
Apache Ignite is a distributed database for high-performance computing with in-memory speed.



%pre
#-------------------------------------------------------------------------------
#
# Preinstall scripts
# $1 can be:
#     1 - Initial installation
#     2 - Upgrade
#
echo "Preinstall mode: '$1'"
case $1 in
    1|configure)
        # Add service user
        /usr/sbin/useradd -r -M -d %{_datadir}/%{name} -s /bin/bash %{user} 
        #useradd -r -md %{_datadir}/%{name} %{user}
        #[ -f "%{_datadir}/%{name}/.bashrc" ] && echo "cd ~" >> %{_datadir}/%{name}/.bashrc
        ;;
esac



%post
#-------------------------------------------------------------------------------
#
# Postinstall scripts
# $1 can be:
#     1 - Initial installation
#     2 - Upgrade
#

echoUpgradeMessage () {
    echo "======================================================================================================="
    echo "  WARNING: Updating Apache Ignite's cluster version requires updating every node before starting grid  "
    echo "======================================================================================================="
}

case $1 in
    1|configure)
        # DEB postinst upgrade
        if [ ! -z "${2}" ]; then
            echoUpgradeMessage
        fi

        # Set firewall rules
        if [[ "$(type firewall-cmd &>/dev/null; echo $?)" -eq 0 && "$(systemctl is-active firewalld 2>/dev/null)" == "active" ]]
        then
            for port in s d
            do
                ${firewallCmd} -p tcp -m multiport --${port}ports 11211:11220,47500:47509,47100:47109 -j ACCEPT &>/dev/null
                ${firewallCmd} -p udp -m multiport --${port}ports 47400:47409 -j ACCEPT &>/dev/null
            done
            ${firewallCmd} -m pkttype --pkt-type multicast -j ACCEPT &>/dev/null

            systemctl restart firewalld
        fi
        ;;
    2)
        # RPM postinst upgrade
        echoUpgradeMessage
esac

# Copy and configure skel
cp -rfv /etc/skel/.bash* %{_datadir}/%{name} 
echo "cd ~" >> %{_datadir}/%{name}/.bashrc

# Change ownership for work and log directories (yum resets permissions on upgrade nevertheless)
chown -R %{user}:%{user} %{_datadir}/%{name} \
                             %{_sharedstatedir}/%{name} \
                             %{_log}/%{name} \
                             %{_bindir}/ignite



%preun
#-------------------------------------------------------------------------------
#
# Pre-uninstall scripts
# $1 can be:
#     0 - Uninstallation
#     1 - Upgrade
#

stopIgniteNodes () {
    echo "Stopping ignite nodes"
}

case $1 in
    0|remove)
        # Stop all nodes (both service and standalone)
        stopIgniteNodes
        ;;
    1|upgrade)
        # Stop all nodes (both service and standalone)
        echo "=================================================================================="
        echo "  WARNING: All running Apache Ignite's nodes will be stopped upon package update  "
        echo "=================================================================================="
        stopIgniteNodes
        ;;
esac



%postun
#-------------------------------------------------------------------------------
#
# Post-uninstall scripts
# $1 can be:
#     0 - Uninstallation
#     1 - Upgrade
#

case $1 in
    0|remove)
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
    1|upgrade)
        :
        ;;
esac



%install
#-------------------------------------------------------------------------------
#
# Prepare packages' layout
#

# Create base directory structure
mkdir -pv %{buildroot}%{_datadir}/%{name}
mkdir -pv %{buildroot}%{_libdir}/%{name}
mkdir -pv %{buildroot}%{_log}/%{name}
mkdir -pv %{buildroot}%{_sharedstatedir}/%{name}
mkdir -pv %{buildroot}%{_sysconfdir}/%{name}
mkdir -pv %{buildroot}%{_bindir}

# Install binary
cp -rfv ignite %{buildroot}%{_bindir}
chmod +x %{buildroot}%{_bindir}/ignite

# Install ignite-cli configuraton file
touch %{buildroot}%{_sysconfdir}/%{name}/cfg
echo "bin=%{_libdir}/%{name}" >> %{buildroot}%{_sysconfdir}/%{name}/cfg
echo "work=%{_sharedstatedir}/%{name}" >> %{buildroot}%{_sysconfdir}/%{name}/cfg
echo "config=%{_sysconfdir}/%{name}" >> %{buildroot}%{_sysconfdir}/%{name}/cfg
echo "log=%{_log}/%{name}" >> %{buildroot}%{_sysconfdir}/%{name}/cfg
ln -sfv %{_sysconfdir}/%{name}/cfg %{buildroot}%{_datadir}/%{name}/.ignitecfg



%files
#-------------------------------------------------------------------------------
#
# Package file list check
#

%dir %{_datadir}/%{name}
%dir %{_sysconfdir}/%{name}
%dir %{_sharedstatedir}/%{name}
%dir %{_log}/%{name}

%{_libdir}/%{name}
%{_bindir}/ignite
%{_sysconfdir}/%{name}
%{_datadir}/%{name}/.ignitecfg


%changelog
#-------------------------------------------------------------------------------
#
# Changelog
#

* Fri Dec 11 2020 Petr Ivanov <mr.weider@gmail.com> - 3.0.0-1
- Apache Ignite 3 initial release

