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
Version:          2.7.0
Release:          1
Summary:          Apache Ignite In-Memory Computing, Database and Caching Platform
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org/
Source:           %{name}-fabric-%{version}-bin.zip
Requires:         java-1.8.0, chkconfig
Requires(pre):    shadow-utils
Provides:         %{name}
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description
Ignite™ is a memory-centric distributed database, caching, and processing
platform for transactional, analytical, and streaming workloads, delivering
in-memory speeds at petabyte scale


%prep
#-------------------------------------------------------------------------------
#
# Prepare step: unpack sources
#

%setup -q -n %{name}-fabric-%{version}-bin


#%pre
#-------------------------------------------------------------------------------
#
# Preinstall scripts
# $1 can be:
#     1 - Initial install 
#     2 - Upgrade
#


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

setPermissions () {
    chown -R %{user}:%{user} %{_sharedstatedir}/%{name} %{_log}/%{name}
}

case $1 in
    1|configure)
        # DEB postinst upgrade
        if [ ! -z "${2}" ]; then
            echoUpgradeMessage
        fi

        # Add user for service operation
        useradd -r -d %{_datadir}/%{name} -s /usr/sbin/nologin %{user}

        # Change ownership for work and log directories
        setPermissions

        # Install alternatives
        # Commented out until ignitevisorcmd / ignitesqlline is ready to work from any user
        #update-alternatives --install %{_bindir}/ignitevisorcmd ignitevisorcmd %{_datadir}/%{name}/bin/ignitevisorcmd.sh 0
        #update-alternatives --auto ignitevisorcmd
        #update-alternatives --display ignitevisorcmd
        #update-alternatives --install %{_bindir}/ignitesqlline ignitesqlline %{_datadir}/%{name}/bin/sqlline.sh 0
        #update-alternatives --auto ignitesqlline
        #update-alternatives --display ignitesqlline
        ;;
    2)
        # RPM postinst upgrade
        echoUpgradeMessage

        # Workaround for upgrade from 2.4.0
        if [ -d /usr/com/apache-ignite/ ]; then
            for file in /usr/com/apache-ignite/*; do
                if [ ! -h $file ]; then
                    cp -rf $file %{_sharedstatedir}/%{name}/
                fi
            done
        fi

        # Change ownership for work and log directories (yum resets permissions on upgrade nevertheless)
        setPermissions
        ;;
esac


%preun
#-------------------------------------------------------------------------------
#
# Pre-uninstall scripts
# $1 can be:
#     0 - Uninstallation
#     1 - Upgrade
#

stopIgniteNodes () {
    if ! $(grep -q "Microsoft" /proc/version); then
        systemctl stop 'apache-ignite@*'
    fi
    ps ax | grep '\-DIGNITE_HOME' | head -n-1 | awk {'print $1'} | while read pid; do
        kill -INT ${pid}
    done
}

case $1 in
    0|remove)
        # Stop all nodes (both service and standalone)
        stopIgniteNodes

        # Remove alternatives
        # Commented out until ignitevisorcmd / ignitesqlline is ready to work from any user
        #update-alternatives --remove ignitevisorcmd /usr/share/%{name}/bin/ignitevisorcmd.sh
        #update-alternatives --display ignitevisorcmd || true
        #update-alternatives --remove ignitesqlline /usr/share/%{name}/bin/sqlline.sh
        #update-alternatives --display ignitesqlline || true
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
mkdir -p %{buildroot}%{_datadir}/%{name}
mkdir -p %{buildroot}%{_libdir}/%{name}
mkdir -p %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin
mkdir -p %{buildroot}%{_log}/%{name}
mkdir -p %{buildroot}%{_sharedstatedir}/%{name}
mkdir -p %{buildroot}%{_sysconfdir}/systemd/system
mkdir -p %{buildroot}%{_bindir}

# Copy nessessary files and remove *.bat files
cp -rf benchmarks bin platforms %{buildroot}%{_datadir}/%{name}
cp -rf docs/* examples %{buildroot}%{_datadir}/doc/%{name}-%{version}
mv -f %{buildroot}%{_datadir}/%{name}/bin/ignitevisorcmd.sh %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin/
find %{buildroot}%{_datadir}/%{name}/ -name *.bat -exec rm -rf {} \;

# Copy libs to /usr/lib and map them to IGNITE_HOME
cp -rf libs/* %{buildroot}%{_libdir}/%{name}
ln -sf %{_libdir}/%{name} %{buildroot}%{_datadir}/%{name}/libs

# Setup configuration
cp -rf config %{buildroot}%{_sysconfdir}/%{name}
ln -sf %{_sysconfdir}/%{name} %{buildroot}%{_datadir}/%{name}/config

# Setup systemctl service
cp -rf %{_sourcedir}/name.service %{buildroot}%{_sysconfdir}/systemd/system/%{name}@.service
cp -rf %{_sourcedir}/service.sh %{buildroot}%{_datadir}/%{name}/bin/
chmod +x %{buildroot}%{_datadir}/%{name}/bin/service.sh
for file in %{buildroot}%{_sysconfdir}/systemd/system/%{name}@.service %{buildroot}%{_datadir}/%{name}/bin/service.sh
do
    sed -i -r -e "s|#name#|%{name}|g" \
              -e "s|#user#|%{user}|g" \
        ${file}
done

# Map work and log directories
ln -sf %{_sharedstatedir}/%{name} %{buildroot}%{_datadir}/%{name}/work
ln -sf %{_log}/%{name} %{buildroot}%{_sharedstatedir}/%{name}/log


%files
#-------------------------------------------------------------------------------
#
# Package file list check
#

%dir %{_datadir}/%{name}
%dir %{_sysconfdir}/%{name}
%dir %{_sharedstatedir}/%{name}
%dir %{_log}/%{name}

%{_datadir}/%{name}/benchmarks
%{_datadir}/%{name}/bin
%{_datadir}/%{name}/config
%{_datadir}/%{name}/libs
%{_datadir}/%{name}/platforms
%{_datadir}/%{name}/work
%{_datadir}/doc/%{name}-%{version}
%{_libdir}/%{name}
%{_sysconfdir}/systemd/system/%{name}@.service
%{_sharedstatedir}/%{name}/log

%config(noreplace) %{_sysconfdir}/%{name}/*

%doc README.txt
%doc NOTICE
%doc RELEASE_NOTES.txt
%doc MIGRATION_GUIDE.txt
%license LICENSE


%changelog
#-------------------------------------------------------------------------------
#
# Changelog
#

* Thu Jul 26 2018 Peter Ivanov <mr.weider@gmail.com> - 2.7.0-1
- Updated Apache Ignite to version 2.7.0

* Fri Jun 15 2018 Peter Ivanov <mr.weider@gmail.com> - 2.6.0-1
- Updated Apache Ignite to version 2.6.0

* Tue Apr 17 2018 Peter Ivanov <mr.weider@gmail.com> - 2.5.0-1
- Updated Apache Ignite to version 2.5.0

* Wed Jan 17 2018 Peter Ivanov <mr.weider@gmail.com> - 2.4.0-1
- Initial package release

