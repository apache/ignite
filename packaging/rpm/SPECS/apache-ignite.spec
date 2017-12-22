%define __jar_repack %{nil}
%define user ignite


#-------------------------------------------------------------------------------
#
# Packages' descriptions
#

Name:             apache-ignite
Version:          2.4.0
Release:          1%{?dist}
Summary:          Apache Ignite In-Memory Computing Platform
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org/
Source:           %{name}.zip
Requires:         java-1.8.0, chkconfig
Requires(pre):    shadow-utils
Provides:         %{name}
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description
Apache Ignite™ is the in-memory computing platform composed of a strongly
consistent distributed database with powerful SQL, key-value and processing APIs


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
cd $(ls)

# Create base directory structure
mkdir -p %{buildroot}%{_datadir}/%{name}
mkdir -p %{buildroot}%{_libdir}/%{name}
mkdir -p %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin
mkdir -p %{buildroot}%{_var}/log/%{name}
mkdir -p %{buildroot}%{_sharedstatedir}/%{name}
mkdir -p %{buildroot}%{_sysconfdir}/systemd/system
mkdir -p %{buildroot}%{_bindir}

# Copy nessessary files and remove *.bat files
cp -rf benchmarks bin platforms %{buildroot}%{_datadir}/%{name}
cp -rf docs/* examples %{buildroot}%{_datadir}/doc/%{name}-%{version}
mv -f %{buildroot}%{_datadir}/%{name}/bin/{ignitevisorcmd.sh,sqlline.sh} %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin/
find %{buildroot}%{_datadir}/%{name}/ -name *.bat -exec rm -rf {} \;

# Copy libs to /usr/lib and map them to IGNITE_HOME
cp -rf libs/* %{buildroot}%{_libdir}/%{name}
ln -sf %{_libdir}/%{name} %{buildroot}%{_datadir}/%{name}/libs

# Setup configuration
cp -rf config %{buildroot}%{_sysconfdir}/%{name}
ln -sf %{_sysconfdir}/%{name} %{buildroot}%{_datadir}/%{name}/config
cp -rf %{_sourcedir}/default-config.xml %{buildroot}%{_sysconfdir}/%{name}/

# Setup systemctl service
cp -rf %{_sourcedir}/name.service %{buildroot}%{_sysconfdir}/systemd/system/%{name}.service
cp -rf %{_sourcedir}/service.sh %{buildroot}%{_datadir}/%{name}/bin/
chmod +x %{buildroot}%{_datadir}/%{name}/bin/service.sh
for file in %{buildroot}%{_sysconfdir}/systemd/system/%{name}.service %{buildroot}%{_datadir}/%{name}/bin/service.sh
do
    sed -i -r -e "s|#name#|%{name}|g" \
              -e "s|#user#|%{user}|g" \
        ${file}
done
echo "pathToConfig=\"\"" > %{buildroot}%{_sysconfdir}/%{name}/node.cfg

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
%{_datadir}/doc/%{name}-%{version}/bin
%{_libdir}/%{name}
%{_sysconfdir}/systemd/system/%{name}.service
%{_sharedstatedir}/%{name}/log

%config(noreplace) %{_sysconfdir}/%{name}/*

%doc %{name}-*/README.txt
%doc %{name}-*/NOTICE
%doc %{name}-*/RELEASE_NOTES.txt
%license %{name}-*/LICENSE
