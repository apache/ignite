%define __jar_repack %{nil}
%define user ignite
%define nameCore apache-ignite-core
%define nameLibs apache-ignite-libs
%define nameBenchmarks apache-ignite-benchmarks
%define nameDocs apache-ignite-docs
%define nameExamples apache-ignite-examples
%define nameCpp apache-ignite-cpp
%define _libdir /usr/lib
%define _log %{_var}/log
%define _sharedstatedir /var/lib



#-------------------------------------------------------------------------------
#
# Packages' descriptions
#

Name:             apache-ignite
Version:          2.5.0
Release:          1
Summary:          Apache Ignite In-Memory Computing Platform
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Source:           %{name}-fabric-%{version}-bin.zip
Requires:         %{name}-libs = %{version}-%{release}, %{name}-benchmarks = %{version}-%{release}, %{name}-docs = %{version}-%{release}, %{name}-examples = %{version}-%{release}, %{name}-cpp = %{version}-%{release}
Provides:         %{name}
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description
Apache Ignite™ is the in-memory computing platform composed of a strongly
consistent distributed database with powerful SQL, key-value and processing APIs

This package is a virtual aggregation package for all packages for current
Apache Ignite release - install it to install all other packages


%package -n %{nameCore}
Summary:          Apache Ignite execution core 
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Requires:         java-1.8.0, pkgconfig
Requires(pre):    shadow-utils
Provides:         %{name}-core
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameCore}
Apache Ignite's core files and libs, necessary for node operation


%package -n %{nameLibs}
Summary:          Apache Ignite optional libs
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Requires:         %{name}-core = %{version}-%{release}
Provides:         %{name}-libs
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameLibs}
Apache Ignite's optinal libs and integrations


%package -n %{nameBenchmarks}
Summary:          Apache Ignite benchmarks
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Requires:         %{name}-core = %{version}-%{release}
Provides:         %{name}-benchmarks
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameBenchmarks}
Prepared maven project for Apache Ignite's benchmarks conduct


%package -n %{nameExamples}
Summary:          Apache Ignite examples
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Provides:         %{name}-examples
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameExamples}
Apache Ignite's integration and usage examples


%package -n %{nameDocs}
Summary:          Apache Ignite documentation
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Provides:         %{name}-docs
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameDocs}
Apache Ignite's Javadoc and Scaladoc documentation


%package -n %{nameCpp}
Summary:          Apache Ignite C++ platform
Version:          %{version}
Group:            Development/System
License:          ASL 2.0
URL:              https://ignite.apache.org
Obsoletes:        apache-ignite < 2.5.0
Provides:         %{name}-cpp
AutoReq:          no
AutoProv:         no
BuildArch:        noarch
%description -n %{nameCpp}
C++ files necessary for using Apache Ignite



%prep
#-------------------------------------------------------------------------------
#
# Prepare step: unpack sources
#

%setup -q -n %{name}-fabric-%{version}-bin



#-------------------------------------------------------------------------------
#
# Preinstall scripts
# $1 can be:
#     1 - Initial install 
#     2 - Upgrade
#



%post -n %{nameCore}
case $1 in
    1)  # Initial installation
        # Add user for service operation
        useradd -r -d %{_datadir}/%{name} -s /usr/sbin/nologin %{user}
        # Change ownership for work and log directories
        chown -vR %{user}:%{user} %{_sharedstatedir}/%{name} %{_log}/%{name}
        # Install alternatives (commented out until ignitevisorcmd / ignitesqlline is ready to work from any user)
        #update-alternatives --install %{_bindir}/ignitevisorcmd ignitevisorcmd %{_datadir}/%{name}/bin/ignitevisorcmd.sh 0
        #update-alternatives --auto ignitevisorcmd
        #update-alternatives --display ignitevisorcmd
        #update-alternatives --install %{_bindir}/ignitesqlline ignitesqlline %{_datadir}/%{name}/bin/sqlline.sh 0
        #update-alternatives --auto ignitesqlline
        #update-alternatives --display ignitesqlline
        ;;
    2)  # Upgrade
        :
        ;;
esac



%preun -n %{nameCore}
case $1 in
    0)  # Uninstallation
        # Remove alternatives (commented out until ignitevisorcmd / ignitesqlline is ready to work from any user)
        #update-alternatives --remove ignitevisorcmd /usr/share/%{name}/bin/ignitevisorcmd.sh
        #update-alternatives --display ignitevisorcmd || true
        #update-alternatives --remove ignitesqlline /usr/share/%{name}/bin/sqlline.sh
        #update-alternatives --display ignitesqlline || true
        ;;
    1)  # Upgrade
        :
        ;;
esac



%postun -n %{nameCore}
case $1 in
    0)  # Uninstallation
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
    1)  # Upgrade
        :
        ;;
esac



%install
#-------------------------------------------------------------------------------
#
# Prepare packages' layout
#

# Create base directory structure
mkdir -p %{buildroot}%{_datadir}/%{name}                       # Main installation directory
mkdir -p %{buildroot}%{_libdir}/%{name}                        # Libs directory
mkdir -p %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin    # Docs | Examples directory (with bin for ignitevisorcmd.sh)
mkdir -p %{buildroot}%{_log}/%{name}                           # Log directory
mkdir -p %{buildroot}%{_sharedstatedir}/%{name}                # Work directory
mkdir -p %{buildroot}%{_libdir}/systemd/system                 # Config | System Service directory
mkdir -p %{buildroot}%{_sysconfdir}/systemd/system
#mkdir -p %{buildroot}%{_bindir}                                # Executables directory (commented out until ignitevisorcmd / ignitesqlline is ready to work from any user)

# Install run scripts and and remove *.bat files
cp -rf bin %{buildroot}%{_datadir}/%{name}
find %{buildroot}%{_datadir}/%{name}/ -name *.bat -exec rm -rf {} \;

# Install libs /usr/lib and map them to ${IGNITE_HOME}/libs
cp -rf libs/* %{buildroot}%{_libdir}/%{name}
ln -sf %{_libdir}/%{name} %{buildroot}%{_datadir}/%{name}/libs

# Install benchmark files
cp -rf benchmarks %{buildroot}%{_datadir}/%{name}
cd %{buildroot}%{_datadir}/%{name}/benchmarks/libs
for lib in $(for file in $(ls); do find ../../../../lib/%{name} -name $file | grep -v optional || true; done); do
    rm -rfv $(basename $lib)
    ln -sv $lib $(basename $lib)
done
cd ${OLDPWD}

# Install platform files
cp -rf platforms %{buildroot}%{_datadir}/%{name}
rm -rf %{buildroot}%{_datadir}/%{name}/platforms/dotnet

# Install documentation and examples
cp -rf docs/* examples README.txt NOTICE RELEASE_NOTES.txt %{buildroot}%{_datadir}/doc/%{name}-%{version}
mv -f %{buildroot}%{_datadir}/%{name}/bin/ignitevisorcmd.sh %{buildroot}%{_datadir}/doc/%{name}-%{version}/bin/

# Setup configuration
cp -rf config %{buildroot}%{_sysconfdir}/%{name}
ln -sf %{_sysconfdir}/%{name} %{buildroot}%{_datadir}/%{name}/config

# Setup systemctl service
cp -rf %{_sourcedir}/name.service %{buildroot}%{_libdir}/systemd/system/%{name}@.service
ln -sf %{_libdir}/systemd/system/%{name}@.service %{buildroot}%{_sysconfdir}/systemd/system/%{name}@.service
cp -rf %{_sourcedir}/service.sh %{buildroot}%{_datadir}/%{name}/bin/
chmod +x %{buildroot}%{_datadir}/%{name}/bin/service.sh
for file in %{buildroot}%{_libdir}/systemd/system/%{name}@.service %{buildroot}%{_datadir}/%{name}/bin/service.sh
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

%files -n %{nameCore}
%dir %{_datadir}/%{name}
%{_datadir}/%{name}/bin
%{_datadir}/%{name}/config
%{_datadir}/%{name}/libs
%{_datadir}/%{name}/work

%{_libdir}/%{name}
%exclude %{_libdir}/%{name}/optional

%dir %{_sysconfdir}/%{name}
%{_libdir}/systemd/system/%{name}@.service
%{_sysconfdir}/systemd/system/%{name}@.service
%config(noreplace) %{_sysconfdir}/%{name}/*

%dir %{_log}/%{name}
%{_sharedstatedir}/%{name}/log

%dir %{_datadir}/doc/%{name}-%{version}
%{_datadir}/doc/%{name}-%{version}/README.txt
%{_datadir}/doc/%{name}-%{version}/NOTICE
%{_datadir}/doc/%{name}-%{version}/RELEASE_NOTES.txt

%license LICENSE


%files -n %{nameLibs}
%{_libdir}/%{name}/optional


%files -n %{nameBenchmarks}
%{_datadir}/%{name}/benchmarks


%files -n %{nameExamples}
%{_datadir}/doc/%{name}-%{version}/examples


%files -n %{nameDocs}
%{_datadir}/doc/%{name}-%{version}
%exclude %{_datadir}/doc/%{name}-%{version}/examples
%exclude %{_datadir}/doc/%{name}-%{version}/README.txt
%exclude %{_datadir}/doc/%{name}-%{version}/NOTICE
%exclude %{_datadir}/doc/%{name}-%{version}/RELEASE_NOTES.txt


%files -n %{nameCpp}
%{_datadir}/%{name}/platforms



%changelog
#-------------------------------------------------------------------------------
#
# Changelog
#

* Tue Mar 20 2018 Peter Ivanov <mr.weider@gmail.com> - 2.5.0-1
- Updated Apache Ignite to version 2.5.0
- Updated sources definition for package building
- Introduced split packages design
- Reorganized and optimised install section

* Wed Jan 17 2018 Peter Ivanov <mr.weider@gmail.com> - 2.4.0-1
- Initial package release

