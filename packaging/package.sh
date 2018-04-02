#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

cd "$(dirname "${BASH_SOURCE[0]}")"    # Run from the script's root



##############
#  SETTINGS  #
##############
BUILD_DEB_FLAG=false         # Whether to build DEB
BUILD_RPM_FLAG=false         # Whether to build RPM
BUILD_FROM_SRC_FLAG=false    # Whether to build packages from sources
MVN="./mvnw"                 # Maven wrapper executable
MVN_VERSION="3.5.3"          # Default maven version to execute



###############
#  FUNCTIONS  #
###############

# Usage help
usage () {
    cat <<EOF

######################################################################
#  Build RPM or DEB package from Apache Ignite's sources or binaries #
######################################################################

Usage: ./$(basename ${BASH_SOURCE[0]}) --rpm,--deb --src|--bin

Options:
    --rpm, --deb     select package type for building (can be selected all types)

    --src            prepare binary artifacts from source before packaging

EOF
}


# Check and prepare build environment
prepEnv () {
    installCmd=""
    packages=""
    executables="rpmbuild unzip"

    # Check OS
    name=$(cat /etc/*release | grep ^NAME | sed -r 's|.*"(.*)".*|\1|')
    case ${name} in
        "Ubuntu")
            installCmd='apt --no-install-recommends'
            packages="rpm unzip"
            ;;
        "CentOS Linux")
            installCmd="yum"
            packages="rpm-build unzip"
            ;;
        *)
            echo "Unknown or unsupported linux detected"
            echo "Will skip 'Check and prepare build environment' step"
            echo "Please, prepare you environment manually"
            ;;
    esac

    # Install missing software if necessary
    installFlag=false
    if [ ! -z installCmd ]; then
        for executable in ${executables}; do
            type ${executable} &>/dev/null || {
                installFlag=true
                break
            }
        done
        if ${installFlag}; then
            ${installCmd} install ${packages}
        fi
    fi
}


# Prepare binary artifacts from sources
prepBin () {
    echo "Packaging from sources is not implemented"
}


getBin () {
    igniteVersion=$(cat rpm/SPECS/apache-ignite.spec | grep '^*' | head -1 | sed -r 's|.*\ -\ (.*)-.*|\1|')
    binName="apache-ignite-fabric-${igniteVersion}-bin.zip"
    binPreparedFlag=false

    cd rpm/SOURCES
    ls ${binName} && binPreparedFlag=true || true
    if ! ${binPreparedFlag}; then
        curl -O https://archive.apache.org/dist/ignite/${igniteVersion}/${binName} && binPreparedFlag=true || true
    fi
    cd ${OLDPWD}

    if ! ${binPreparedFlag}; then
        echo "[ERROR] Can't find | get Apache Ignite's binary archive."
        exit 1
    fi
}


# Build RPM package
buildRPM () {
    # Prepare build layout
    mkdir -pv rpm/{BUILD,RPMS,SRPMS}

    # Run RPMBUILD
    rpmbuild -bb -v --define "_topdir $(pwd)/rpm" rpm/SPECS/apache-ignite.spec

    # Gather RPMS
    find rpm/ -name "*.rpm" -exec cp -rfv {} ./ \;
}


# Build DEB package
buildDEB () {
    echo "Debian packages build is not implemented"
}


# Trap function
processTrap () {
    # Removing temporary files
    rm -rf rpm/{BUILD,RPMS,SRPMS}

    # Finish
    echo
    TIME="$(($(date +%s) - START_TIME))"
    echo "=== Run time: $(printf '%dh:%02dm:%02ds\n' $((TIME/3600)) $((TIME%3600/60)) $((TIME%60))) ==="
    echo
}



###########
#  START  #
###########
START_TIME=$(date +%s)
clear


# Parse input options
if [[ ! -z "${1-}" ]]; then
    until [[ -z "${1-}" ]]; do
        case "$1" in
            --src)
                shift
                BUILD_FROM_SRC_FLAG=true
                ;;
            --rpm)
                shift
                BUILD_RPM_FLAG=true
                ;;
            --deb)
                BUILD_DEV_FLAG=true
                ;;
            --help)
                usage
                exit 0
            ;;
            *)
                echo "[ERROR] Unknown argument '${1}'"
                usage
                exit 1
            ;;
        esac
    done
fi
if [ ${BUILD_RPM_FLAG} == false -a ${BUILD_DEB_FLAG} == false ]; then
    echo "[ERROR] At least one type of package should be specified: RPM or DEB"
    usage
    exit 1
fi


# Trap
trap 'processTrap' EXIT


# Build packages
prepEnv
if ${BUILD_FROM_SRC_FLAG}; then
    prepBin
else getBin
fi
if ${BUILD_RPM_FLAG}; then buildRPM; fi
if ${BUILD_DEB_FLAG}; then buildDEB; fi
