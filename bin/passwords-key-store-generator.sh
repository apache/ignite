#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SOURCE="${BASH_SOURCE[0]}"

# Resolve $SOURCE until the file is no longer a symlink.
while [ -h "$SOURCE" ]
    do
        IGNITE_HOME="$(cd -P "$( dirname "$SOURCE"  )" && pwd)"

        SOURCE="$(readlink "$SOURCE")"

        # If $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located.
        [[ $SOURCE != /* ]] && SOURCE="$IGNITE_HOME/$SOURCE"
    done

#
# Set IGNITE_HOME.
#
export IGNITE_HOME="$(cd -P "$( dirname "$SOURCE" )" && pwd)"

source "${IGNITE_HOME}"/include/functions.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

# Define functions
createKeystore() {
    "$KEYTOOL" -importpassword -alias ${1} -keystore passwords.p12 -storetype pkcs12 -storepass ${2} -keypass ${2}
}

addToKeystore() {
    REPLY=${REPLY:-Y}
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        createKeystore ${1} ${2}
    fi
}

# Read Passwords
read -rep $'Do you want to encrypt your passwords? [Y/n]\n'
REPLY=${REPLY:-Y}
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo $'Please Enter a master password for keystore:\n' 
    read -s MASTER_PASSWORD

    read -rep $'Do you want to add a node-password to keystore? [Y/n]\n' 
    addToKeystore "node-password" ${MASTER_PASSWORD}

    read -rep $'Do you want to add a node key store password? [Y/n]\n' 
    addToKeystore "node-key-store-password" ${MASTER_PASSWORD}

    read -rep $'Do you want to add a node trust store password? [Y/n]\n' 
    addToKeystore "node-trust-store-password" ${MASTER_PASSWORD}

    read -rep $'Do you want to add a server key store password? [Y/n]\n' 
    addToKeystore "server-key-store-password" ${MASTER_PASSWORD}

    read -rep $'Do you want to add a server trust store password? [Y/n]\n' 
    addToKeystore "server-trust-store-password" ${MASTER_PASSWORD}
else
    exit 0
fi
