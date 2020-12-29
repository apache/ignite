#!/usr/bin/env bash

CERTS_DIR="$(dirname "$0")"

cd "${CERTS_DIR}"

source ./functions.sh

PSWD=123456

makeRoot root "CN=Ignite Root" ${PSWD}

makeCA root ca "CN=Ignite CA" ${PSWD}

makeTruststore root ca

mkCert ca server "CN=Ignite Server" ${PSWD}
mkCert ca client "CN=Ignite Client" ${PSWD}
mkCert ca admin "CN=Ignite Admin" ${PSWD}

cd -

