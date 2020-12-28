#!/usr/bin/env bash

source ./functions.sh

PSWD=123456

makeRoot root "CN=Ignite Root" ${PSWD}

makeCA root ca "CN=Ignite CA" ${PSWD}

mkCert ca server "CN=Ignite Server" ${PSWD}

mkCert ca client "CN=Ignite Client" ${PSWD}

makeTruststore root ca