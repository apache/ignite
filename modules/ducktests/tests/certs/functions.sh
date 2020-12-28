#!/usr/bin/env bash

function makeRoot() {
    ALIAS=$1
    DNAME=$2
    PSWD=$3

    if [[ ${ALIAS} == "" ]] || [[ ${DNAME} == "" ]] || [[ ${PSWD} == "" ]]
    then
        error "makeRoot: Need ALIAS, DNAME, PSWD"
    fi

    rm ${ALIAS}.jks
    rm ${ALIAS}.pem

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname "${DNAME}" -ext bc:c -storepass ${PSWD} \
     -keypass ${PSWD} -noprompt -v

    keytool -keystore ${ALIAS}.jks -storepass ${PSWD} -keypass ${PSWD} -alias ${ALIAS} -exportcert \
     -rfc -file ${ALIAS}.pem -v
}

function makeCA() {
    ROOT=$1
    ALIAS=$2
    DNAME=$3
    PSWD=$4

    if [[ ${ROOT} == "" ]] || [[ ${ALIAS} == "" ]] || [[ ${DNAME} == "" ]] || [[ ${PSWD} == "" ]]
    then
        error "makeCA: Need CA, ALIAS, DNAME, PSWD"
    fi

    rm ${ALIAS}.jks
    rm ${ALIAS}.pem

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname "${DNAME}" -ext bc:c -storepass ${PSWD} \
     -keypass ${PSWD} -noprompt -v

    keytool -storepass ${PSWD} -keypass ${PSWD} -keystore ${ALIAS}.jks -certreq -alias ${ALIAS} \
      | keytool -storepass ${PSWD} -keypass ${PSWD} -keystore ${ROOT}.jks -gencert -alias ${ROOT} -ext BC=0 -rfc \
       -outfile ${ALIAS}.pem -v

    keytool -keystore ${ALIAS}.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias ${ROOT} -file ${ROOT}.pem \
     -noprompt -v
    keytool -keystore ${ALIAS}.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias ${ALIAS} -file ${ALIAS}.pem \
     -noprompt -v
}

function mkCert() {
    CA=$1
    ALIAS=$2
    DNAME=$3
    PSWD=$4

    if [[ ${CA} == "" ]] || [[ ${ALIAS} == "" ]] || [[ ${DNAME} == "" ]] || [[ ${PSWD} == "" ]]
    then
        error "mkCert: Need CA, ALIAS, DNAME, PSWD"
    fi

    rm -rf "${ALIAS}.jks"
    rm -rf "${ALIAS}.pem"
    rm -rf "${ALIAS}.csr"

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname ${DNAME} -keyalg RSA -keysize 2048 \
     -keypass ${PSWD} -storepass ${PSWD} -noprompt -v || error

    keytool -storepass ${PSWD} -keystore ${ALIAS}.jks -certreq -alias ${ALIAS} -file ${ALIAS}.csr -v || error

    keytool -gencert -infile ${ALIAS}.csr -keystore ${CA}.jks -alias ${CA} -storepass ${PSWD} -rfc \
     -outfile ${ALIAS}.pem -v || error

    keytool -keystore ${ALIAS}.jks -importcert -alias ${ALIAS} -storepass ${PSWD} -file ${ALIAS}.pem -noprompt -v || error

    rm -rf "${ALIAS}.csr"
    rm -rf "${ALIAS}.pem"
}

function makeTruststore() {
    rm truststore.jks
    # shellcheck disable=SC2068
    for cert in $@ ; do
      keytool -keystore truststore.jks -importcert -alias ${cert} -storepass 123456 -file ${cert}.pem -noprompt -v || error

    done
}

function error() {
    # shellcheck disable=SC2145
    echo "¯\_(ツ)_/¯ Something went wrong: $@"
    exit 1
}
