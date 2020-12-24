#!/usr/bin/env bash

# Создает сертификат JKS.
# 1 CA, 2 ALIAS, 3 DNAME, 4 DAYS(default 3650).
function mkcert () {
    CA=$1
    ALIAS=$2
    DNAME=$3
    PSWD=123456

    if [[ ${CA} == "" ]] || [[ ${ALIAS} == "" ]] || [[ ${DNAME} == "" ]]
    then
        echo "Необходимо указать CA, ALIAS, DNAME"
        exit 666
    fi

    if [[ ${4} =~ ^-?[[:digit:]]+$ ]]
    then
        DAYS=$4
    else
        echo "Для параметра DAYS используется значение 3650"
        DAYS=3650
    fi

    rm -rf "${ALIAS}.jks"
    rm -rf "${ALIAS}.pem"

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname ${DNAME} -keyalg RSA -keysize 2048 \
     -keypass ${PSWD} -storepass ${PSWD} -noprompt -v || error

    keytool -storepass ${PSWD} -keystore ${ALIAS}.jks -certreq -alias ${ALIAS} -file ${ALIAS}.csr -v || error

    keytool -gencert -infile ${ALIAS}.csr -validity ${DAYS} -keystore ${CA}.jks -alias ${CA} -storepass ${PSWD} -rfc \
     -outfile ${ALIAS}.pem -v || error

    keytool -keystore ${ALIAS}.jks -importcert -alias ${ALIAS} -storepass ${PSWD} -file ${ALIAS}.pem -noprompt -v || error

    rm -rf "${ALIAS}.csr"
    rm -rf "${ALIAS}.pem"
}

function makeRoot() {
    ALIAS=$1
    DNAME=$2
    PSWD=$3

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname "${DNAME}" -ext bc:c -storepass ${PSWD} -keypass ${PSWD} -noprompt -v
    keytool -keystore ${ALIAS}.jks -storepass ${PSWD} -keypass ${PSWD} -alias ${ALIAS} -exportcert -rfc > ${ALIAS}.pem
}

function makeCA() {
    ALIAS=$1
    ROOT=$2
    DNAME=$3
    PSWD=$4

    keytool -genkeypair -keystore ${ALIAS}.jks -alias ${ALIAS} -dname "${DNAME}" -ext bc:c -storepass ${PSWD} -keypass ${PSWD} -noprompt -v

    kkeytool -storepass ${PSWD} -keypass ${PSWD} -keystore ${ALIAS}.jks -certreq -alias ${ALIAS} \
      | keytool -storepass ${PSWD} -keypass ${PSWD} -keystore ${ROOT}.jks -gencert -alias ${ROOT} -ext BC=0 -rfc > ${ALIAS}.pem
}

function error () {
    echo "Что-то пошло не так... $@"
    exit 666
}
