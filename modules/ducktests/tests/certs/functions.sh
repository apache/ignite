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

# Создает сертификат JKS.
# Потом импортирует его в формат PKCS12 и формирует файл с расширением .p12.
# 1 CA, 2 ALIAS, 3 DNAME, 4 DAYS(default 3650).
function mkcertUser () {
    mkcert $1 $2 "${3}" $4

    rm -rf  $2".p12"

    keytool -importkeystore -srcstoretype JKS -srckeystore $2".jks" -srcstorepass ${PSWD} -deststoretype PKCS12 \
     -destkeystore $2".p12" -deststorepass ${PSWD} -destkeypass ${PSWD} -noprompt || error
}

function error () {
    echo "Что-то пошло не так... $@"
    exit 666
}
