#!/usr/bin/env bash

source ./functions.sh

PASS=123456

rm -rf *.jks *.pem

keytool -genkeypair -keystore root.jks -alias root -dname "CN=Ignite Root" -ext bc:c -keypass ${PASS} -storepass ${PASS} -noprompt -v

keytool -keystore root.jks -keypass ${PASS} -storepass ${PASS} -alias root -exportcert -rfc > root.pem

keytool -genkeypair -keystore ca.jks -alias ca -dname "CN=Ignite CA" \
 -storepass ${PASS} -keypass ${PASS} -noprompt -v

keytool -storepass ${PASS} -keystore ca.jks -certreq -alias ca \
 | keytool -storepass ${PASS} -keystore root.jks -gencert -alias root -ext BC=0 -rfc > ca.pem

# Импорт полученного сертификата в jks.
keytool -keystore ca.jks -storepass ${PASS} -importcert -alias root -file root.pem -noprompt -v
keytool -keystore ca.jks -storepass ${PASS} -importcert -alias ca -file ca.pem -noprompt -v
