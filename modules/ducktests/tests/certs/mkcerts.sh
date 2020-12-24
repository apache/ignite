#!/usr/bin/env bash

source ./functions.sh

PSWD=123456

rm -rf *.jks *.pem

makeRoot root "CN=Ignite Root" ${PSWD}

#keytool -genkeypair -keystore root.jks -alias root -dname "CN=Ignite Root" -ext bc:c -storepass ${PSWD} -keypass ${PSWD} -noprompt -v
keytool -genkeypair -keystore ca.jks -alias ca -dname "CN=Ignite CA" -ext bc:c -storepass ${PSWD} -keypass ${PSWD} -noprompt -v
keytool -genkeypair -keystore server.jks -alias server -dname "CN=Ignite Server" -storepass ${PSWD} -keypass ${PSWD} -noprompt -v

#keytool -keystore root.jks -storepass ${PSWD} -keypass ${PSWD} -alias root -exportcert -rfc > root.pem

keytool -storepass ${PSWD} -keypass ${PSWD} -keystore ca.jks -certreq -alias ca \
 | keytool -storepass ${PSWD} -keypass ${PSWD} -keystore root.jks -gencert -alias root -ext BC=0 -rfc > ca.pem

keytool -storepass ${PSWD} -keypass ${PSWD} -keystore server.jks -certreq -alias server \
 | keytool -storepass ${PSWD} -keypass ${PSWD} -keystore ca.jks -gencert -alias ca -rfc > server.pem

keytool -keystore ca.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias root -file root.pem -v
keytool -keystore ca.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias ca -file ca.pem -v

#cat root.pem ca.pem server.pem | keytool -keystore server.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias server

for cert in root ca server
do
  keytool -keystore server.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias ${cert} -file ${cert}.pem -v
done


#keytool -keystore server.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias root -file root.pem -v
#keytool -keystore server.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias ca -file ca.pem -v
#keytool -keystore server.jks -storepass ${PSWD} -keypass ${PSWD} -importcert -alias server -file server.pem -v