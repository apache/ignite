#!/bin/sh

#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -e

pwd="123456"

#
# Create artifacts for specified name: key pair-> cert request -> ca-signed certificate.
# Save private key and CA-signed certificate into JKS key storage.
#
# param $1 Artifact name.
# param $2 Name of a certificate authority.
# param $3 If true, then an expired certificate will be generated.
#
function createStore {
	artifact=$1
	ca_name=$2
	expired=$3

	if [[ "$expired" = true ]]; then
        startdate=`date -d '2 days ago' '+%y%m%d%H%M%SZ'`
        enddate=`date -d 'yesterday' '+%y%m%d%H%M%SZ'`
    else
        startdate=`date -d 'today 00:00:00' '+%y%m%d%H%M%SZ'`
        enddate=`date -d 'today + 7305 days' '+%y%m%d%H%M%SZ'`
    fi

	ca_cert=ca/${ca_name}.pem

	echo
	echo Clean up all old artifacts: ${artifact}.*
	rm -f ${artifact}.*

	echo
	echo Generate a certificate and private key pair for ${artifact}.
	keytool -genkey -keyalg RSA -keysize 1024 \
	        -dname "emailAddress=${artifact}@ignite.apache.org, CN=${artifact}, OU=Dev, O=Ignite, L=SPb, ST=SPb, C=RU" \
	        -alias ${artifact} -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Create a certificate signing request for ${artifact}.
	keytool -certreq -alias ${artifact} -file ${artifact}.csr -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Sign the CSR using ${ca_name}.
	openssl ca -config ca/${ca_name}.cnf \
	        -startdate ${startdate} -enddate ${enddate} \
	        -batch -out ${artifact}.pem -infiles ${artifact}.csr

	echo
	echo Convert to PEM format.
	openssl x509 -in ${artifact}.pem -out ${artifact}.pem -outform PEM

	echo
	echo Concatenate the CA certificate file and ${artifact}.pem certificate file into certificates chain.
	cat ${artifact}.pem ${ca_cert} > ${artifact}.chain

	echo
	echo Update the keystore, ${artifact}.jks, by importing the full certificate chain for the ${artifact}.
	keytool -import -alias ${artifact} -file ${artifact}.chain -keypass ${pwd} -noprompt -trustcacerts -keystore ${artifact}.jks -storepass ${pwd}

	rm -f ${artifact}.chain ${artifact}.csr ${artifact}.pem
}

mkdir -p ca/certs

createStore client oneca
createStore server oneca
createStore thinClient twoca
createStore thinServer twoca
createStore connectorClient threeca
createStore connectorServer threeca

createStore node01 oneca
createStore node02 twoca
createStore node03 twoca
createStore node02old twoca true
