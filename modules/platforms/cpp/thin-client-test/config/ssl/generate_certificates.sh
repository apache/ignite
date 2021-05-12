#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

OSSL=$(command -v openssl11)

if [ -z "$OSSL" ]
then
    OSSL=$(command -v openssl)
fi

echo "Using following openssl: $OSSL"

function generate_ca {
    CA_KEY="$1.key"
    CA_CRT="$1.crt"
    OU="$2"

    # Generating CA private key and self-signed certificate
    $OSSL req \
        -newkey rsa:2048 -nodes -sha256 -keyout $CA_KEY \
        -subj "/C=US/ST=Massachusetts/L=Wakefield/CN=ignite.apache.org/O=The Apache Software Foundation/OU=$OU/emailAddress=dev@ignite.apache.org" \
        -x509 -days 3650 -out $CA_CRT
}

function generate_client_key_and_crt {
    CA_KEY="$1.key"
    CA_CRT="$1.crt"
    CA_SRL="$1.srl"
    CLIENT_KEY="$2.key"
    CLIENT_CSR="$2.scr"
    CLIENT_CRT="$2.crt"
    OU="$3"

    # Generating client private key and certificate signature request to be used for certificate signing
    $OSSL req \
        -newkey rsa:2048 -nodes -sha256 -keyout $CLIENT_KEY \
        -subj "/C=US/ST=Massachusetts/L=Wakefield/CN=ignite.apache.org/O=The Apache Software Foundation/OU=$OU/emailAddress=dev@ignite.apache.org" \
        -out $CLIENT_CSR

    # Signing client cerificate
    $OSSL x509 -req \
        -in $CLIENT_CSR -CA $CA_CRT -CAkey $CA_KEY -CAcreateserial \
        -days 3650 -sha256 -out $CLIENT_CRT

    # Cleaning up.
    rm -f $CLIENT_CSR

    # Protecting key with the password if required
    if [ "$4" == "1" ]; then
      openssl rsa -aes256 -in $CLIENT_KEY -passout pass:654321 -out $CLIENT_KEY
    fi
}

function generate_jks {
    CA_CRT="$1.crt"
    CA_JKS="$1.jks"
    SERVER_KEY="$2.key"
    SERVER_CRT="$2.crt"
    SERVER_PEM="$2.pem"
    SERVER_P12="$2.pkcs12"
    SERVER_JKS="$2.jks"

    rm -f $CA_JKS $SERVER_JKS

    cat $SERVER_KEY $SERVER_CRT > $SERVER_PEM

    $OSSL pkcs12 -export -passout pass:123456 -out $SERVER_P12 -in $SERVER_PEM
    
    keytool -import -v -trustcacerts \
        -file $CA_CRT -alias certificateauthority -noprompt \
        -keystore $CA_JKS -deststorepass 123456

    keytool -v -importkeystore \
        -srckeystore $SERVER_P12 -srcstoretype PKCS12 -srcstorepass 123456 \
        -destkeystore $SERVER_JKS -deststoretype JKS -deststorepass 123456

    rm -f $SERVER_P12 $SERVER_PEM
}

CA='ca'
CLIENT='client'
CLIENT_WITH_PASS='client_with_pass'
SERVER='server'
CA_UNKNOWN='ca_unknown'
CLIENT_UNKNOWN='client_unknown'

generate_ca $CA 'Apache Ignite CA'
generate_client_key_and_crt $CA $CLIENT 'Apache Ignite Client Test'
generate_client_key_and_crt $CA $CLIENT_WITH_PASS 'Apache Ignite Client Test' 1
generate_client_key_and_crt $CA $SERVER 'Apache Ignite Server Test'

# We won't sign up any other certs so we do not need CA key or srl
rm -f "$CA.key" "$CA.srl"

generate_jks $CA $SERVER

generate_ca $CA_UNKNOWN 'Unknown CA'
generate_client_key_and_crt $CA_UNKNOWN $CLIENT_UNKNOWN 'Unknown Client'

# We do not need this CA anymore
rm -f $CA_UNKNOWN*

# Re-naming everything as needed
cat $CLIENT.key $CLIENT.crt > "$CLIENT"_full.pem
cat $CLIENT_WITH_PASS.key $CLIENT_WITH_PASS.crt > "$CLIENT_WITH_PASS"_full.pem
cat $CLIENT_UNKNOWN.key $CLIENT_UNKNOWN.crt > $CLIENT_UNKNOWN.pem
mv $CA.jks trust.jks
mv $CA.crt ca.pem

rm -f $CLIENT.crt $CLIENT.key $CLIENT_WITH_PASS.key $CLIENT_WITH_PASS.crt $CLIENT_UNKNOWN.key $CLIENT_UNKNOWN.crt $SERVER_KEY $SERVER_CRT


