#!/usr/bin/env bash
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

#
# SSL certificates generation.
#

#
# Preconditions:
#  1. If needed, install Open SSL (for example: "sudo apt-get install openssl")
#  2. If needed, install JDK 8 or newer. We need "keytool" from "JDK/bin".
#  3. Create "openssl.cnf" in some folder (for example: "/opt/openssl").
#     You may use "https://github.com/openssl/openssl/blob/master/apps/openssl.cnf" as template.
#  4. If needed, add "opensll" & "keytool" to PATH variable.
#
#  NOTE: In case of custom SERVER_DOMAIN_NAME you may need to tweak your "etc/hosts" file.
#

set -x

# Set Open SSL variables.
OPENSSL_CONF=/opt/openssl/openssl.cnf

# Certificates password.
PWD=p123456

# Server.
SERVER_DOMAIN_NAME=localhost
SERVER_EMAIL=support@test.local

# Client.
CLIENT_DOMAIN_NAME=localhost
CLIENT_EMAIL=client@test.local

# Cleanup.
rm -vf server.*
rm -vf client.*
rm -vf ca.*

# Generate server config.
cat << EOF > server.cnf
[req]
prompt                 = no
distinguished_name     = dn
req_extensions         = req_ext
[ dn ]
countryName            = RU
stateOrProvinceName    = Moscow
localityName           = Moscow
organizationName       = test
commonName             = ${SERVER_DOMAIN_NAME}
organizationalUnitName = IT
emailAddress           = ${SERVER_EMAIL}
[ req_ext ]
subjectAltName         = @alt_names
[ alt_names ]
DNS.1                  = ${SERVER_DOMAIN_NAME}
EOF

# Generate client config.
cat << EOF > client.cnf
[req]
prompt                 = no
distinguished_name     = dn
req_extensions         = req_ext
[ dn ]
countryName            = RU
stateOrProvinceName    = Moscow
localityName           = Moscow
organizationName       = test
commonName             = ${CLIENT_DOMAIN_NAME}
organizationalUnitName = IT
emailAddress           = ${CLIENT_EMAIL}
[ req_ext ]
subjectAltName         = @alt_names
[ alt_names ]
DNS.1                  = ${CLIENT_DOMAIN_NAME}
EOF

# Generate certificates.
openssl genrsa -des3 -passout pass:${PWD} -out server.key 1024
openssl req -new -passin pass:${PWD} -key server.key -config server.cnf -out server.csr
openssl req -new -newkey rsa:1024 -nodes -keyout ca.key -x509 -days 365 -config server.cnf -out ca.crt
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -extensions req_ext -extfile server.cnf -out server.crt
openssl rsa -passin pass:${PWD} -in server.key -out server.nopass.key
openssl req -new -utf8 -nameopt multiline,utf8 -newkey rsa:1024 -nodes -keyout client.key -config client.cnf -out client.csr
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt
openssl pkcs12 -export -in server.crt -inkey server.key -certfile server.crt -out server.p12 -passin pass:${PWD} -passout pass:${PWD}
openssl pkcs12 -export -in client.crt -inkey client.key -certfile ca.crt -out client.p12 -passout pass:${PWD}
openssl pkcs12 -export -in ca.crt -inkey ca.key -certfile ca.crt -out ca.p12 -passout pass:${PWD}
keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 -destkeystore server.jks -deststoretype JKS -noprompt -srcstorepass ${PWD} -deststorepass ${PWD}
keytool -importkeystore -srckeystore client.p12 -srcstoretype PKCS12 -destkeystore client.jks -deststoretype JKS -noprompt -srcstorepass ${PWD} -deststorepass ${PWD}
keytool -importkeystore -srckeystore ca.p12 -srcstoretype PKCS12 -destkeystore ca.jks -deststoretype JKS -noprompt -srcstorepass ${PWD} -deststorepass ${PWD}
openssl x509 -text -noout -in server.crt
openssl x509 -text -noout -in client.crt
openssl x509 -text -noout -in ca.crt
