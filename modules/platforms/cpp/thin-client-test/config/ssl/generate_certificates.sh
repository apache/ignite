#!/bin/bash

OSSL=$(command -v openssl11)

if [ -z "$OSSL" ]
then
    OSSL=$(command -v openssl)
fi

echo "Using following openssl: $OSSL"

function generate_ca_and_client_key_and_crt {
    CA_KEY="$1.key"
    CA_CRT="$1.crt"
    CA_SRL="$1.srl"
    CLIENT_KEY="$2.key"
    CLIENT_CSR="$2.scr"
    CLIENT_CRT="$2.crt"

    # Generating CA private key and self-signed certificate
    $OSSL req \
        -newkey rsa:2048 -nodes -sha256 -keyout $CA_KEY \
        -subj '/C=US/ST=Massachusetts/L=Wakefield/CN=ignite.apache.org/O=The Apache Software Foundation/OU=Apache Ignite CA/emailAddress=dev@ignite.apache.org' \
        -x509 -days=3650 -out $CA_CRT

    # Generating client private key and certificate signature request to be used for certificate signing
    $OSSL req \
        -newkey rsa:2048 -nodes -sha256 -keyout $CLIENT_KEY \
        -subj '/C=US/ST=Massachusetts/L=Wakefield/CN=ignite.apache.org/O=The Apache Software Foundation/OU=Apache Ignite Test/emailAddress=dev@ignite.apache.org' \
        -out $CLIENT_CSR

    # Signing client cerificate
    $OSSL x509 -req \
        -in $CLIENT_CSR -CA $CA_CRT -CAkey $CA_KEY -CAcreateserial \
        -days=3650 -sha256 -out $CLIENT_CRT

    # Cleaning up.
    rm -f $CA_KEY $CLIENT_CSR $CA_SRL
}

generate_ca_and_client_key_and_crt 'ca' 'client'
generate_ca_and_client_key_and_crt 'ca_unknown' 'client_unknown'

rm -f 'ca_unknown'*

