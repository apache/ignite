#
# Initialize openssl & CA.
#
# Instruction: http://fusesource.com/docs/framework/2.1/security/i305191.html
#

# Path to CA certificate.
ca_cert=/usr/ssl/ca/ca.pem

#
# Create artifacts for specified name: key pair-> cert request -> ca-signed certificate.
# Save private key and CA-signed certificate into key storages: PEM, JKS, PFX (PKCS12).
#
# param $1 Artifact name.
# param $2 Password for all keys and storages.
#
function createStore {
	artifact=$1
	pwd=$2

	echo
	echo Clean up all old artifacts: ${artifact}.*
	rm -f ${artifact}.*
	
	echo
	echo Generate a certificate and private key pair for ${artifact}.
	keytool -genkey -keyalg RSA -keysize 1024 -dname "emailAddress=${artifact}@gridgain.com, CN=${artifact}, OU=Dev, O=GridGain, L=SPb, ST=SPb, C=RU" -validity 7305 -alias ${artifact} -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}

	echo
	echo Create a certificate signing request for ${artifact}.
	keytool -certreq -alias ${artifact} -file ${artifact}.csr -keypass ${pwd} -keystore ${artifact}.jks -storepass ${pwd}
	
	echo
	echo "Sign the CSR using CA (default SSL configuration)."
	openssl ca -days 7305 -in ${artifact}.csr -out ${artifact}.pem
	
	echo
	echo Convert to PEM format.
	openssl x509 -in ${artifact}.pem -out ${artifact}.pem -outform PEM
	
	echo
	echo Concatenate the CA certificate file and ${artifact}.pem certificate file into certificates chain.
	cat ${artifact}.pem ${ca_cert} > ${artifact}.chain
	
	echo
	echo Update the keystore, ${artifact}.jks, by importing the CA certificate.
	keytool -import -alias ca          -file ${ca_cert} -keypass ${pwd} -noprompt -trustcacerts -keystore ${artifact}.jks -storepass ${pwd} 

	echo
	echo Update the keystore, ${artifact}.jks, by importing the full certificate chain for the ${artifact}.
	keytool -import -alias ${artifact} -file ${artifact}.chain -keypass ${pwd} -noprompt -trustcacerts -keystore ${artifact}.jks -storepass ${pwd} 
	
	echo
	echo Generate PKCS12 storage for the private key and certificate chain.
	keytool -importkeystore \
		-srcstoretype JKS -deststoretype PKCS12 \
		-srckeystore ${artifact}.jks -destkeystore ${artifact}.pfx \
		-srcstorepass ${pwd} -deststorepass ${pwd} \
		-srcalias ${artifact} -destalias ${artifact} \
		-srckeypass ${pwd} -destkeypass ${pwd} \
		-noprompt
	
	echo
	echo Generate PEM storage for the private key and certificate chain.
	openssl pkcs12 \
		-in ${artifact}.pfx -out ${artifact}.pem \
		-passin pass:${pwd} -passout pass:${pwd}
		
	rm -f ${artifact}.chain ${artifact}.csr
}

pwd="123456"

createStore "client" ${pwd}
createStore "server" ${pwd}

echo
echo Update trust store with certificates: CA, client, server.
keytool -import -alias ca -file ${ca_cert} -keypass ${pwd} -noprompt -trustcacerts -keystore trust.jks -storepass ${pwd}
#keytool -importkeystore -srckeystore client.jks -destkeystore trust.jks -srcstorepass ${pwd} -deststorepass ${pwd} -alias client -noprompt
#keytool -importkeystore -srckeystore server.jks -destkeystore trust.jks -srcstorepass ${pwd} -deststorepass ${pwd} -alias server -noprompt
keytool -export -alias client -keystore client.jks -storepass ${pwd} | keytool -importcert -alias client -noprompt -keystore trust.jks -storepass ${pwd}
keytool -export -alias server -keystore server.jks -storepass ${pwd} | keytool -importcert -alias server -noprompt -keystore trust.jks -storepass ${pwd}