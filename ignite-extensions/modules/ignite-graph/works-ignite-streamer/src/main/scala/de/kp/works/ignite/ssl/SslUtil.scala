package de.kp.works.ignite.ssl
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder
import org.bouncycastle.openssl.{PEMEncryptedKeyPair, PEMKeyPair, PEMParser}

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.file.{Files, Paths}
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, KeyStore, PrivateKey}
import javax.net.ssl.{KeyManagerFactory, TrustManager, TrustManagerFactory}

object SslUtil {

  private val CA_CERT_ALIAS = "caCert-cert"
  private val CERT_ALIAS = "cert"

  private val PRIVATE_KEY_ALIAS = "private-key"

  private def isNullOrEmpty(text: String): Boolean = {

    if (text == null)
      return true

    text.isEmpty

  }

  /** KEY MANAGER SUPPORT * */

  def getStoreKeyManagerFactory(keystoreFile: String, keystoreType: String, keystorePass: String, keystoreAlgo: String): KeyManagerFactory = {

    var keystore = loadKeystore(keystoreFile, keystoreType, keystorePass)
    /*
     * We have to manually fall back to default keystore. SSLContext won't provide
     * such a functionality.
     */
    if (keystore == null) {

      val ksFile = System.getProperty("javax.net.ssl.keyStore")
      val ksType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType)
      val ksPass = System.getProperty("javax.net.ssl.keyStorePassword", "")

      keystore = loadKeystore(ksFile, ksType, ksPass)

    }

    val algo =
      if (isNullOrEmpty(keystoreAlgo)) KeyManagerFactory.getDefaultAlgorithm else keystoreAlgo

    val factory = KeyManagerFactory.getInstance(algo)

    val password =
      if (keystorePass == null) null else keystorePass.toCharArray

    factory.init(keystore, password)
    factory

  }

  def getCertFileKeyManagerFactory(crtFile: String, keyFile: String, keyPass: String): KeyManagerFactory = {

    val cert = getX509CertFromPEM(crtFile)
    val privateKey = getPrivateKeyFromPEM(keyFile, keyPass)

    getCertKeyManagerFactory(cert, privateKey, keyPass)

  }

  def getCertKeyManagerFactory(cert: X509Certificate, privateKey: PrivateKey, password: String): KeyManagerFactory = {

    val keystore = createKeystore
    /*
     * Add client certificate to key store, the client certificate alias is
     * 'certificate' (see IBM Watson IoT platform)
     */
    val certAlias = CERT_ALIAS
    keystore.setCertificateEntry(certAlias, cert)

    /*
     * Add private key to keystore and distinguish between use case with and without
     * password
     */
    val passwordArray = if (password != null) password.toCharArray else null

    val keyAlias = PRIVATE_KEY_ALIAS
    keystore.setKeyEntry(keyAlias, privateKey, passwordArray, Array(cert))

    /*
     * Initialize key manager from the key store; note, the default algorithm also
     * supported by IBM Watson IoT platform is PKIX
     */
    val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    factory.init(keystore, passwordArray)

    factory

  }

  /** TRUST MANAGER SUPPORT * */

  def getAllTrustManagers: Array[TrustManager] = {
    Array[TrustManager](new AllTrustManager())
  }

  def getStoreTrustManagerFactory(truststoreFile: String, truststoreType: String, truststorePass: String, truststoreAlgo: String): TrustManagerFactory = {

     val trustStore = SslUtil.loadKeystore(truststoreFile, truststoreType, truststorePass)

    if (trustStore != null) {

      val algo =
        if (isNullOrEmpty(truststoreAlgo)) TrustManagerFactory.getDefaultAlgorithm else truststoreAlgo

      val factory = TrustManagerFactory.getInstance(algo)
      factory.init(trustStore)

      factory

    } else null

  }

  def getCertFileTrustManagerFactory(caCrtFile: String): TrustManagerFactory = {

    val caCert = getX509CertFromPEM(caCrtFile)
    getCertTrustManagerFactory(caCert)

  }


  def getCertTrustManagerFactory(caCert: X509Certificate): TrustManagerFactory = {

    val keyStore = createKeystore
    /*
     * Add CA certificate to keystore; note, the CA certificate alias is set to
     * 'ca-certificate' (see IBM Watson IoT platform)
     */
    val caCertAlias = CA_CERT_ALIAS
    keyStore.setCertificateEntry(caCertAlias, caCert)

    /*
     * Establish certificate trust chain; note, the default algorithm also supported
     * by IBM Watson IoT platform is PKIX
     */
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    factory.init(keyStore)

    factory

  }

  /*
   * Load a Java KeyStore of 'keystoreType', loacted at 'keystoreFile'
   * and secured with 'keystorePassword'
   */
  def loadKeystore(keystoreFile: String, keystoreType: String, keystorePassword: String): KeyStore = {

    var keystore: KeyStore = null
    if (keystoreFile != null) {

      keystore = KeyStore.getInstance(keystoreType)
      val passwordArr =
        if (keystorePassword == null) null else keystorePassword.toCharArray

      val is = Files.newInputStream(Paths.get(keystoreFile))
      keystore.load(is, passwordArr)

    }

    keystore

  }

  /*
   * Create a default (JKS) keystore without any password. Method load(null, null)
   * indicates to create a new one
   */
  def createKeystore: KeyStore = {

    val keystore = KeyStore.getInstance(KeyStore.getDefaultType)
    keystore.load(null, null)

    keystore

  }

  /** *** X509 CERTIFICATE **** */

  private def getX509CertFromPEM(crtFile: String): X509Certificate = {
    /*
     * Since Java cannot read PEM formatted certificates, this method is using
     * bouncy castle (http://www.bouncycastle.org/) to load the necessary files.
     *
     * IMPORTANT: Bouncycastle Provider must be added before this method can be
     * called
     *
     */
    val bytes = Files.readAllBytes(Paths.get(crtFile))
    val bais = new ByteArrayInputStream(bytes)

    val reader = new PEMParser(new InputStreamReader(bais))
    val cert = reader.readObject().asInstanceOf[X509Certificate]

    reader.close()
    cert

  }

  /** *** PRIVATE KEY SUPPORT **** */

  def getPrivateKeyFromPEM(keyFile: String, keyPass: String): PrivateKey = {

    val bytes = Files.readAllBytes(Paths.get(keyFile))
    val bais = new ByteArrayInputStream(bytes)

    val reader = new PEMParser(new InputStreamReader(bais))
    val keyObject = reader.readObject()

    reader.close()

    val keyPair: PEMKeyPair = keyObject match {
      case pair: PEMEncryptedKeyPair =>

        if (keyPass == null)
          throw new Exception("Reading private key from file without password is not supported.")

        val passwordArray = keyPass.toCharArray
        val provider = new JcePEMDecryptorProviderBuilder().build(passwordArray)

        pair.decryptKeyPair(provider)

      case _ =>
        keyObject.asInstanceOf[PEMKeyPair]
    }

    val factory = KeyFactory.getInstance("RSA", "BC")
    val keySpec = new PKCS8EncodedKeySpec(keyPair.getPrivateKeyInfo.getEncoded())

    factory.generatePrivate(keySpec)

  }

}
