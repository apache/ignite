package de.kp.works.ignite.ssl
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import com.typesafe.config.Config
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security._
import java.security.cert.X509Certificate
import javax.net.ssl._

class AllTrustManager extends X509TrustManager {

  def getAcceptedIssuers:Array[X509Certificate] = {
    Array.empty[X509Certificate]
  }

  def checkClientTrusted(chain:Array[X509Certificate], authType:String):Unit = {
  }

  def checkServerTrusted(chain:Array[X509Certificate], authType:String):Unit = {
  }

}

object SslOptions {

  def getOptions(securityCfg: Config): SslOptions = {

    val ksFile = {
      val v = securityCfg.getString("ksFile")
      if (v.isEmpty) None else Option(v)
    }

    val ksType = {
      val v = securityCfg.getString("ksType")
      if (v.isEmpty) None else Option(v)
    }

    val ksPass = {
      val v = securityCfg.getString("ksPass")
      if (v.isEmpty) None else Option(v)
    }

    val ksAlgo = {
      val v = securityCfg.getString("ksAlgo")
      if (v.isEmpty) None else Option(v)
    }

    val tsFile = {
      val v = securityCfg.getString("tsFile")
      if (v.isEmpty) None else Option(v)
    }

    val tsType = {
      val v = securityCfg.getString("tsType")
      if (v.isEmpty) None else Option(v)
    }

    val tsPass = {
      val v = securityCfg.getString("tsPass")
      if (v.isEmpty) None else Option(v)
    }

    val tsAlgo = {
      val v = securityCfg.getString("tsAlgo")
      if (v.isEmpty) None else Option(v)
    }

    val caCertFile = {
      val v = securityCfg.getString("caCertFile")
      if (v.isEmpty) None else Option(v)
    }

    val certFile = {
      val v = securityCfg.getString("certFile")
      if (v.isEmpty) None else Option(v)
    }

    val privateKeyFile = {
      val v = securityCfg.getString("privateKeyFile")
      if (v.isEmpty) None else Option(v)
    }

    val privateKeyFilePass = {
      val v = securityCfg.getString("privateKeyFilePass")
      if (v.isEmpty) None else Option(v)
    }

    new SslOptions(
      /* KEYSTORE SUPPORT */
      keystoreFile = ksFile,
      keystoreType = ksType,
      keystorePass = ksPass,
      keystoreAlgo = ksAlgo,

      /* TRUSTSTORE SUPPORT */
      truststoreFile = tsFile,
      truststoreType = tsType,
      truststorePass = tsPass,
      truststoreAlgo = tsAlgo,

      /* CERTIFICATE SUPPORT */
      caCertFile         = caCertFile,
      certFile           = certFile,
      privateKeyFile     = privateKeyFile,
      privateKeyFilePass = privateKeyFilePass)
  }

}

class SslOptions(

  val tlsVersion: String = "TLS",

  /* KEY STORE */

  /* Path to the keystore file */
  keystoreFile: Option[String] = None,
  /* Keystore type */
  keystoreType: Option[String] = None,
  /* Keystore password */
  keystorePass: Option[String] = None,
  /* Keystore algorithm */
  keystoreAlgo: Option[String] = None,

  /* TRUST STORE */

  /* Path to the truststore file */
  truststoreFile: Option[String] = None,
  /* Truststore type */
  truststoreType: Option[String] = None,
  /* Truststore password */
  truststorePass: Option[String] = None,
  /* Truststore algorithm */
  truststoreAlgo: Option[String] = None,

  /* CERTIFICATES */

  caCert: Option[X509Certificate] = None,
  cert: Option[X509Certificate] = None,
  privateKey: Option[PrivateKey] = None,
  privateKeyPass: Option[String] = None,

  /* CERTIFICATES FILES */

  caCertFile: Option[String] = None,
  certFile: Option[String] = None,
  privateKeyFile: Option[String] = None,
  privateKeyFilePass: Option[String] = None) {

  def getSslSocketFactory: SSLSocketFactory = {

    Security.addProvider(new BouncyCastleProvider())

    val keyManagerFactory = getKeyManagerFactory
    val trustManagerFactory = getTrustManagerFactory

    val sslContext = SSLContext.getInstance(tlsVersion)
    sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new java.security.SecureRandom())

    sslContext.getSocketFactory

  }
  def getKeyManagerFactory: KeyManagerFactory = {

    try {

      if (keystoreFile.isDefined && keystoreType.isDefined && keystorePass.isDefined && keystoreAlgo.isDefined) {
        /*
         * SSL authentication based on an existing key store
         */
        val ksFile = keystoreFile.get
        val ksType = keystoreType.get

        val ksPass = keystorePass.get
        val ksAlgo = keystoreAlgo.get

        SslUtil.getStoreKeyManagerFactory(ksFile, ksType, ksPass, ksAlgo)

      } else if (cert.isDefined && privateKey.isDefined && privateKeyPass.isDefined) {
        /*
         * SSL authentication based on a provided client certificate,
         * private key and associated password; the certificate will
         * be added to a newly created key store
         */
        SslUtil.getCertKeyManagerFactory(cert.get, privateKey.get, privateKeyPass.get)

      } else if (certFile.isDefined && privateKeyFile.isDefined && privateKeyFilePass.isDefined) {
        /*
         * SSL authentication based on a provided client certificate file,
         * private key file and associated password; the certificate will
         * be added to a newly created key store
         */
        SslUtil.getCertFileKeyManagerFactory(certFile.get, privateKeyFile.get, privateKeyFilePass.get)

      } else
        throw new Exception("Failed to retrieve KeyManager factory.")

    } catch {

      case _: Throwable =>
        /* Do nothing */
        null
    }

  }

  def getTrustManagerFactory: TrustManagerFactory = {

    try {

      if (truststoreFile.isDefined && truststoreType.isDefined && truststorePass.isDefined && truststoreAlgo.isDefined) {
        /*
         * SSL authentication based on an existing trust store
         */
        val tsFile = truststoreFile.get
        val tsType = truststoreType.get

        val tsPass = truststorePass.get
        val tsAlgo = truststoreAlgo.get

        SslUtil.getStoreTrustManagerFactory(tsFile, tsType, tsPass, tsAlgo)

      } else if (caCert.isDefined) {
        /*
         * SSL authentication based on a provided CA certificate;
         * this certificate will be added to a newly created trust
         * store
         */
        SslUtil.getCertTrustManagerFactory(caCert.get)

      } else if (caCertFile.isDefined) {
        /*
         * SSL authentication based on a provided CA certificate file;
         * the certificate is loaded and will be added to a newly created
         * trust store
         */
        SslUtil.getCertFileTrustManagerFactory(caCertFile.get)

      } else
        throw new Exception("Failed to retrieve TrustManager factory.")

    } catch {

      case _: Throwable =>
        /* Do nothing */
        null
    }

  }

  def getSslContext: SSLContext = {

    var keyManagers:Array[KeyManager] = null
    var trustManagers:Array[TrustManager] = null

    /** KEY STORE **/

    if (keystoreFile.isDefined && keystoreType.isDefined && keystorePass.isDefined && keystoreAlgo.isDefined) {

      val factory = SslUtil.getStoreKeyManagerFactory(
        keystoreFile.get,  keystoreType.get, keystorePass.get, keystoreAlgo.get)

      if (factory != null) keyManagers = factory.getKeyManagers
    }

    /** TRUST STORE **/

    if (truststoreFile.isDefined && truststoreType.isDefined & truststorePass.isDefined && truststoreAlgo.isDefined) {

      val factory = SslUtil.getStoreTrustManagerFactory(
        truststoreFile.get, truststoreType.get,  truststorePass.get, truststoreAlgo.get)

      if (factory != null) trustManagers = factory.getTrustManagers
    }


    /** CERTIFICATE FILE & PRIVATE KEY **/

    Security.addProvider(new BouncyCastleProvider())

    if (certFile.isDefined && privateKeyFile.isDefined && privateKeyFilePass.isDefined) {
      /*
       * SSL authentication based on a provided client certificate file,
       * private key file and associated password; the certificate will
       * be added to a newly created key store
       */
      val factory = SslUtil.getCertFileKeyManagerFactory(
        certFile.get, privateKeyFile.get, privateKeyFilePass.get)

      if (factory != null) keyManagers = factory.getKeyManagers

    }

    if (caCertFile.isDefined) {
      val factory = SslUtil.getCertFileTrustManagerFactory(caCertFile.get)
      if (factory != null) trustManagers = factory.getTrustManagers
    }


    val secureRandom = Option(new SecureRandom())
    buildSslContext(keyManagers, trustManagers, secureRandom)

  }

  def getTrustAllContext: SSLContext = {

    val keyManagers:Array[KeyManager] = null
    val trustManagers:Array[TrustManager] = Array(new AllTrustManager())

    val secureRandom = Option(new SecureRandom())
    buildSslContext(keyManagers, trustManagers, secureRandom)

  }

  private def buildSslContext(keyManagers:Seq[KeyManager], trustManagers:Seq[TrustManager], secureRandom:Option[SecureRandom]) = {

    val sslContext = SSLContext.getInstance(tlsVersion)

    sslContext.init(nullIfEmpty(keyManagers.toArray), nullIfEmpty(trustManagers.toArray), secureRandom.orNull)
    sslContext

  }

  private def nullIfEmpty[T](array: Array[T]) = {
    if (array.isEmpty) null else array
  }

}