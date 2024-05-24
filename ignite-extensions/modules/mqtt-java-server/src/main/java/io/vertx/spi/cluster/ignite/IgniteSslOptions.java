/*
 * Copyright 2020 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;

import static org.apache.ignite.ssl.SslContextFactory.*;

/**
 * @author Lukas Prettenthaler
 */
@DataObject(generateConverter = true)
public class IgniteSslOptions {
  private String protocol;
  private String keyAlgorithm;
  private String keyStoreType;
  private String keyStoreFilePath;
  private String keyStorePassword;
  private String trustStoreType;
  private String trustStoreFilePath;
  private String trustStorePassword;
  private PemKeyCertOptions pemKeyCertOptions;
  private PemTrustOptions pemTrustOptions;
  private PfxOptions pfxKeyCertOptions;
  private PfxOptions pfxTrustOptions;
  private JksOptions jksKeyCertOptions;
  private JksOptions jksTrustOptions;
  private boolean trustAll;

  /**
   * Default constructor
   */
  public IgniteSslOptions() {
    protocol = DFLT_SSL_PROTOCOL;
    keyAlgorithm = DFLT_KEY_ALGORITHM;
    keyStoreType = DFLT_STORE_TYPE;
    trustStoreType = DFLT_STORE_TYPE;
    trustAll = false;
  }

  /**
   * Copy constructor
   *
   * @param options the one to copy
   */
  public IgniteSslOptions(IgniteSslOptions options) {
    this.protocol = options.protocol;
    this.keyAlgorithm = options.keyAlgorithm;
    this.keyStoreType = options.keyStoreType;
    this.keyStoreFilePath = options.keyStoreFilePath;
    this.keyStorePassword = options.keyStorePassword;
    this.trustStoreType = options.trustStoreType;
    this.trustStoreFilePath = options.trustStoreFilePath;
    this.trustStorePassword = options.trustStorePassword;
    this.pemKeyCertOptions = options.pemKeyCertOptions;
    this.pemTrustOptions = options.pemTrustOptions;
    this.pfxKeyCertOptions = options.pfxKeyCertOptions;
    this.pfxTrustOptions = options.pfxTrustOptions;
    this.jksKeyCertOptions = options.jksKeyCertOptions;
    this.jksTrustOptions = options.jksTrustOptions;
    this.trustAll = options.trustAll;
  }

  /**
   * Constructor from JSON
   *
   * @param options the JSON
   */
  public IgniteSslOptions(JsonObject options) {
    this();
    IgniteSslOptionsConverter.fromJson(options, this);
  }

  /**
   * Gets protocol for secure transport.
   *
   * @return SSL protocol name.
   */
  public String getProtocol() {
    return protocol;
  }

  /**
   * Sets protocol for secure transport.
   *
   * @param protocol SSL protocol name.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setProtocol(String protocol) {
    this.protocol = protocol;
    return this;
  }

  /**
   * Gets algorithm that will be used to create a key manager.
   *
   * @return Key manager algorithm.
   * @deprecated Use vert.x ssl certificate options instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getKeyAlgorithm() {
    return keyAlgorithm;
  }

  /**
   * Sets key manager algorithm that will be used to create a key manager. Notice that in most cased default value
   * suites well, however, on Android platform this value need to be set to <tt>X509<tt/>.
   *
   * @param keyAlgorithm Key algorithm name.
   * @return reference to this, for fluency
   * @deprecated Use vert.x ssl certificate options instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setKeyAlgorithm(String keyAlgorithm) {
    this.keyAlgorithm = keyAlgorithm;
    return this;
  }

  /**
   * Gets key store type used for context creation.
   *
   * @return Key store type.
   * @deprecated Use vert.x ssl certificate options instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getKeyStoreType() {
    return keyStoreType;
  }

  /**
   * Sets key store type used in context initialization.
   *
   * @param keyStoreType Key store type.
   * @return reference to this, for fluency
   * @deprecated Use vert.x ssl certificate options instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setKeyStoreType(String keyStoreType) {
    this.keyStoreType = keyStoreType;
    return this;
  }

  /**
   * Gets path to the key store file.
   *
   * @return Path to key store file.
   * @deprecated Use vert.x jksKeyCertOptions.path instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getKeyStoreFilePath() {
    return keyStoreFilePath;
  }

  /**
   * Sets path to the key store file. This is a mandatory parameter since
   * ssl context could not be initialized without key manager.
   *
   * @param keyStoreFilePath Path to key store file.
   * @return reference to this, for fluency
   * @deprecated Use vert.x jksKeyCertOptions.path instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setKeyStoreFilePath(String keyStoreFilePath) {
    this.keyStoreFilePath = keyStoreFilePath;
    return this;
  }

  /**
   * Gets key store password.
   *
   * @return Key store password.
   * @deprecated Use vert.x jksKeyCertOptions.password instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  /**
   * Sets key store password.
   *
   * @param keyStorePassword Key store password.
   * @return reference to this, for fluency
   * @deprecated Use vert.x jksKeyCertOptions.password instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
    return this;
  }

  /**
   * Gets trust store type used for context creation.
   *
   * @return trust store type.
   * @deprecated Use vert.x jksTrustOptions instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getTrustStoreType() {
    return trustStoreType;
  }

  /**
   * Sets trust store type used in context initialization.
   *
   * @param trustStoreType Trust store type.
   * @return reference to this, for fluency
   * @deprecated Use vert.x jksTrustOptions instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setTrustStoreType(String trustStoreType) {
    this.trustStoreType = trustStoreType;
    return this;
  }

  /**
   * Gets path to the trust store file.
   *
   * @return Path to the trust store file.
   * @deprecated Use vert.x jksTrustOptions.path instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getTrustStoreFilePath() {
    return trustStoreFilePath;
  }

  /**
   * Sets path to the trust store file.
   *
   * @param trustStoreFilePath Path to the trust store file.
   * @return reference to this, for fluency
   * @deprecated Use vert.x jksTrustOptions.path instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setTrustStoreFilePath(String trustStoreFilePath) {
    this.trustStoreFilePath = trustStoreFilePath;
    return this;
  }

  /**
   * Gets trust store password.
   *
   * @return Trust store password.
   * @deprecated Use vert.x jksTrustOptions.password instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * Sets trust store password.
   *
   * @param trustStorePassword Trust store password.
   * @return reference to this, for fluency
   * @deprecated Use vert.x jksTrustOptions.password instead. It will be removed in vert.x 4.1
   */
  @Deprecated
  public IgniteSslOptions setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  public PemKeyCertOptions getPemKeyCertOptions() {
    return pemKeyCertOptions;
  }

  /**
   * Sets PemKeyCertOptions that will be used for creating a secure socket layer.
   *
   * @param pemKeyCertOptions Vertx PEM KeyCertOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setPemKeyCertOptions(PemKeyCertOptions pemKeyCertOptions) {
    this.pemKeyCertOptions = pemKeyCertOptions;
    return this;
  }

  public PemTrustOptions getPemTrustOptions() {
    return pemTrustOptions;
  }

  /**
   * Sets PemTrustOptions that will be used for creating a secure socket layer.
   *
   * @param pemTrustOptions Vertx PEM TrustOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setPemTrustOptions(PemTrustOptions pemTrustOptions) {
    this.pemTrustOptions = pemTrustOptions;
    return this;
  }

  public PfxOptions getPfxKeyCertOptions() {
    return pfxKeyCertOptions;
  }

  /**
   * Sets PfxKeyCertOptions that will be used for creating a secure socket layer.
   *
   * @param pfxKeyCertOptions Vertx PFX KeyCertOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setPfxKeyCertOptions(PfxOptions pfxKeyCertOptions) {
    this.pfxKeyCertOptions = pfxKeyCertOptions;
    return this;
  }

  public PfxOptions getPfxTrustOptions() {
    return pfxTrustOptions;
  }

  /**
   * Sets PfxTrustOptions that will be used for creating a secure socket layer.
   *
   * @param pfxTrustOptions Vertx PFX TrustOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setPfxTrustOptions(PfxOptions pfxTrustOptions) {
    this.pfxTrustOptions = pfxTrustOptions;
    return this;
  }

  public JksOptions getJksKeyCertOptions() {
    return jksKeyCertOptions;
  }

  /**
   * Sets JksKeyCertOptions that will be used for creating a secure socket layer.
   *
   * @param jksKeyCertOptions Vertx JKS KeyCertOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setJksKeyCertOptions(JksOptions jksKeyCertOptions) {
    this.jksKeyCertOptions = jksKeyCertOptions;
    return this;
  }

  public JksOptions getJksTrustOptions() {
    return jksTrustOptions;
  }

  /**
   * Sets JksTrustOptions that will be used for creating a secure socket layer.
   *
   * @param jksTrustOptions Vertx JKS TrustOptions.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setJksTrustOptions(JksOptions jksTrustOptions) {
    this.jksTrustOptions = jksTrustOptions;
    return this;
  }

  /**
   * When using ssl, trust ALL certificates.
   * WARNING Trusting ALL certificates will open you up to potential security issues such as MITM attacks.
   *
   * @return Trust all flag.
   */
  public boolean isTrustAll() {
    return trustAll;
  }

  /**
   * When using ssl, trust ALL certificates.
   * WARNING Trusting ALL certificates will open you up to potential security issues such as MITM attacks.
   *
   * @param trustAll Trust all flag.
   * @return reference to this, for fluency
   */
  public IgniteSslOptions setTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
    return this;
  }

  /**
   * Convert to JSON
   *
   * @return the JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    IgniteSslOptionsConverter.toJson(this, json);
    return json;
  }
}
