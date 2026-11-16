/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.internal.systemview.SensitiveProperty;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * SSL context factory that provides SSL context configuration with specified key and trust stores.
 * <p>
 * In some cases it is useful to disable certificate validation of client side (e.g. when connecting
 * to a server with self-signed certificate). This can be achieved by setting a disabled trust manager
 * to this factory, which can be obtained by {@link #getDisabledTrustManager()} method:
 * <pre>
 *     SslContextFactory factory = new SslContextFactory();
 *     factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
 *     // Rest of initialization.
 * </pre>
 */
public class SslContextFactory extends AbstractSslContextFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default key / trust store type. */
    public static final String DFLT_STORE_TYPE = System.getProperty("javax.net.ssl.keyStoreType", "JKS");

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = AbstractSslContextFactory.DFLT_SSL_PROTOCOL;

    /** Default key manager / trust manager algorithm. Specifying different trust manager algorithm is not supported. */
    public static final String DFLT_KEY_ALGORITHM = System.getProperty("ssl.KeyManagerFactory.algorithm", "SunX509");

    /** Default lead time for certificate hot reload: 7 days. */
    public static final long DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS = TimeUnit.DAYS.toMillis(7);

    /** Key manager algorithm. */
    protected String keyAlgorithm = DFLT_KEY_ALGORITHM;

    /** Key store type. */
    protected String keyStoreType = DFLT_STORE_TYPE;

    /** Path to key store file */
    protected String keyStoreFilePath;

    /** Key store password */
    protected char[] keyStorePwd;

    /** Trust store type. */
    protected String trustStoreType = DFLT_STORE_TYPE;

    /** Path to trust store. */
    protected String trustStoreFilePath;

    /** Trust store password */
    protected char[] trustStorePwd;

    /** Trust managers. */
    protected TrustManager[] trustMgrs;

    /** */
    private boolean hotReloadEnabled;

    /** */
    private volatile long hotReloadBeforeExpiryMs = DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;

    /** */
    private transient volatile long hotReloadTime;

    /** */
    private transient long pendingHotReloadTime;

    /**
     * @return Whether automatic certificate hot reload is enabled. Defaults to {@code false}.
     */
    public boolean isHotReloadEnabled() {
        return hotReloadEnabled;
    }

    /**
     * @param enabled Whether automatic certificate hot reload is enabled. Changes require a node restart.
     */
    public void setHotReloadEnabled(boolean enabled) {
        hotReloadEnabled = enabled;
    }

    /**
     * @return How long before the certificates expire hot reload becomes due, in milliseconds.
     *     Defaults to {@link #DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS}.
     */
    public long getHotReloadBeforeExpiryMs() {
        return hotReloadBeforeExpiryMs;
    }

    /**
     * @param millis How long before the certificates expire hot reload becomes due, in milliseconds. Must be positive.
     *     The factory applies a change at its next load.
     */
    public void setHotReloadBeforeExpiryMs(long millis) {
        A.ensure(millis > 0, "millis > 0");

        hotReloadBeforeExpiryMs = millis;
    }

    /**
     * The hot reload time is calculated from the certificates loaded with hot reload enabled.
     * It comes {@link #getHotReloadBeforeExpiryMs()} before the earliest expiry among the chains of the private keys.
     * A chain expires when any certificate in it expires.
     * Keys without an X.509 certificate chain are ignored, as TLS cannot use them.
     *
     * @return Hot reload time in milliseconds since the epoch, or {@code 0} if the current certificates
     *     were loaded with hot reload disabled, have not been loaded yet, or have no private key with an X.509 chain.
     */
    public long getHotReloadTime() {
        return hotReloadTime;
    }

    /** {@inheritDoc} */
    @Override SSLContext createSslContext() throws SSLException {
        SSLContext ctx = super.createSslContext();

        hotReloadTime = pendingHotReloadTime;

        return ctx;
    }

    /**
     * Gets key store type used for context creation.
     *
     * @return Key store type.
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Sets key store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
     * be used.
     *
     * @param keyStoreType Key store type.
     */
    public void setKeyStoreType(String keyStoreType) {
        A.notNull(keyStoreType, "keyStoreType");

        this.keyStoreType = keyStoreType;
    }

    /**
     * Gets trust store type used for context creation.
     *
     * @return trust store type.
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * Sets trust store type used in context initialization. If not provided, {@link #DFLT_STORE_TYPE} will
     * be used.
     *
     * @param trustStoreType Trust store type.
     */
    public void setTrustStoreType(String trustStoreType) {
        A.notNull(trustStoreType, "trustStoreType");

        this.trustStoreType = trustStoreType;
    }

    /**
     * Gets algorithm that will be used to create a key manager. If not specified, {@link #DFLT_KEY_ALGORITHM}
     * will be used.
     *
     * @return Key manager algorithm.
     */
    public String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Sets key manager algorithm that will be used to create a key manager.
     *
     * @param keyAlgorithm Key algorithm name.
     */
    public void setKeyAlgorithm(String keyAlgorithm) {
        A.notNull(keyAlgorithm, "keyAlgorithm");

        this.keyAlgorithm = keyAlgorithm;
    }

    /**
     * Gets path to the key store file.
     *
     * @return Path to key store file.
     */
    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    /**
     * Sets path to the key store file. This is a mandatory parameter since
     * ssl context could not be initialized without key manager.
     *
     * @param keyStoreFilePath Path to key store file.
     */
    public void setKeyStoreFilePath(String keyStoreFilePath) {
        A.notNull(keyStoreFilePath, "keyStoreFilePath");

        this.keyStoreFilePath = keyStoreFilePath;
    }

    /**
     * Gets key store password.
     *
     * @return Key store password.
     */
    @SensitiveProperty
    public char[] getKeyStorePassword() {
        return keyStorePwd;
    }

    /**
     * Sets key store password.
     *
     * @param keyStorePwd Key store password.
     */
    public void setKeyStorePassword(char[] keyStorePwd) {
        A.notNull(keyStorePwd, "keyStorePwd");

        this.keyStorePwd = keyStorePwd;
    }

    /**
     * Gets path to the trust store file.
     *
     * @return Path to the trust store file.
     */
    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    /**
     * Sets path to the trust store file. This is an optional parameter,
     * however one of the {@code setTrustStoreFilePath(String)}, {@link #setTrustManagers(TrustManager[])}
     * properties must be set.
     *
     * @param trustStoreFilePath Path to the trust store file.
     */
    public void setTrustStoreFilePath(String trustStoreFilePath) {
        this.trustStoreFilePath = trustStoreFilePath;
    }

    /**
     * Gets trust store password.
     *
     * @return Trust store password.
     */
    @SensitiveProperty
    public char[] getTrustStorePassword() {
        return trustStorePwd;
    }

    /**
     * Sets trust store password.
     *
     * @param trustStorePwd Trust store password.
     */
    public void setTrustStorePassword(char[] trustStorePwd) {
        this.trustStorePwd = trustStorePwd;
    }

    /**
     * Gets pre-configured trust managers.
     *
     * @return Trust managers.
     */
    public TrustManager[] getTrustManagers() {
        return trustMgrs;
    }

    /**
     * Sets pre-configured trust managers. This is an optional parameter,
     * however one of the {@link #setTrustStoreFilePath(String)}, {@code #setTrustManagers(TrustManager[])}
     *
     * @param trustMgrs Pre-configured trust managers.
     */
    public void setTrustManagers(TrustManager... trustMgrs) {
        this.trustMgrs = trustMgrs;
    }

    /**
     * Returns an instance of trust manager that will always succeed regardless of certificate provided.
     *
     * @return Trust manager instance.
     */
    public static TrustManager getDisabledTrustManager() {
        return new DisabledX509TrustManager();
    }

    /** {@inheritDoc} */
    @Override protected final KeyManager[] createKeyManagers() throws SSLException {
        KeyManagerFactory keyMgrFactory;

        try {
            keyMgrFactory = KeyManagerFactory.getInstance(keyAlgorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
        }

        KeyStore keyStore = loadKeyStore(keyStoreType, keyStoreFilePath, keyStorePwd);

        try {
            keyMgrFactory.init(keyStore, keyStorePwd);

            pendingHotReloadTime = hotReloadEnabled ? hotReloadTime(keyStore) : 0;

            return keyMgrFactory.getKeyManagers();
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Key Manager (security exception occurred) [type=" +
                keyStoreType + ", keyStorePath=" + keyStoreFilePath + ']', e);
        }
    }

    /**
     * @param store Loaded key store.
     * @return Hot reload time of the chain that expires first, or {@code 0} if the store has no private key
     *     with an X.509 certificate chain.
     * @throws GeneralSecurityException If the key store cannot be read.
     */
    private long hotReloadTime(KeyStore store) throws GeneralSecurityException {
        long earliestExpiry = Long.MAX_VALUE;

        for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
            String alias = aliases.nextElement();

            if (!store.isKeyEntry(alias))
                continue;

            Certificate[] chain = store.getCertificateChain(alias);

            if (chain != null && Arrays.stream(chain).allMatch(X509Certificate.class::isInstance)) {
                for (Certificate cert : chain)
                    earliestExpiry = Math.min(earliestExpiry, ((X509Certificate)cert).getNotAfter().getTime());
            }
        }

        return earliestExpiry == Long.MAX_VALUE ? 0 : earliestExpiry - hotReloadBeforeExpiryMs;
    }

    /** {@inheritDoc} */
    @Override protected final TrustManager[] createTrustManagers() throws SSLException {
        if (trustMgrs != null)
            return trustMgrs;

        KeyStore trustStore = loadKeyStore(trustStoreType, trustStoreFilePath, trustStorePwd);

        try {
            TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(keyAlgorithm);

            trustMgrFactory.init(trustStore);

            return trustMgrFactory.getTrustManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Trust Manager (security exception occurred) [type=" +
                trustStoreType + ", trustStorePath=" + trustStoreFilePath + ']', e);
        }
    }

    /**
     * Builds human-readable string with factory parameters.
     *
     * @return Parameters string.
     */
    private String parameters() {
        StringBuilder buf = new StringBuilder("[keyStoreType=").append(keyStoreType);

        buf.append(", proto=").append(proto).append(", keyStoreFile=").append(keyStoreFilePath);

        if (trustMgrs != null)
            buf.append(", trustMgrs=").append(Arrays.toString(trustMgrs));
        else
            buf.append(", trustStoreFile=").append(trustStoreFilePath);

        buf.append(']');

        return buf.toString();
    }

    /** {@inheritDoc} */
    @Override protected void checkParameters() throws SSLException {
        assert keyStoreType != null;
        assert proto != null;

        checkNullParameter(keyStoreFilePath, "keyStoreFilePath");
        checkNullParameter(keyStorePwd, "keyStorePwd");

        if (trustMgrs == null) {
            if (trustStoreFilePath == null)
                throw new SSLException("Failed to initialize SSL context (either trustStoreFilePath or " +
                    "trustManagers must be provided)");
            else
                checkNullParameter(trustStorePwd, "trustStorePwd");
        }
    }

    /**
     * By default, this method simply opens a raw file input stream. Subclasses may override this method
     * if some specific location should be handled (this may be a case for Android users).
     *
     * @param filePath Path to the file.
     * @return Opened input stream.
     * @throws IOException If stream could not be opened.
     */
    protected InputStream openFileInputStream(String filePath) throws IOException {
        return new FileInputStream(filePath);
    }

    /**
     * Loads key store with configured parameters.
     *
     * @param keyStoreType Type of key store.
     * @param storeFilePath Path to key store file.
     * @param keyStorePwd Store password.
     * @return Initialized key store.
     * @throws SSLException If key store could not be initialized.
     */
    protected KeyStore loadKeyStore(String keyStoreType, String storeFilePath, char[] keyStorePwd)
        throws SSLException {
        try {
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);

            try (InputStream input = openFileInputStream(storeFilePath)) {

                keyStore.load(input, keyStorePwd);

                return keyStore;
            }
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize key store (security exception occurred) [type=" +
                keyStoreType + ", keyStorePath=" + storeFilePath + ']', e);
        }
        catch (FileNotFoundException e) {
            throw new SSLException("Failed to initialize key store (key store file was not found): [path=" +
                storeFilePath + ", msg=" + e.getMessage() + ']');
        }
        catch (IOException e) {
            throw new SSLException("Failed to initialize key store (I/O error occurred): " + storeFilePath, e);
        }
    }

    /** Restores the default lead time for factories serialized before the setting existed. */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (hotReloadBeforeExpiryMs == 0)
            hotReloadBeforeExpiryMs = DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + parameters();
    }

    /**
     * Disabled trust manager, will skip all certificate checks.
     */
    private static class DisabledX509TrustManager implements X509TrustManager {
        /** Empty certificate array. */
        private static final X509Certificate[] CERTS = new X509Certificate[0];

        /** {@inheritDoc} */
        @Override public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            // No-op, all clients are trusted.
        }

        /** {@inheritDoc} */
        @Override public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            // No-op, all servers are trusted.
        }

        /** {@inheritDoc} */
        @Override public X509Certificate[] getAcceptedIssuers() {
            return CERTS;
        }
    }
}
