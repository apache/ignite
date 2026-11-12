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
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
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

    /** Fraction of a certificate lifetime reserved for renewal. */
    private static final double RENEWAL_WINDOW_FRACTION = 0.15;

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
    private boolean renewalEnabled;

    /** */
    private transient volatile long renewalTime;

    /** Key store read by the load in progress. */
    private transient KeyStore loadedKeyStore;

    /**
     * @return Whether automatic certificate renewal is enabled. Defaults to {@code false}.
     */
    public boolean isRenewalEnabled() {
        return renewalEnabled;
    }

    /**
     * @param enabled Whether automatic certificate renewal is enabled. Changes require a node restart.
     */
    public void setRenewalEnabled(boolean enabled) {
        renewalEnabled = enabled;
    }

    /**
     * The renewal time is calculated from the certificates loaded with renewal enabled.
     * The chain that expires first determines the window, which starts with 15% of its lifetime remaining.
     * Its lifetime runs from the key certificate's {@code notBefore} to the earliest {@code notAfter} in its chain.
     * For chains with the same expiry, the longest lifetime is used.
     *
     * @return Start of the renewal window in milliseconds since the epoch, or {@code 0} if renewal is disabled
     *     or certificates have not yet been loaded with renewal enabled.
     */
    public long getRenewalTime() {
        return renewalEnabled ? renewalTime : 0;
    }

    /**
     * Replaces the current SSL context when the renewal window has started.
     * Does nothing when renewal is disabled, no renewal date has been loaded, or the window has not started.
     * <p>
     * Every certificate in the replacement's private-key chains must be valid now.
     * A failed replacement leaves the current context and renewal time unchanged.
     *
     * @throws IgniteInterruptedException If the thread is interrupted before a due replacement starts loading.
     * @throws IgniteException If the replacement cannot be loaded or accepted.
     */
    public void renew() {
        if (!renewalEnabled)
            return;

        synchronized (this) {
            if (renewalTime == 0 || renewalTime > System.currentTimeMillis())
                return;

            if (Thread.currentThread().isInterrupted())
                throw new IgniteInterruptedException("Certificate renewal was interrupted before loading.");

            init();
        }
    }

    /** {@inheritDoc} */
    @Override SSLContext createSslContext() throws SSLException {
        try {
            SSLContext ctx = super.createSslContext();
            boolean replacement = sslCtx.get() != null;

            renewalTime = renewalEnabled || replacement ? certificateRenewalTime(loadedKeyStore, replacement) : 0;

            return ctx;
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to read TLS material [type=" + keyStoreType + ", keyStorePath=" + keyStoreFilePath + ']', e);
        }
        finally {
            loadedKeyStore = null;
        }
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
        try {
            KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(keyAlgorithm);

            KeyStore keyStore = loadKeyStore(keyStoreType, keyStoreFilePath, keyStorePwd);

            loadedKeyStore = keyStore;

            keyMgrFactory.init(keyStore, keyStorePwd);

            return keyMgrFactory.getKeyManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + keyAlgorithm, e);
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Key Manager (security exception occurred) [type=" +
                keyStoreType + ", keyStorePath=" + keyStoreFilePath + ']', e);
        }
    }

    /**
     * Validates the key chains and calculates the renewal date.
     *
     * @param store Loaded key store.
     * @param replacement Whether the certificates replace an existing context.
     * @return Start of the renewal window, or {@code 0} if renewal is disabled.
     * @throws GeneralSecurityException If a key or certificate cannot be read.
     * @throws SSLException If the certificates cannot be accepted.
     */
    private long certificateRenewalTime(KeyStore store, boolean replacement) throws GeneralSecurityException, SSLException {
        long earliestExpiry = Long.MAX_VALUE;
        long selectedStart = Long.MAX_VALUE;
        Date now = new Date();

        for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
            String alias = aliases.nextElement();

            if (!store.isKeyEntry(alias) || !(store.getKey(alias, keyStorePwd) instanceof PrivateKey))
                continue;

            Certificate[] chain = store.getCertificateChain(alias);

            if (chain == null || chain.length == 0)
                throw new SSLException("Certificate chain is unavailable [alias=" + alias + "]. Remove the key or add its X.509 chain.");

            long chainExpiry = Long.MAX_VALUE;

            for (Certificate cert : chain) {
                if (!(cert instanceof X509Certificate x509))
                    throw new SSLException("Certificate chain is not X.509 [alias=" + alias + "]. Remove the key or add its X.509 chain.");

                if (replacement) {
                    try {
                        x509.checkValidity(now);
                    }
                    catch (CertificateException e) {
                        throw new SSLException("Certificate is not currently valid [alias=" + alias + ", subject=" +
                            x509.getSubjectX500Principal() + ']', e);
                    }
                }

                chainExpiry = Math.min(chainExpiry, x509.getNotAfter().getTime());
            }

            long chainStart = ((X509Certificate)chain[0]).getNotBefore().getTime();

            if (chainExpiry < earliestExpiry || chainExpiry == earliestExpiry && chainStart < selectedStart) {
                earliestExpiry = chainExpiry;
                selectedStart = chainStart;
            }
        }

        if (renewalEnabled && earliestExpiry == Long.MAX_VALUE)
            throw new SSLException("Certificate renewal requires a private key with an X.509 certificate chain.");

        if (replacement && earliestExpiry < System.currentTimeMillis())
            throw new SSLException("Replacement certificates expired while loading the SSL context [expiry=" + earliestExpiry + ']');

        return renewalEnabled ?
            earliestExpiry - (long)(Math.max(0, earliestExpiry - (double)selectedStart) * RENEWAL_WINDOW_FRACTION) : 0;
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
