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

import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Base class for SSL context factories that cache their {@link SSLContext}.
 */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Default lead time for certificate hot reload: 7 days. */
    public static final long DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS = TimeUnit.DAYS.toMillis(7);

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Cached instance of an {@link SSLContext}. */
    protected final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();

    /** */
    private boolean hotReloadEnabled;

    /** */
    private volatile long hotReloadBeforeExpiryMs = DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;

    /** */
    private transient volatile long earliestExpiry;

    /** */
    private transient long pendingExpiry;

    /**
     * Automatic hot reload takes its date from the key store that the factory passes to
     * {@link #createKeyManagers(KeyManagerFactory, KeyStore, char[])}.
     *
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
    public final long getHotReloadBeforeExpiryMs() {
        return hotReloadBeforeExpiryMs;
    }

    /**
     * @param millis How long before the certificates expire hot reload becomes due, in milliseconds. Must be positive.
     */
    public void setHotReloadBeforeExpiryMs(long millis) {
        A.ensure(millis > 0, "millis > 0");

        hotReloadBeforeExpiryMs = millis;
    }

    /**
     * The hot reload time is calculated from the key store passed to
     * {@link #createKeyManagers(KeyManagerFactory, KeyStore, char[])} during the last successful load.
     * It comes {@link #getHotReloadBeforeExpiryMs()} before the earliest expiry among the chains of the private keys.
     * A chain expires when any certificate in it expires.
     * Keys without an X.509 certificate chain are ignored, as TLS cannot use them.
     *
     * @return Hot reload time in milliseconds since the epoch, or {@code 0} if the certificates have not been loaded yet
     *     or the last successful load supplied no key store or no private key with an X.509 chain.
     */
    public final long getHotReloadTime() {
        long expiry = earliestExpiry;

        return expiry == 0 ? 0 : expiry - hotReloadBeforeExpiryMs;
    }

    /**
     * Gets protocol for secure transport.
     *
     * @return SSL protocol name.
     */
    public String getProtocol() {
        return proto;
    }

    /**
     * Sets protocol for secure transport. If not specified, {@link #DFLT_SSL_PROTOCOL} will be used.
     *
     * @param proto SSL protocol name.
     */
    public void setProtocol(String proto) {
        A.notNull(proto, "proto");

        this.proto = proto;
    }

    /**
     * Sets enabled cipher suites.
     *
     * @param cipherSuites enabled cipher suites.
     */
    public void setCipherSuites(String... cipherSuites) {
        this.cipherSuites = cipherSuites;
    }

    /**
     * Gets enabled cipher suites.
     *
     * @return enabled cipher suites
     */
    public String[] getCipherSuites() {
        return cipherSuites;
    }

    /**
     * Gets enabled protocols.
     *
     * @return Enabled protocols.
     */
    public String[] getProtocols() {
        return protocols;
    }

    /**
     * Sets enabled protocols.
     *
     * @param protocols Enabled protocols.
     */
    public void setProtocols(String... protocols) {
        this.protocols = protocols;
    }

    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    private SSLContext createSslContext() throws SSLException {
        checkParameters();

        pendingExpiry = 0;

        KeyManager[] keyMgrs = createKeyManagers();

        TrustManager[] trustMgrs = createTrustManagers();

        try {
            SSLContext ctx = SSLContext.getInstance(proto);

            if (cipherSuites != null || protocols != null) {
                SSLParameters sslParameters = new SSLParameters();

                if (cipherSuites != null)
                    sslParameters.setCipherSuites(cipherSuites);

                if (protocols != null)
                    sslParameters.setProtocols(protocols);

                ctx = new SSLContextWrapper(ctx, sslParameters);
            }

            ctx.init(keyMgrs, trustMgrs, null);

            earliestExpiry = pendingExpiry;

            return ctx;
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported SSL protocol: " + proto, e);
        }
        catch (KeyManagementException e) {
            throw new SSLException("Failed to initialized SSL context.", e);
        }
    }

    /**
     * @param param Value.
     * @param name Name.
     * @throws SSLException If {@code null}.
     */
    protected void checkNullParameter(Object param, String name) throws SSLException {
        if (param == null)
            throw new SSLException("Failed to initialize SSL context (parameter cannot be null): " + name);
    }

    /**
     * Checks that all required parameters are set.
     *
     * @throws SSLException If any of required parameters is missing.
     */
    protected abstract void checkParameters() throws SSLException;

    /**
     * @return Created Key Managers.
     * @throws SSLException If Key Managers could not be created.
     */
    protected abstract KeyManager[] createKeyManagers() throws SSLException;

    /**
     * Creates key managers from the key store. For the hot reload date, call it once per load with the key store that
     * supplies the key managers.
     *
     * @param keyMgrFactory Key manager factory.
     * @param store Key store with the private keys and their certificate chains.
     * @param pwd Password of the private keys.
     * @return Key managers of the key store.
     * @throws GeneralSecurityException If the key managers cannot be created.
     */
    protected final KeyManager[] createKeyManagers(KeyManagerFactory keyMgrFactory, KeyStore store, char[] pwd)
        throws GeneralSecurityException {
        keyMgrFactory.init(store, pwd);

        pendingExpiry = keyChainsExpiry(store);

        return keyMgrFactory.getKeyManagers();
    }

    /**
     * @param store Loaded key store.
     * @return Earliest certificate expiry in the private key chains, or {@code 0} if the store has no private key
     *     with an X.509 certificate chain.
     * @throws GeneralSecurityException If the key store cannot be read.
     */
    private static long keyChainsExpiry(KeyStore store) throws GeneralSecurityException {
        long earliest = Long.MAX_VALUE;

        for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
            String alias = aliases.nextElement();

            if (!store.isKeyEntry(alias))
                continue;

            earliest = Math.min(earliest, chainExpiry(store.getCertificateChain(alias)));
        }

        return earliest == Long.MAX_VALUE ? 0 : earliest;
    }

    /**
     * @param chain Certificate chain of a key entry, or {@code null} if the entry has none.
     * @return Earliest expiry in the chain, or {@link Long#MAX_VALUE} if there is no chain or it holds a certificate other
     *     than X.509, as TLS ignores such keys.
     */
    private static long chainExpiry(Certificate[] chain) {
        long expiry = Long.MAX_VALUE;

        if (chain == null)
            return expiry;

        for (Certificate cert : chain) {
            if (!(cert instanceof X509Certificate x509))
                return Long.MAX_VALUE;

            expiry = Math.min(expiry, x509.getNotAfter().getTime());
        }

        return expiry;
    }

    /**
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /**
     * Loads a new SSL context and caches it for the base implementation of {@link #create()}.
     * If loading fails, the previously cached context is kept.
     *
     * @throws IgniteException If the context cannot be initialized.
     */
    public final synchronized void init() {
        try {
            sslCtx.set(createSslContext());
        }
        catch (SSLException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public SSLContext create() {
        SSLContext ctx = sslCtx.get();

        if (ctx != null)
            return ctx;

        synchronized (this) {
            if (sslCtx.get() == null)
                init();

            return sslCtx.get();
        }
    }
}
