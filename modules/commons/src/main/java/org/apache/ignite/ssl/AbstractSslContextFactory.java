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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.ssl.ReloadableSslFactory;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Base class for SSL context factories that cache their {@link SSLContext}. {@link #init()} loads a new context, and
 * {@link #create()} returns the last one that it loaded, loading one on the first call. The automatic reload time comes
 * {@link #getAutoReloadBeforeExpiryMs()} before the earliest expiry in the key store that the factory passed to
 * {@link #createKeyManagers(KeyManagerFactory, KeyStore, char[])} during the last successful load.
 */
public abstract class AbstractSslContextFactory implements ReloadableSslFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Default lead time for automatic certificate reload: 7 days. */
    public static final long DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS = TimeUnit.DAYS.toMillis(7);

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Cached instance of an {@link SSLContext}. */
    protected final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();

    /** */
    private boolean autoReloadEnabled = true;

    /** */
    private volatile long autoReloadBeforeExpiryMs = DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS;

    /** */
    private transient volatile long earliestExpiry;

    /** */
    private transient long pendingExpiry;

    /** {@inheritDoc} */
    @Override public boolean isAutoReloadEnabled() {
        return autoReloadEnabled;
    }

    /**
     * @param enabled Whether automatic certificate reload is enabled. It is enabled by default. Changes require
     *     a node restart.
     */
    public void setAutoReloadEnabled(boolean enabled) {
        autoReloadEnabled = enabled;
    }

    /**
     * @return How long before the certificates expire automatic reload becomes due, in milliseconds.
     *     Defaults to {@link #DFLT_AUTO_RELOAD_BEFORE_EXPIRY_MS}.
     */
    public long getAutoReloadBeforeExpiryMs() {
        return autoReloadBeforeExpiryMs;
    }

    /**
     * @param millis How long before the certificates expire automatic reload becomes due, in milliseconds. Must be positive.
     */
    public void setAutoReloadBeforeExpiryMs(long millis) {
        A.ensure(millis > 0, "millis > 0");

        autoReloadBeforeExpiryMs = millis;
    }

    /** {@inheritDoc} */
    @Override public long autoReloadTime() {
        long expiry = earliestExpiry;

        return expiry == 0 ? 0 : expiry - autoReloadBeforeExpiryMs;
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
     * Creates key managers from the key store. For the automatic reload date, call it once per load with the key store that
     * supplies the key managers.
     *
     * @param keyMgrFactory Key manager factory.
     * @param store Key store with the private keys and their certificate chains.
     * @param pwd Password of the private keys.
     * @return Key managers of the key store.
     * @throws GeneralSecurityException If the key managers cannot be created.
     */
    protected final synchronized KeyManager[] createKeyManagers(KeyManagerFactory keyMgrFactory, KeyStore store, char[] pwd)
        throws GeneralSecurityException {
        keyMgrFactory.init(store, pwd);

        pendingExpiry = ReloadableSslFactory.keyChainsExpiry(store);

        return keyMgrFactory.getKeyManagers();
    }

    /**
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /** {@inheritDoc} */
    @Override public final synchronized void init() {
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
