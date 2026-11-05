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

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.ssl.SslCertificateValidity;
import org.apache.ignite.internal.ssl.SslFactoryAccess;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for SSL context factories that cache their {@link SSLContext}.
 */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Internal access for the node's TLS reload command. */
    public static final SslFactoryAccess NODE_ACCESS = new SslFactoryAccess() {
        /** {@inheritDoc} */
        @Override protected @Nullable SSLContext context(AbstractSslContextFactory factory) {
            return factory.sslCtx.get();
        }

        /** {@inheritDoc} */
        @Override protected @Nullable Map<String, SslCertificateValidity> certificates(AbstractSslContextFactory factory) {
            synchronized (factory) {
                ContextCertificates certs = factory.certs;

                return certs != null && certs.ctx() == factory.sslCtx.get() ? certs.validity() : null;
            }
        }

        /** {@inheritDoc} */
        @Override protected void reload(AbstractSslContextFactory factory) throws SSLException {
            factory.load(true, true);
        }
    };

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Cached instance of an {@link SSLContext}. */
    protected final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();

    /** Certificate dates from the key store used to build the cached context. */
    @Nullable private transient ContextCertificates certs;

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
     * Creates an SSL context from the configured trust material and the loaded keys.
     *
     * @param keyMgrs Key managers for this context.
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    private SSLContext createSslContext(KeyManager[] keyMgrs) throws SSLException {
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
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /**
     * Loads a new SSL context and caches it for the base implementation of {@link #create()}.
     * When replacing a cached context, checks stream TLS support and any available certificate chains.
     * If loading fails, the previously cached context is kept.
     *
     * @throws IgniteException If the context cannot be initialized.
     */
    protected final void init() {
        try {
            load(true, false);
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

        try {
            return load(false, false);
        }
        catch (SSLException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param replace Whether to replace a context already in the cache.
     * @param reload Whether to validate the replacement for reload on a running node.
     * @return Context in the cache after loading.
     * @throws SSLException If loading or validation fails.
     */
    private synchronized SSLContext load(boolean replace, boolean reload) throws SSLException {
        SSLContext cur = sslCtx.get();

        if (!replace && cur != null)
            return cur;

        boolean validate = reload || cur != null;

        checkParameters();

        Map<String, SslCertificateValidity> validity = null;
        KeyManager[] keyMgrs;

        if (this instanceof SslContextFactory fileFactory) {
            validity = new TreeMap<>();
            keyMgrs = fileFactory.loadKeyManagers(validity, validate);
        }
        else
            keyMgrs = createKeyManagers();

        SSLContext candidate = createSslContext(keyMgrs);

        if (validate) {
            SslFactoryAccess.checkContext(candidate);

            if (validity != null) {
                if (validity.isEmpty())
                    throw new SSLException("The TLS key store has no X.509 certificate chains");

                long now = System.currentTimeMillis();

                for (Map.Entry<String, SslCertificateValidity> entry : validity.entrySet()) {
                    if (!entry.getValue().validAt(now))
                        throw new SSLException("TLS certificate chain is not valid now [alias=" + entry.getKey() + ']');
                }
            }
        }

        if (replace)
            sslCtx.set(candidate);
        else if (!sslCtx.compareAndSet(null, candidate))
            return sslCtx.get();

        certs = validity == null ? null : new ContextCertificates(candidate, Collections.unmodifiableMap(validity));

        return candidate;
    }

    /** Associates certificate dates with the context built from those keys. */
    private record ContextCertificates(SSLContext ctx, Map<String, SslCertificateValidity> validity) {
        // No-op.
    }
}
