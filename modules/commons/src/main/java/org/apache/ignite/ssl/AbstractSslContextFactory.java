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
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Owns the SSL context and its certificate metadata. {@link #init()} loads replacement material before publishing it;
 * {@link #create()} returns a stable context. Its newly created engines and newly obtained socket factories use the
 * accepted material; previously obtained engines, socket factories and sockets retain their original material.
 * Configure the factory before its first use. Reusing an initialized factory, including after a node restart, retains
 * its material until {@code init()} is called.
 */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Accepted context and the metadata loaded with it. Runtime state is not serialized with configuration. */
    private transient volatile SslBundle bundle;

    /** Stable context returned to consumers after the first successful initialization. */
    private transient volatile SSLContext sharedCtx;

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

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
     * Checks the factory parameters, loads its key material once, and builds a fresh context with the certificate chains from
     * that material. The factory does not retain the result.
     *
     * Override this method to load a candidate from another source. It must not change the material already in use.
     * Outside loading hooks, use {@link #sslBundle()} to inspect the accepted material.
     *
     * @return Fresh context and the certificate metadata available to this factory.
     * @throws IgniteException If the context could not be created.
     */
    public SslBundle createSslBundle() {
        try {
            checkParameters();

            KeyMaterial material = loadKeyMaterial();

            return new SslBundle(createSslContext(material.keyManagers()), material.certificateChains());
        }
        catch (SSLException e) {
            throw new IgniteException(e.getMessage() + " [factory=" + this + ']', e);
        }
    }

    /**
     * @param keyMgrs Key managers for the loaded material.
     * @return Initialized context.
     * @throws SSLException If trust managers or the context cannot be created.
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
     * Loads the key store and the settings used to read its keys. Called once per context, after the parameters are checked.
     * For a context without private keys, return material over an empty store initialized with {@code KeyStore.load(null, null)}.
     *
     * @return Loaded key material, never {@code null}.
     * @throws SSLException If the key material cannot be loaded.
     */
    protected abstract KeyMaterial loadKeyMaterial() throws SSLException;

    /**
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /**
     * Loads keys and trust settings and atomically replaces the accepted context and its metadata. A failed initialization
     * leaves the previous material in use. New engines and socket factories obtained through {@link #create()} use the
     * replacement; already opened connections keep their original material.
     *
     * @return Context and metadata accepted by this initialization.
     * @throws IgniteException If the material cannot be loaded or the context cannot be created.
     */
    public final synchronized SslBundle init() {
        SslBundle next = createSslBundle();

        next.context().createSSLEngine();

        SSLContext shared = sharedCtx;

        if (shared == null)
            shared = new CurrentSslContext(next.context(), () -> bundle.context());

        bundle = next;
        sharedCtx = shared;

        return next;
    }

    /**
     * Returns the accepted context and its metadata, initializing the factory on first use.
     *
     * @return One snapshot of the material in use.
     * @throws IgniteException If the first initialization fails.
     */
    public final SslBundle sslBundle() {
        create();

        return bundle;
    }

    /**
     * Returns the same forwarding context on every call, initializing it on first use. Obtain new engines and socket
     * factories from this context to use the material accepted by the last successful {@link #init()}. Repeated calls
     * do not read the key or trust stores.
     *
     * @return Stable initialized context.
     * @throws IgniteException If the first initialization fails.
     */
    @Override public final SSLContext create() {
        if (sharedCtx == null) {
            synchronized (this) {
                if (sharedCtx == null)
                    init();
            }
        }

        return sharedCtx;
    }
}
