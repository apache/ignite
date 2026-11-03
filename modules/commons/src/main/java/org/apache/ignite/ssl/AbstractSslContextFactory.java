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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the SSL context and its certificate metadata. {@link #init()} loads replacement material before publishing it;
 * {@link #create()} returns the accepted context. Obtain it again after initialization to use the replacement;
 * previously obtained contexts, engines, socket factories and sockets retain their original material.
 * Configure the factory before its first use. Reusing an initialized factory, including after a node restart, retains
 * its material until {@code init()} succeeds.
 */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Accepted context and the metadata loaded with it. Runtime state is not serialized with configuration. */
    private transient volatile SslBundle bundle;

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
     * Loads a fresh context and matching certificate metadata from this factory's source. Check source parameters before
     * loading it. The result must not change material already in use. Outside loading hooks, use {@link #sslBundle()} to
     * inspect the accepted material.
     *
     * @return Fresh context and the certificate metadata available to this factory.
     * @throws SSLException If the context could not be created.
     */
    protected abstract SslBundle createSslBundle() throws SSLException;

    /**
     * Creates a context and certificate metadata from one loaded key store, using this factory's trust and TLS settings.
     * Do not modify the store while the context is in use: key managers such as PKIX read it during handshakes.
     * Load replacement material into a separate store.
     *
     * @param store Loaded key store, never {@code null}. For a context without private keys, use an empty store initialized
     *      with {@code KeyStore.load(null, null)}.
     * @param keyPwd Password for the loaded store's private keys, or {@code null} if no password is required.
     * @param algorithm Key manager algorithm.
     * @return Context and nonempty X.509 chains of key entries, ordered by alias. An empty map means no such chains were found.
     * @throws SSLException If the store cannot be read or the context cannot be created.
     */
    protected final SslBundle createSslBundle(KeyStore store, @Nullable char[] keyPwd, String algorithm) throws SSLException {
        A.notNull(store, "store", algorithm, "algorithm");

        KeyManager[] keyMgrs;

        try {
            KeyManagerFactory factory = KeyManagerFactory.getInstance(algorithm);

            factory.init(store, keyPwd);

            keyMgrs = factory.getKeyManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SSLException("Unsupported key algorithm: " + algorithm, e);
        }
        catch (GeneralSecurityException e) {
            throw new SSLException("Failed to initialize Key Manager (security exception occurred)", e);
        }

        Map<String, List<X509Certificate>> chains = certificateChains(store);

        return new SslBundle(createSslContext(keyMgrs), chains);
    }

    /** @return Fresh candidate with factory context added to checked SSL errors. */
    private SslBundle prepareSslBundle() {
        try {
            return createSslBundle();
        }
        catch (SSLException e) {
            throw new IgniteException(e.getMessage() + " [factory=" + this + ']', e);
        }
    }

    /**
     * @param store Loaded key store.
     * @return Nonempty X.509 chains of key entries; entries without such a chain are omitted.
     * @throws SSLException If the store cannot be read.
     */
    private static Map<String, List<X509Certificate>> certificateChains(KeyStore store) throws SSLException {
        try {
            Map<String, List<X509Certificate>> chains = new TreeMap<>();

            for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
                String alias = aliases.nextElement();

                if (!store.isKeyEntry(alias))
                    continue;

                List<X509Certificate> chain = certificateChain(store.getCertificateChain(alias));

                if (!chain.isEmpty())
                    chains.put(alias, chain);
            }

            return chains;
        }
        catch (KeyStoreException e) {
            throw new SSLException("Failed to read certificate chains from key store", e);
        }
    }

    /**
     * @param certs Chain from the store, if present.
     * @return X.509 chain, or an empty list if it is absent or contains another certificate type.
     */
    private static List<X509Certificate> certificateChain(@Nullable Certificate[] certs) {
        if (certs == null)
            return Collections.emptyList();

        List<X509Certificate> chain = new ArrayList<>(certs.length);

        for (Certificate cert : certs) {
            if (!(cert instanceof X509Certificate))
                return Collections.emptyList();

            chain.add((X509Certificate)cert);
        }

        return chain;
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
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /**
     * Loads keys and trust settings and atomically replaces the accepted context and its metadata. A failed initialization
     * leaves the previous material in use. Call {@link #create()} again to obtain the replacement; previously obtained
     * contexts and their connections keep their original material.
     *
     * @return Context and metadata accepted by this initialization.
     * @throws IgniteException If the material cannot be loaded or the context cannot be created.
     * @throws IllegalArgumentException If a configured protocol or cipher suite is unsupported.
     */
    public final synchronized SslBundle init() {
        SslBundle next = prepareSslBundle();

        next.context().createSSLEngine();

        bundle = next;

        return next;
    }

    /**
     * Returns the accepted context and its metadata, initializing the factory on first use.
     *
     * @return One snapshot of the material in use.
     * @throws IgniteException If the first initialization fails.
     */
    public final SslBundle sslBundle() {
        SslBundle cur = bundle;

        if (cur == null) {
            synchronized (this) {
                cur = bundle;

                if (cur == null)
                    cur = init();
            }
        }

        return cur;
    }

    /**
     * Returns the accepted context, initializing it on first use. Repeated calls do not read the key or trust stores.
     * After a successful {@link #init()}, call this method again to obtain the replacement. Previously returned contexts
     * retain their original material. Do not reinitialize the returned context directly.
     *
     * @return Currently accepted context.
     * @throws IgniteException If the first initialization fails.
     */
    @Override public final SSLContext create() {
        return sslBundle().context();
    }
}
