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
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Represents abstract implementation of SSL Context Factory that caches the result of the first successful
 * attempt to create an {@link SSLContext} and always returns it as a result of further invocations of the
 * {@link AbstractSslContextFactory#create()}} method.
 */
public abstract class AbstractSslContextFactory implements Factory<SSLContext> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default SSL protocol. */
    public static final String DFLT_SSL_PROTOCOL = "TLS";

    /** Default share of the certificate lifetime left when a node starts to renew it. */
    public static final double DFLT_RENEW_BEFORE_FRACTION = 0.15;

    /** SSL protocol. */
    protected String proto = DFLT_SSL_PROTOCOL;

    /** Enabled cipher suites. */
    protected String[] cipherSuites;

    /** Enabled protocols. */
    protected String[] protocols;

    /** Cached instance of an {@link SSLContext}. */
    protected final AtomicReference<SSLContext> sslCtx = new AtomicReference<>();

    /** Whether a node builds its SSL context again before the certificate expires, to pick up a new certificate. */
    private boolean renewalEnabled;

    /** Share of the certificate lifetime left when a node starts to renew it. */
    private double renewBeforeFraction = DFLT_RENEW_BEFORE_FRACTION;

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

    /** @return Whether a node builds its SSL context again before the certificate expires, to pick up a new certificate. */
    public boolean isRenewalEnabled() {
        return renewalEnabled;
    }

    /**
     * Sets whether a node builds its SSL context again before the certificate expires. This lets the node pick up a new certificate
     * from wherever the factory takes certificates. A file-based factory reads its files again, so replace the files yourself.
     * If the factory provides no certificate chain metadata, the node does not start. Disabled by default.
     * Set this before starting the node; changing it on a running node takes effect after a restart.
     *
     * @param renewalEnabled Whether renewal is enabled.
     */
    public void setRenewalEnabled(boolean renewalEnabled) {
        this.renewalEnabled = renewalEnabled;
    }

    /** @return Share of the certificate lifetime left when a node starts to renew the certificate. */
    public double getRenewBeforeFraction() {
        return renewBeforeFraction;
    }

    /**
     * Sets the share of the certificate lifetime left when a node starts to renew the certificate. The lifetime runs from the time
     * the certificate becomes valid to the earliest expiry in its chain. The value must be greater than {@code 0} and less than
     * {@code 1}, otherwise this method throws {@link IllegalArgumentException}.
     * If not specified, {@link #DFLT_RENEW_BEFORE_FRACTION} is used.
     * Set this before starting the node; changing it on a running node takes effect after a restart.
     *
     * @param renewBeforeFraction Share of the lifetime.
     */
    public void setRenewBeforeFraction(double renewBeforeFraction) {
        A.ensure(renewBeforeFraction > 0 && renewBeforeFraction < 1, "renewBeforeFraction must be greater than 0 and less than 1");

        this.renewBeforeFraction = renewBeforeFraction;
    }

    /**
     * Creates SSL context based on factory settings.
     *
     * @return Initialized SSL context.
     * @throws SSLException If SSL context could not be created.
     */
    private SSLContext createSslContext() throws SSLException {
        checkParameters();

        KeyMaterial material = createKeyMaterial();

        TrustManager[] trustMgrs = createTrustManagers();

        try {
            SSLContext ctx = SSLContext.getInstance(proto);

            SSLParameters sslParameters = null;

            if (cipherSuites != null || protocols != null) {
                sslParameters = new SSLParameters();

                if (cipherSuites != null)
                    sslParameters.setCipherSuites(cipherSuites);

                if (protocols != null)
                    sslParameters.setProtocols(protocols);
            }

            if (sslParameters != null || material.chain != null || material.privateKeyMissing)
                ctx = new SSLContextWrapper(ctx, sslParameters, material.chain, material.privateKeyMissing);

            ctx.init(material.keyMgrs, trustMgrs, null);

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
     * Creates key managers and their certificate chain from the same load of the key material. The default calls
     * {@link #createKeyManagers()} and leaves the chain unknown. Subclasses that support automatic certificate renewal should
     * override this method and supply the chain without loading the material again.
     *
     * @return Key managers and their chain, if known.
     * @throws SSLException If the key material could not be loaded.
     */
    protected KeyMaterial createKeyMaterial() throws SSLException {
        return new KeyMaterial(createKeyManagers(), null);
    }

    /**
     * @return Created Trust Managers.
     * @throws SSLException If Trust Managers could not be created.
     */
    protected abstract TrustManager[] createTrustManagers() throws SSLException;

    /** {@inheritDoc} */
    @Override public SSLContext create() {
        SSLContext ctx = sslCtx.get();

        if (ctx == null) {
            try {
                ctx = createSslContext();

                if (!sslCtx.compareAndSet(null, ctx))
                    ctx = sslCtx.get();
            }
            catch (SSLException e) {
                throw new IgniteException(e);
            }
        }

        return ctx;
    }

    /**
     * Builds a new SSL context, unlike {@link #create()}, which keeps returning the first one. The node calls this method on every
     * {@code control.sh --ssl reload} and on every attempt to renew the certificate automatically. A subclass that builds its
     * context in its own {@link #create()} must override this method too and build the context from the same sources. Otherwise,
     * this method builds the context without the code in that {@link #create()}.
     * <p>
     * To report certificate dates and support automatic renewal, both methods must return the loaded chain with the context,
     * using {@link SSLContextWrapper#SSLContextWrapper(SSLContext, SSLParameters, X509Certificate[])}. A subclass that only changes
     * the certificate source can override {@link #createKeyMaterial()} instead and keep both methods unchanged.
     * <p>
     * Reload and renewal calls to the same factory on one node never overlap. Calls to different factories may run at the same
     * time. A subclass that fetches certificates from a remote source must limit how long a call takes and stop when its thread is
     * interrupted. When the node stops, it interrupts an automatic renewal in progress and waits for it to end.
     *
     * @return SSL context.
     */
    public SSLContext createNew() {
        try {
            return createSslContext();
        }
        catch (SSLException e) {
            throw new IgniteException(e);
        }
    }

    /** Key managers and the certificate chain loaded with them. */
    protected static final class KeyMaterial {
        /** */
        private final KeyManager[] keyMgrs;

        /** */
        private final X509Certificate[] chain;

        /** Whether the loaded store is known to contain no private key. */
        private final boolean privateKeyMissing;

        /**
         * @param keyMgrs Key managers, or {@code null} to use the provider's defaults.
         * @param chain Chain of the single private key, own certificate first, or {@code null} if unknown.
         */
        public KeyMaterial(@Nullable KeyManager[] keyMgrs, @Nullable X509Certificate[] chain) {
            this(keyMgrs, chain, false);
        }

        /**
         * @param keyMgrs Key managers.
         * @param chain Certificate chain, if known.
         * @param privateKeyMissing Whether the loaded store contains no private key.
         */
        private KeyMaterial(@Nullable KeyManager[] keyMgrs, @Nullable X509Certificate[] chain, boolean privateKeyMissing) {
            this.keyMgrs = keyMgrs == null ? null : keyMgrs.clone();
            this.chain = chain == null || chain.length == 0 ? null : chain.clone();
            this.privateKeyMissing = privateKeyMissing;
        }

        /** @return Key managers, or {@code null} to use the provider's defaults. */
        public @Nullable KeyManager[] keyManagers() {
            return keyMgrs == null ? null : keyMgrs.clone();
        }

        /**
         * Takes the chain from the store used to create the key managers. Stores with no private key, more than one private key,
         * or a chain containing a non-X.509 certificate leave the chain unknown. A store without a private key is also marked
         * so that a node can refuse it during reload, without preventing its use by a client that needs no client certificate.
         *
         * @param keyMgrs Key managers initialized from the store.
         * @param store Already loaded key store.
         * @return Key managers and the chain of the single private key, if known.
         * @throws KeyStoreException If the store cannot be read.
         */
        public static KeyMaterial fromKeyStore(KeyManager[] keyMgrs, KeyStore store) throws KeyStoreException {
            String keyAlias = null;

            for (Enumeration<String> aliases = store.aliases(); aliases.hasMoreElements(); ) {
                String alias = aliases.nextElement();

                if (!store.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                    continue;

                if (keyAlias != null)
                    return new KeyMaterial(keyMgrs, null);

                keyAlias = alias;
            }

            if (keyAlias == null)
                return new KeyMaterial(keyMgrs, null, true);

            Certificate[] certs = store.getCertificateChain(keyAlias);

            if (certs == null || certs.length == 0)
                return new KeyMaterial(keyMgrs, null);

            X509Certificate[] chain = new X509Certificate[certs.length];

            for (int i = 0; i < certs.length; i++) {
                if (!(certs[i] instanceof X509Certificate))
                    return new KeyMaterial(keyMgrs, null);

                chain[i] = (X509Certificate)certs[i];
            }

            return new KeyMaterial(keyMgrs, chain);
        }
    }
}
