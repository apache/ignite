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

package org.apache.ignite.internal.ssl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Owns the SSL context built out of one configured factory, and hands it to everything that factory was given to.
 * <p>
 * A transport asks for the context whenever it opens a connection, so replacing the context here is what puts
 * rotated certificates in use: connections opened afterwards get the new one, established ones are not touched.
 * One provider stands for one factory, however many transports share it, so a rotation cannot leave them on
 * certificates read at different moments.
 */
public class SslContextProvider implements SslContextReloadable {
    /** Builds a context out of whatever the configured stores hold at the moment of the call. */
    private final Factory<SSLContext> factory;

    /** Transports served, sorted, as the reload command reports them. */
    private final Set<String> users = new ConcurrentSkipListSet<>();

    /** Whether any user connects nodes to each other, which is what makes a context worth checking before use. */
    private volatile boolean interNode;

    /** Context in use. */
    private volatile SSLContext ctx;

    /** What the context in use presents, worked out once per context; {@code null} until asked. */
    private volatile Served served;

    /** Authorities the context in use trusts, read when it was built. */
    private volatile List<X509Certificate> trusted;

    /** Outcome of the reloads. */
    private final SslReloadState state = new SslReloadState();

    /** Told about every context the reload command puts in use, {@code null} if nobody listens. */
    private volatile Runnable reloadLsnr;

    /**
     * @param factory Factory to build the context with.
     */
    public SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();

        trusted = readTrustedAuthorities();
    }

    /**
     * @return Context to open the next connection with.
     */
    public SSLContext context() {
        return ctx;
    }

    /**
     * @param user Transport the context is handed to.
     * @param interNode Whether that transport connects nodes to each other.
     */
    public void addUser(String user, boolean interNode) {
        users.add(user);

        if (interNode)
            this.interNode = true;
    }

    /**
     * @return Transports this provider serves.
     */
    public Collection<String> users() {
        return Collections.unmodifiableCollection(users);
    }

    /**
     * @param lsnr Told about every context {@link #reload()} puts in use. Called while the provider is locked, so it
     *      must only hand the news over.
     */
    public void onReload(Runnable lsnr) {
        reloadLsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean reload() throws IgniteCheckedException {
        SSLContext rebuilt = rebuild();

        if (rebuilt == null)
            return false;

        put(rebuilt, null);

        Runnable lsnr = reloadLsnr;

        if (lsnr != null)
            lsnr.run();

        return true;
    }

    /**
     * Puts in use a context built anew, provided its certificate expires later than the one in use: a renewal that
     * does not move the expiry gains nothing and would be due again at once.
     *
     * @throws IgniteCheckedException If the context could not be built, an inter-node transport would refuse it, or
     *      its certificate does not expire later than the one in use.
     */
    public synchronized void renew() throws IgniteCheckedException {
        SSLContext rebuilt = rebuild();

        if (rebuilt == null)
            throw new IgniteCheckedException("The factory handed back the SSL context already in use");

        X509Certificate[] next = SslContextValidator.servedChain(rebuilt);

        if (next == null)
            throw new IgniteCheckedException("Cannot tell which certificate the new SSL context presents");

        X509Certificate[] cur = servedChain();

        if (cur != null && SslCertificates.chainNotAfter(next) <= SslCertificates.chainNotAfter(cur)) {
            throw new IgniteCheckedException("The new certificate expires no later than the one in use [new: " +
                SslCertificates.describe(next[0]) + "; in use: " + SslCertificates.describe(cur[0]) + ']');
        }

        put(rebuilt, next);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean check() throws IgniteCheckedException {
        return rebuild() != null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable X509Certificate[] servedChain() {
        SSLContext ctx0 = ctx;

        Served served0 = served;

        // Metrics read this on every poll, so the handshake runs once per context rather than once per read.
        if (served0 == null || served0.ctx != ctx0)
            served = served0 = new Served(ctx0, SslContextValidator.servedChain(ctx0));

        return served0.chain;
    }

    /** {@inheritDoc} */
    @Override public List<X509Certificate> trustedAuthorities() {
        return trusted;
    }

    /** {@inheritDoc} */
    @Override public SslReloadState reloadState() {
        return state;
    }

    /**
     * @param rebuilt Context to put in use.
     * @param chain Chain it presents, {@code null} if not worked out yet.
     */
    private void put(SSLContext rebuilt, @Nullable X509Certificate[] chain) {
        ctx = rebuilt;

        if (chain != null)
            served = new Served(rebuilt, chain);

        trusted = readTrustedAuthorities();
    }

    /**
     * @return Context built from the stores as they are now, or {@code null} if the factory handed back the one
     *      already in use and there is therefore nothing to put in use.
     * @throws IgniteCheckedException If the context could not be built, or an inter-node transport would refuse it.
     */
    private @Nullable SSLContext rebuild() throws IgniteCheckedException {
        SSLContext rebuilt = factory.create();

        if (rebuilt == ctx)
            return null;

        if (interNode) {
            try {
                SslContextValidator.validateInterNode(rebuilt);
            }
            catch (SSLException e) {
                X509Certificate[] chain = SslContextValidator.servedChain(rebuilt);

                throw new IgniteCheckedException("A handshake between nodes on the new certificate was refused, " +
                    "checked against this node's own trust store [" +
                    SslCertificates.describe(chain == null ? null : chain[0]) + ']', e);
            }
        }

        return rebuilt;
    }

    /**
     * @return Authorities in the trust store the factory reads, or an empty list if the factory is not one that
     *      names a trust store file, or the file cannot be read.
     */
    private List<X509Certificate> readTrustedAuthorities() {
        if (!(factory instanceof SslContextFactory))
            return Collections.emptyList();

        SslContextFactory f = (SslContextFactory)factory;

        if (f.getTrustStoreFilePath() == null)
            return Collections.emptyList();

        try (InputStream in = new FileInputStream(f.getTrustStoreFilePath())) {
            KeyStore store = KeyStore.getInstance(f.getTrustStoreType());

            store.load(in, f.getTrustStorePassword());

            List<X509Certificate> res = new ArrayList<>();

            for (String alias : Collections.list(store.aliases())) {
                Certificate cert = store.getCertificate(alias);

                if (cert instanceof X509Certificate)
                    res.add((X509Certificate)cert);
            }

            return Collections.unmodifiableList(res);
        }
        catch (Exception ignored) {
            // Only shown to the operator; the context itself was built from the same file without trouble.
            return Collections.emptyList();
        }
    }

    /** Chain a context presents, kept together with the context it was worked out for. */
    private static class Served {
        /** Context the chain was worked out for. */
        private final SSLContext ctx;

        /** Chain the context presents, {@code null} if it cannot be told. */
        private final X509Certificate[] chain;

        /**
         * @param ctx Context the chain was worked out for.
         * @param chain Chain the context presents, {@code null} if it cannot be told.
         */
        private Served(SSLContext ctx, @Nullable X509Certificate[] chain) {
            this.ctx = ctx;
            this.chain = chain;
        }
    }
}
