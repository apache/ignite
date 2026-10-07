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

import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslCertificates.chainNotAfter;
import static org.apache.ignite.internal.ssl.SslCertificates.describe;

/** Owns the SSL context of one configured factory for every transport configured with it. */
public class SslContextProvider extends SslContextReloadable {
    /** */
    private final Factory<SSLContext> factory;

    /** */
    private final Set<String> transports = new ConcurrentSkipListSet<>();

    /** Context in use. */
    private volatile SSLContext ctx;

    /** Chain the context in use presents, {@code null} if it is unknown. */
    private volatile X509Certificate[] chain;

    /** Told about every new context once it is recorded. */
    private volatile Runnable reloadLsnr = () -> {};

    /** @param factory Factory to build the context with. */
    public SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();
        chain = SslCertificates.servedChain(ctx);
    }

    /** @return Context to open the next connection with. */
    public SSLContext context() {
        return ctx;
    }

    /** @param transport Transport the context is handed to. */
    public void addTransport(String transport) {
        transports.add(transport);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> transports() {
        return Collections.unmodifiableCollection(transports);
    }

    /** {@inheritDoc} */
    @Override public @Nullable X509Certificate[] servedChain() {
        return chain;
    }

    /** @param lsnr Told about every new context once it is recorded; must not block. */
    void onReload(Runnable lsnr) {
        reloadLsnr = lsnr;
    }

    /**
     * {@inheritDoc} The new context goes in use only if every certificate of its chain is valid now and, if nodes connect on it, this
     * node's own trust store accepts it.
     *
     * @throws IgniteCheckedException {@inheritDoc}
     */
    @Override protected void rebuild() throws IgniteCheckedException {
        SSLContext rebuilt = factory.create();

        if (rebuilt == ctx)
            throw new IgniteCheckedException("The SSL context factory hands back the context in use, there is nothing to read again");

        X509Certificate[] next = SslCertificates.servedChain(rebuilt);

        check(rebuilt, next);

        chain = next;
        ctx = rebuilt;
    }

    /** {@inheritDoc} */
    @Override void onReloaded() {
        super.onReloaded();

        reloadLsnr.run();
    }

    /**
     * Puts in use a context built anew, provided its chain expires later than the one in use. A renewal that does not move the expiry gains
     * nothing and would be due again at once. A context that presents the chain in use leaves nothing to renew yet and is not checked.
     *
     * @param expected Context the renewal was planned for.
     * @return What the renewal did.
     * @throws IgniteCheckedException If the new context presents another chain that fails the checks of {@link #rebuild()}, or does not
     *      expire later.
     */
    public synchronized Renewed renew(SSLContext expected) throws IgniteCheckedException {
        if (ctx != expected)
            return Renewed.SUPERSEDED;

        try {
            SSLContext rebuilt = factory.create();

            X509Certificate[] next = SslCertificates.servedChain(rebuilt);

            if (next == null)
                throw new IgniteCheckedException("Cannot tell which certificate the new SSL context presents");

            if (Arrays.equals(next, chain))
                return Renewed.UNCHANGED;

            check(rebuilt, next);

            if (chain != null && chainNotAfter(next) <= chainNotAfter(chain)) {
                throw new IgniteCheckedException("The new certificate chain expires no later than the one in use [" + describe(next[0]) +
                    ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(next)) +
                    ", inUseChainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(chain)) + ']');
            }

            chain = next;
            ctx = rebuilt;
        }
        catch (Throwable e) {
            onFailure(e);

            throw e;
        }

        onReloaded();

        return Renewed.RENEWED;
    }

    /**
     * @param rebuilt Context built anew.
     * @param next Chain it presents, {@code null} if it is unknown.
     * @throws IgniteCheckedException If a certificate it presents is not valid now, or nodes refuse it.
     */
    private void check(SSLContext rebuilt, @Nullable X509Certificate[] next) throws IgniteCheckedException {
        long now = System.currentTimeMillis();

        X509Certificate invalid = next == null ? null : SslCertificates.invalidAt(next, now);

        if (invalid != null) {
            throw new IgniteCheckedException("The new certificate chain is not valid now [" + describe(invalid) + ", now=" +
                Instant.ofEpochMilli(now) + ']');
        }

        if (transports.contains(COMMUNICATION) || transports.contains(DISCOVERY)) {
            try {
                SslCertificates.validateInterNode(rebuilt);
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("A handshake between nodes on the new certificate was refused, checked against this " +
                    "node's own trust store [" + describe(next == null ? null : next[0]) + ']', e);
            }
        }
    }

    /** What a renewal did. */
    public enum Renewed {
        /** Put a new certificate in use. */
        RENEWED,

        /** Nothing: another context was put in use since the renewal was planned. */
        SUPERSEDED,

        /** Nothing: the factory hands back the certificates in use, there is no newer one yet. */
        UNCHANGED
    }
}
