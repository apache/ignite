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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslCertificates.chainNotAfter;
import static org.apache.ignite.internal.ssl.SslCertificates.describe;

/**
 * Owns the SSL context of one configured factory for every transport configured with it. Transports take the context on each new
 * connection, so replacing it here puts new certificates in use for new connections without touching established ones.
 */
public class SslContextProvider extends SslContextReloadable {
    /** */
    private final Factory<SSLContext> factory;

    /** */
    private final Set<String> transports = new ConcurrentSkipListSet<>();

    /** Context in use. */
    private volatile SSLContext ctx;

    /** Chain the context in use presents, {@code null} if it cannot be told. */
    private volatile X509Certificate[] chain;

    /** Told about every new context once it is recorded, {@code null} if nobody listens. */
    private volatile Runnable reloadLsnr;

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
    public void onReload(Runnable lsnr) {
        reloadLsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public synchronized void reload() throws IgniteCheckedException {
        SSLContext rebuilt = factory.create();

        chain = check(rebuilt);
        ctx = rebuilt;
    }

    /** {@inheritDoc} */
    @Override public String onReloaded(IgniteLogger log, String initiator) {
        String desc = super.onReloaded(log, initiator);

        Runnable lsnr = reloadLsnr;

        if (lsnr != null)
            lsnr.run();

        return desc;
    }

    /**
     * Puts in use a context built anew, provided its chain expires later than the one in use: a renewal that does not move the expiry gains
     * nothing and would be due again at once.
     *
     * @param expected Context the renewal was planned for.
     * @return {@code False} if another context was put in use since then, which leaves nothing to renew.
     * @throws IgniteCheckedException If the new context fails the checks of {@link #reload()}, or does not expire later.
     */
    public synchronized boolean renew(SSLContext expected) throws IgniteCheckedException {
        if (ctx != expected)
            return false;

        SSLContext rebuilt = factory.create();

        X509Certificate[] next = check(rebuilt);

        if (next == null)
            throw new IgniteCheckedException("Cannot tell which certificate the new SSL context presents");

        if (chain != null && chainNotAfter(next) <= chainNotAfter(chain)) {
            throw new IgniteCheckedException("The new certificate chain expires no later than the one in use [" + describe(next[0]) +
                ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(next)) +
                ", inUseChainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(chain)) + ']');
        }

        chain = next;
        ctx = rebuilt;

        return true;
    }

    /**
     * @param rebuilt Context built anew.
     * @return Chain it presents, {@code null} if it cannot be told.
     * @throws IgniteCheckedException If it is the context in use, a certificate it presents is not valid now, or nodes refuse it.
     */
    private @Nullable X509Certificate[] check(SSLContext rebuilt) throws IgniteCheckedException {
        if (rebuilt == ctx)
            throw new IgniteCheckedException("The SSL context factory hands back the context in use, there is nothing to read again");

        X509Certificate[] next = SslCertificates.servedChain(rebuilt);

        long now = System.currentTimeMillis();

        X509Certificate invalid = next == null ? null : SslCertificates.invalidAt(next, now);

        if (invalid != null) {
            throw new IgniteCheckedException("The new certificate chain is not valid now [" + describe(invalid) + ", now=" +
                Instant.ofEpochMilli(now) + ']');
        }

        // Clients may legitimately trust differently, so only a context between nodes is worth a handshake with itself.
        if (transports.contains(COMMUNICATION) || transports.contains(DISCOVERY)) {
            try {
                SslCertificates.validateInterNode(rebuilt);
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("A handshake between nodes on the new certificate was refused, checked against this " +
                    "node's own trust store [" + describe(next == null ? null : next[0]) + ']', e);
            }
        }

        return next;
    }
}
