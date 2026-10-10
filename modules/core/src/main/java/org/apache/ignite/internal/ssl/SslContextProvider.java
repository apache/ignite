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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslCertificates.chainNotAfter;
import static org.apache.ignite.internal.ssl.SslCertificates.describe;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;

/** Owns the SSL context of one configured factory for every transport configured with it, together with the outcome of its reloads. */
public class SslContextProvider {
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

    /** Time of the last successful reload, {@code 0} if there was none. */
    private volatile long lastSuccessTime;

    /** Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    private volatile long lastFailureTime;

    /** Reason of the last failed reload since the last successful one. */
    private volatile String lastFailureReason;

    /** Failed reloads in a row since the last successful one. */
    private volatile int failures;

    /** Time of the next automatic renewal, {@code 0} if none is planned. */
    private volatile long nextRenewalTime;

    /** @param factory Factory to build the context with. */
    SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();
        chain = SslCertificates.servedChain(ctx);
    }

    /** @return Factory the context is built with. */
    Factory<SSLContext> factory() {
        return factory;
    }

    /** @return Context to open the next connection with. */
    SSLContext context() {
        return ctx;
    }

    /** @param transport Transport the context is handed to. */
    void addTransport(String transport) {
        transports.add(transport);
    }

    /** @return Transports served, comma-separated, as the commands and the node log name them. */
    public String transports() {
        return String.join(", ", transports);
    }

    /** @return Chain presented on new connections, own certificate first, or {@code null} if it is unknown. */
    public @Nullable X509Certificate[] servedChain() {
        return chain;
    }

    /** @return Certificate presented on new connections, or {@code null} if it is unknown. */
    public @Nullable X509Certificate servedCertificate() {
        X509Certificate[] chain = servedChain();

        return chain == null ? null : chain[0];
    }

    /** @param lsnr Told about every new context once it is recorded; must not block. */
    void onReload(Runnable lsnr) {
        reloadLsnr = lsnr;
    }

    /**
     * Builds the certificates the configuration points at now, puts them in use for new connections and records the outcome. The new
     * context goes in use only if it presents a certificate when the one in use does, every certificate of its chain is valid now and,
     * if nodes connect on it, this node's own trust store accepts it.
     *
     * @throws IgniteCheckedException If they cannot be built or would be refused, or there is nothing to read again. The ones in use stay.
     */
    public synchronized void reload() throws IgniteCheckedException {
        try {
            SSLContext rebuilt = factory.create();

            if (rebuilt == ctx)
                throw new IgniteCheckedException("The SSL context factory hands back the context in use, there is nothing to read again");

            X509Certificate[] next = SslCertificates.servedChain(rebuilt);

            check(rebuilt, next);

            chain = next;
            ctx = rebuilt;
        }
        catch (Throwable e) {
            onFailure(e);

            throw e;
        }

        onSuccess();
    }

    /**
     * Puts in use a context built anew, provided its chain expires later than the one in use. A renewal that does not move the expiry gains
     * nothing and would be due again at once. A context that presents the chain in use leaves nothing to renew yet and is not checked.
     *
     * @param expected Context the renewal was planned for.
     * @return What the renewal did.
     * @throws IgniteCheckedException If the new context presents another chain that fails the checks of {@link #reload()}, or does not
     *      expire later.
     */
    synchronized RenewalResult renew(SSLContext expected) throws IgniteCheckedException {
        if (ctx != expected)
            return RenewalResult.SUPERSEDED;

        try {
            SSLContext rebuilt = factory.create();

            X509Certificate[] next = SslCertificates.servedChain(rebuilt);

            if (Arrays.equals(next, chain))
                return RenewalResult.UNCHANGED;

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

        onSuccess();

        return RenewalResult.RENEWED;
    }

    /**
     * @param rebuilt Context built anew.
     * @param next Chain it presents, {@code null} if it is unknown.
     * @throws IgniteCheckedException If it presents no certificate while the context in use does, a certificate it presents is not valid
     *      now, or nodes refuse it.
     */
    private void check(SSLContext rebuilt, @Nullable X509Certificate[] next) throws IgniteCheckedException {
        if (next == null && chain != null) {
            throw new IgniteCheckedException("The new SSL context presents no certificate to a client, while the one in use does; " +
                "for example, its key store has no private key");
        }

        long now = System.currentTimeMillis();

        X509Certificate invalid = next == null ? null : SslCertificates.invalidAt(next, now);

        if (invalid != null) {
            throw new IgniteCheckedException("The new certificate chain is not valid now [" + describe(invalid) + ", now=" +
                Instant.ofEpochMilli(now) + ']');
        }

        if (transports.contains(COMMUNICATION) || transports.contains(DISCOVERY)) {
            try {
                SslCertificates.checkInterNodeHandshake(rebuilt);
            }
            catch (SSLException e) {
                throw new IgniteCheckedException("A handshake between nodes on the new certificate was refused, checked against this " +
                    "node's own trust store [" + describe(next == null ? null : next[0]) + ']', e);
            }
        }
    }

    /** Records a successful reload. */
    private void onSuccess() {
        lastSuccessTime = System.currentTimeMillis();
        lastFailureTime = 0;
        lastFailureReason = null;
        failures = 0;

        reloadLsnr.run();
    }

    /** @param e Why the reload failed. */
    private void onFailure(Throwable e) {
        lastFailureTime = System.currentTimeMillis();
        lastFailureReason = SslCertificates.reason(e);
        failures++;
    }

    /** @return Time of the last successful reload, {@code 0} if there was none. */
    public long lastSuccessTime() {
        return lastSuccessTime;
    }

    /** @return Time of the last failed reload since the last successful one, {@code 0} if there was none. */
    public long lastFailureTime() {
        return lastFailureTime;
    }

    /** @return Reason of the last failed reload since the last successful one, {@code null} if there was none. */
    public @Nullable String lastFailureReason() {
        return lastFailureReason;
    }

    /** @return Failed reloads in a row since the last successful one. */
    public int failures() {
        return failures;
    }

    /** @return Time of the next automatic renewal, {@code 0} if none is planned. */
    public long nextRenewalTime() {
        return nextRenewalTime;
    }

    /** @param nextRenewalTime Time of the next automatic renewal, {@code 0} if none is planned. */
    void nextRenewalTime(long nextRenewalTime) {
        this.nextRenewalTime = nextRenewalTime;
    }

    /** What a renewal did. */
    enum RenewalResult {
        /** Put a new certificate in use. */
        RENEWED,

        /** Nothing: another context was put in use since the renewal was planned. */
        SUPERSEDED,

        /** Nothing: the factory hands back the certificates in use, there is no newer one yet. */
        UNCHANGED
    }
}
