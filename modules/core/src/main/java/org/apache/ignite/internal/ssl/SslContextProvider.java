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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.ssl.AbstractSslContextFactory;
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

    /** Outcome of the reloads: the last success and the failures since it. */
    private volatile ReloadOutcome outcome = new ReloadOutcome(0, 0, null, 0);

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

    /** @return Authorities the context in use announces to a client it asks for a certificate; empty if it announces none. */
    public List<X500Principal> trustedAuthorities() {
        return SslCertificates.trustedAuthorities(ctx);
    }

    /** @param lsnr Told about every new context once it is recorded; must not block. */
    void onReload(Runnable lsnr) {
        reloadLsnr = lsnr;
    }

    /**
     * Builds a new context from the factory, puts it in use for new connections and records the outcome. The new context goes in use
     * only if it presents a certificate when the one in use does, every certificate of its chain is valid now and, if nodes connect on
     * it, this node's own trust store accepts it.
     *
     * @throws IgniteCheckedException If the new context fails a check or the factory returns the context in use. The context in use
     *      stays, as it does when the factory itself throws.
     */
    public synchronized void reload() throws IgniteCheckedException {
        try {
            SSLContext rebuilt = build();

            if (rebuilt == ctx)
                throw new IgniteCheckedException("The SSL context factory returns the context in use, so the node gets no new " +
                    "certificates from it");

            X509Certificate[] next = SslCertificates.servedChain(rebuilt);

            check(rebuilt, next);

            putInUse(rebuilt, next);
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
     * If nodes connect on the context and the chain in use is valid now, the trust store in use must also accept the new certificate,
     * or peers that have not renewed yet would refuse it, and the new trust store must accept the certificate in use, or the node
     * would refuse those peers: each node renews at its own time. Peers refuse an expired chain anyway, so it is not compared.
     *
     * @param expected Context the renewal was planned for.
     * @return What the renewal did.
     * @throws IgniteCheckedException If the new context presents another chain that fails the checks of {@link #reload()}, does not
     *      expire later, is refused by the trust store in use or refuses the certificate in use.
     */
    synchronized RenewalResult renew(SSLContext expected) throws IgniteCheckedException {
        if (ctx != expected)
            return RenewalResult.SUPERSEDED;

        try {
            SSLContext rebuilt = build();

            X509Certificate[] next = SslCertificates.servedChain(rebuilt);

            if (Arrays.equals(next, chain))
                return RenewalResult.UNCHANGED;

            check(rebuilt, next);

            if (chain != null && chainNotAfter(next) <= chainNotAfter(chain)) {
                throw new IgniteCheckedException("The new certificate chain expires no later than the one in use; if a certificate " +
                    "authority in the chain expires first, renew it [" + describe(next[0]) +
                    ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(next)) +
                    ", inUseChainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(chain)) + ']');
            }

            if (chain != null && SslCertificates.invalidAt(chain, System.currentTimeMillis()) == null &&
                (transports.contains(COMMUNICATION) || transports.contains(DISCOVERY))) {
                try {
                    SslCertificates.checkAccepts(ctx, rebuilt);
                }
                catch (SSLException e) {
                    throw new IgniteCheckedException("The trust store in use does not accept the new certificate; make sure the trust " +
                        "store of every node holds its issuer, then run control.sh --ssl reload [" + describe(next[0]) + ']', e);
                }

                try {
                    SslCertificates.checkAccepts(rebuilt, ctx);
                }
                catch (SSLException e) {
                    throw new IgniteCheckedException("The new trust store does not accept the certificate in use, so it would refuse " +
                        "peers that have not renewed yet; remove the old authority only after every node serves a certificate from " +
                        "the new one [" + describe(chain[0]) + ']', e);
                }
            }

            putInUse(rebuilt, next);
        }
        catch (Throwable e) {
            onFailure(e);

            throw e;
        }

        onSuccess();

        return RenewalResult.RENEWED;
    }

    /**
     * @param rebuilt Context that passed the checks.
     * @param next Chain it presents, {@code null} if it is unknown.
     */
    private void putInUse(SSLContext rebuilt, @Nullable X509Certificate[] next) {
        chain = next;
        ctx = rebuilt;
    }

    /** @return Context built anew, or the one in use if the factory caches it. */
    private SSLContext build() {
        return factory instanceof AbstractSslContextFactory f ? f.createNew() : factory.create();
    }

    /**
     * @param rebuilt Context built anew.
     * @param next Chain it presents, {@code null} if it is unknown.
     * @throws IgniteCheckedException If it fails a check {@link #reload()} lists.
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
                throw new IgniteCheckedException("The new certificate fails a test handshake with this node's own trust store [" +
                    describe(next == null ? null : next[0]) + ']', e);
            }
        }
    }

    /** Records a successful reload. */
    private void onSuccess() {
        outcome = new ReloadOutcome(System.currentTimeMillis(), 0, null, 0);

        reloadLsnr.run();
    }

    /** @param e Why the reload failed. */
    private void onFailure(Throwable e) {
        ReloadOutcome o = outcome;

        outcome = new ReloadOutcome(o.successTime(), System.currentTimeMillis(), SslCertificates.reason(e), o.failures() + 1);
    }

    /** @return Outcome of the reloads, as one snapshot. */
    public ReloadOutcome reloadOutcome() {
        return outcome;
    }

    /**
     * Outcome of the reloads, as one snapshot.
     *
     * @param successTime Time of the last successful reload, {@code 0} if there was none.
     * @param failureTime Time of the last failed reload since the last successful one, {@code 0} if there was none.
     * @param failureReason Reason of the last failed reload since the last successful one, {@code null} if there was none.
     * @param failures Failed reloads in a row since the last successful one.
     */
    public record ReloadOutcome(long successTime, long failureTime, @Nullable String failureReason, int failures) {
        // No-op.
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

        /** Nothing: the factory returns the certificates in use, there is no newer one yet. */
        UNCHANGED
    }
}
