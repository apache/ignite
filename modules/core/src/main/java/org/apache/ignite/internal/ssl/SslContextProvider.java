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
import java.util.concurrent.ScheduledExecutorService;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SSLContextWrapper;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslCertificates.chainNotAfter;
import static org.apache.ignite.internal.ssl.SslCertificates.describe;

/** Owns one factory's context, reloads and automatic renewal on this node; transports using that factory share its context. */
public class SslContextProvider {
    /** */
    private final Factory<SSLContext> factory;

    /** */
    private final Set<String> transports = new ConcurrentSkipListSet<>();

    /** Context in use. */
    private volatile SSLContext ctx;

    /** Stable context handed to every transport using this factory. */
    private final SSLContext sharedCtx;

    /** Renewal window configured at registration, or {@code 0} when automatic renewal is disabled. */
    private final double renewBeforeFraction;

    /** Automatic renewal, attached before this provider is registered. */
    private @Nullable SslRenewal renewal;

    /** Snapshot of the last successful reload and subsequent failures. */
    private volatile ReloadOutcome outcome = new ReloadOutcome(0, 0, null, 0);

    /** Failed automatic attempts since a chain with a later expiry was put in use. */
    private volatile int renewalFailures;

    /** @param factory Factory to build the context with. */
    SslContextProvider(Factory<SSLContext> factory) {
        this.factory = factory;

        ctx = factory.create();
        ctx.createSSLEngine();

        renewBeforeFraction = factory instanceof AbstractSslContextFactory f && f.isRenewalEnabled() ? f.getRenewBeforeFraction() : 0;

        sharedCtx = new CurrentSslContext(this::context);
    }

    /** @return Factory the context is built with. */
    Factory<SSLContext> factory() {
        return factory;
    }

    /** @return Context to open the next connection with. */
    SSLContext context() {
        return ctx;
    }

    /** @return Stable context shared by every transport configured with this factory. */
    SSLContext sharedContext() {
        return sharedCtx;
    }

    /** @param transport Transport the context is handed to. */
    void addTransport(String transport) {
        transports.add(transport);
    }

    /** @return Comma-separated transport names used in command output and node logs. */
    public String transports() {
        return String.join(", ", transports);
    }

    /** @return Chain loaded with the context in use, own certificate first, or {@code null} if it is unknown. */
    public @Nullable X509Certificate[] certificateChain() {
        return certificateChain(ctx);
    }

    /** @return Certificate loaded with the context in use, or {@code null} if it is unknown. */
    public @Nullable X509Certificate certificate() {
        X509Certificate[] chain = certificateChain(ctx);

        return chain == null ? null : chain[0];
    }

    /**
     * @param exec Renewal thread of the node.
     * @param log Renewal logger.
     */
    void startRenewal(ScheduledExecutorService exec, IgniteLogger log) {
        if (renewBeforeFraction == 0)
            return;

        if (certificateChain() == null) {
            throw new IgniteException("The SSL context factory provides no certificate chain, so it cannot be renewed automatically " +
                "[transports=" + transports() + ']');
        }

        renewal = new SslRenewal(this, renewBeforeFraction, exec, log);
        renewal.replan();
    }

    /**
     * Builds and installs a new context for new connections, recording success or failure. Rejects key material known to have no
     * private key. If a certificate chain is supplied, every certificate must be within its validity period.
     * Certificate metadata is required when automatic renewal is enabled.
     * The administrator must ensure that peers trust the new certificates and can connect with the new context.
     *
     * @return Certificate installed by this reload, or {@code null} if its metadata is unavailable.
     * @throws IgniteCheckedException If the new context fails a check or the factory returns the context in use. On any failure,
     *      including a factory exception, the current context is retained.
     */
    public synchronized @Nullable X509Certificate reload() throws IgniteCheckedException {
        try {
            SSLContext rebuilt = build();

            if (rebuilt == ctx)
                throw new IgniteCheckedException("The SSL context factory returns the context in use, so the node gets no new " +
                    "certificates from it");

            X509Certificate[] next = certificateChain(rebuilt);

            check(next, renewBeforeFraction > 0);

            putInUse(rebuilt, next);
        }
        catch (Throwable e) {
            onFailure(e);

            throw e;
        }

        onSuccess();

        return certificate();
    }

    /**
     * Installs a new context only if its chain expires later than the current chain. Otherwise, renewal would immediately be due again.
     * Returning the current chain counts as an unsuccessful attempt and leaves the context unchanged.
     *
     * @param expected Context the renewal was planned for.
     * @return Certificate installed by this attempt, or {@code null} if the factory returned the chain in use or the planned
     *      context had already been replaced.
     * @throws IgniteCheckedException If the new context supplies no chain, or another chain that fails the checks of {@link #reload()}
     *      or does not expire later.
     */
    synchronized @Nullable X509Certificate renew(SSLContext expected) throws IgniteCheckedException {
        if (ctx != expected)
            return null;

        try {
            SSLContext rebuilt = build();

            X509Certificate[] next = certificateChain(rebuilt);
            X509Certificate[] chain = certificateChain(ctx);

            if (next != null && Arrays.equals(next, chain)) {
                renewalFailures++;

                return null;
            }

            check(next, true);

            if (chain != null && chainNotAfter(next) <= chainNotAfter(chain)) {
                throw new IgniteCheckedException("The new certificate chain expires no later than the one in use; if a certificate " +
                    "authority in the chain expires first, renew it [" + describe(next[0]) +
                    ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(next)) +
                    ", inUseChainNotAfter=" + Instant.ofEpochMilli(chainNotAfter(chain)) + ']');
            }

            putInUse(rebuilt, next);
        }
        catch (Throwable e) {
            if (!Thread.currentThread().isInterrupted() &&
                !X.hasCause(e, InterruptedException.class, IgniteInterruptedException.class, IgniteInterruptedCheckedException.class))
                renewalFailures++;

            onFailure(e);

            throw e;
        }

        onSuccess();

        return certificate();
    }

    /**
     * @param rebuilt Context that passed the checks.
     * @param next Chain loaded with it, {@code null} if it is unknown.
     */
    private void putInUse(SSLContext rebuilt, @Nullable X509Certificate[] next) {
        X509Certificate[] chain = certificateChain(ctx);

        if (next != null && (chain == null || chainNotAfter(next) > chainNotAfter(chain)))
            renewalFailures = 0;

        ctx = rebuilt;
    }

    /**
     * @param ctx Context just built by the factory.
     * @return Chain supplied with that context, or {@code null} if the factory supplies no metadata.
     */
    static @Nullable X509Certificate[] certificateChain(SSLContext ctx) {
        return ctx instanceof SSLContextWrapper wrapper ? wrapper.getCertificateChain() : null;
    }

    /**
     * @return Context built anew, or the one in use if the factory caches it.
     * @throws IgniteCheckedException If the factory reports that the loaded material has no private key.
     */
    private SSLContext build() throws IgniteCheckedException {
        SSLContext rebuilt = factory instanceof AbstractSslContextFactory f ? f.createNew() : factory.create();

        if (rebuilt instanceof SSLContextWrapper wrapper && wrapper.isPrivateKeyMissing())
            throw new IgniteCheckedException("The new key material has no private key, so the node would present no certificate");

        rebuilt.createSSLEngine();

        return rebuilt;
    }

    /**
     * @param next Chain loaded with the new context, {@code null} if it is unknown.
     * @param requireChain Whether automatic renewal requires a known chain.
     * @throws IgniteCheckedException If the chain is required but missing, or a certificate is outside its validity period.
     */
    private void check(@Nullable X509Certificate[] next, boolean requireChain) throws IgniteCheckedException {
        if (next == null && requireChain)
            throw new IgniteCheckedException("The new SSL context provides no certificate chain, so it cannot be renewed automatically");

        long now = System.currentTimeMillis();

        X509Certificate invalid = next == null ? null : SslCertificates.invalidAt(next, now);

        if (invalid != null) {
            throw new IgniteCheckedException("The new certificate chain is not valid now [" + describe(invalid) + ", now=" +
                Instant.ofEpochMilli(now) + ']');
        }
    }

    /** Records a successful reload. */
    private void onSuccess() {
        outcome = new ReloadOutcome(System.currentTimeMillis(), 0, null, 0);

        if (renewal != null)
            renewal.replan();
    }

    /** @param e Why the reload failed. */
    private void onFailure(Throwable e) {
        ReloadOutcome o = outcome;

        outcome = new ReloadOutcome(o.successTime(), System.currentTimeMillis(), SslCertificates.reason(e), o.failures() + 1);
    }

    /** @return Snapshot of the last successful reload and subsequent failures. */
    public ReloadOutcome reloadOutcome() {
        return outcome;
    }

    /** @return Failed automatic attempts since a chain with a later expiry was put in use, including unchanged certificates. */
    int renewalFailures() {
        return renewalFailures;
    }

    /**
     * Snapshot of the last successful reload and subsequent failures.
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
        return renewal == null ? 0 : renewal.nextRenewalTime();
    }
}
