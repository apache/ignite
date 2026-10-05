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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.RenewableSslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/**
 * Renews the certificates of the contexts a {@link RenewableSslContextFactory} builds, before they expire.
 * <p>
 * All the planning and every attempt of a node run in a thread of their own, so that a request to a certificate
 * authority service never holds up a thread of the node. A single thread also keeps an attempt from racing the
 * planning that follows a reload by the {@code --ssl reload} command.
 */
public class SslRenewal {
    /** How the node log names an automatic renewal among those who start a reload. */
    private static final String INITIATOR = "automatic renewal";

    /**
     * The longest pause between failed attempts is cut down to this share of the window, so that a long outage of the
     * issuer still leaves room for several attempts before the certificate expires.
     */
    private static final int ATTEMPTS_IN_WINDOW = 4;

    /** */
    private final @Nullable String igniteInstanceName;

    /** */
    private final IgniteLogger log;

    /** Providers renewed, so that one serving several transports is renewed once. */
    private final Set<SslContextProvider> renewed = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Thread the renewals run in, created when the first renewable context shows up. */
    private volatile ScheduledExecutorService exec;

    /** Whether the node is stopping. */
    private boolean stopped;

    /**
     * @param igniteInstanceName Name of the node, for the thread name.
     * @param log Logger.
     */
    public SslRenewal(@Nullable String igniteInstanceName, IgniteLogger log) {
        this.igniteInstanceName = igniteInstanceName;
        this.log = log;
    }

    /**
     * @param factory Factory to check the renewal settings of.
     * @throws IgniteException If a setting is out of range.
     */
    public static void validate(RenewableSslContextFactory factory) {
        double fraction = factory.getRenewBeforeFraction();
        double jitter = factory.getRenewalJitter();

        String err = null;

        if (!(fraction > 0 && fraction < 1))
            err = "renewBeforeFraction must be greater than 0 and less than 1";
        else if (factory.getRenewBefore() < 0)
            err = "renewBefore must not be negative";
        else if (!(jitter >= 0 && jitter <= 1))
            err = "renewalJitter must be from 0 to 1";
        else if (fraction * (1 + jitter) >= 1)
            err = "renewBeforeFraction * (1 + renewalJitter) must be less than 1, or a renewal may fall before " +
                "the certificate becomes valid";
        else if (factory.getRenewalRetryMinInterval() <= 0)
            err = "renewalRetryMinInterval must be positive";
        else if (factory.getRenewalRetryMaxInterval() < factory.getRenewalRetryMinInterval())
            err = "renewalRetryMaxInterval must not be less than renewalRetryMinInterval";

        if (err != null) {
            throw new IgniteException("Invalid automatic renewal settings of the SSL context factory, " + err +
                " [factory=" + factory.getClass().getName() +
                ", renewBeforeFraction=" + fraction +
                ", renewBefore=" + factory.getRenewBefore() +
                ", renewalJitter=" + jitter +
                ", renewalRetryMinInterval=" + factory.getRenewalRetryMinInterval() +
                ", renewalRetryMaxInterval=" + factory.getRenewalRetryMaxInterval() + ']');
        }
    }

    /**
     * Starts renewing the certificates of the provider, unless they are renewed already. Called once the provider
     * serves a transport: a context that serves nothing is not worth a request to the issuer.
     *
     * @param provider Provider whose context the factory builds.
     * @param factory Factory, with its settings checked by {@link #validate}.
     */
    public synchronized void register(SslContextProvider provider, RenewableSslContextFactory factory) {
        if (stopped || !renewed.add(provider))
            return;

        if (exec == null)
            exec = newSingleThreadScheduledExecutor("ssl-renewal", igniteInstanceName);

        Renewal renewal = new Renewal(provider, factory);

        provider.onReload(renewal::replan);

        renewal.replan();
    }

    /**
     * Stops the renewals, interrupting an attempt in progress. An attempt that does not give way to the interrupt
     * is left to finish, and whatever it brings is dropped.
     */
    public synchronized void stop() {
        stopped = true;

        if (exec != null)
            exec.shutdownNow();
    }

    /** Renewal of one context. Everything but {@link #replan()} runs in the renewal thread only. */
    private class Renewal {
        /** */
        private final SslContextProvider provider;

        /** Read once, so that a factory cannot take the schedule out of the range checked at start. */
        private final double fraction;

        /** */
        private final long renewBefore;

        /** */
        private final double jitter;

        /** */
        private final long minRetry;

        /** */
        private final long maxRetry;

        /** Next attempt, {@code null} if none is planned. */
        private ScheduledFuture<?> next;

        /** Context in use when the next attempt was planned. */
        private SSLContext planned;

        /** Failed attempts in a row. */
        private int failures;

        /** Time of the last attempt, {@code 0} if there was none. */
        private long lastAttempt;

        /**
         * @param provider Provider whose context the factory builds.
         * @param factory Factory.
         */
        private Renewal(SslContextProvider provider, RenewableSslContextFactory factory) {
            this.provider = provider;

            fraction = factory.getRenewBeforeFraction();
            renewBefore = factory.getRenewBefore();
            jitter = factory.getRenewalJitter();
            minRetry = factory.getRenewalRetryMinInterval();
            maxRetry = factory.getRenewalRetryMaxInterval();
        }

        /**
         * Plans the next renewal by the certificate in use and drops the retries: the provider has started serving,
         * or a reload has just put a new certificate in use. Only hands the work over to the renewal thread, as it is
         * called with the provider locked.
         */
        private void replan() {
            // The renewals belong to the node, not to whoever ran the reload: the executor would otherwise carry the
            // operation context of the caller, its security subject included, into every renewal from then on.
            try (Scope clean = OperationContext.restoreSnapshot(null)) {
                exec.execute(() -> {
                    failures = 0;

                    plan();
                });
            }
            catch (RejectedExecutionException ignored) {
                // The node is stopping.
            }
        }

        /** Plans the next renewal by the certificate in use. */
        private void plan() {
            if (exec.isShutdown())
                return;

            X509Certificate[] chain = provider.servedChain();

            if (chain == null) {
                cancel();

                provider.reloadState().nextRenewalTime(0);

                String reason = "Cannot tell when the TLS certificate expires, so it is not renewed automatically";

                // Recorded as a failure, so that the status command and the metrics show that renewal is off.
                provider.reloadState().onFailure(reason);

                U.warn(log, reason + " [transports=" + users() + ']');

                return;
            }

            long expiry = SslCertificates.chainNotAfter(chain);

            long window = window(chain);

            long shift = (long)(ThreadLocalRandom.current().nextDouble() * jitter * window);

            // A certificate due for renewal as soon as it is issued must not make the node come back for another one
            // without a pause.
            long at = Math.max(expiry - window - shift, lastAttempt + minRetry);

            // A window that is open already means now, and that is what the log and the metrics must say.
            at = Math.max(at, U.currentTimeMillis());

            schedule(at);

            if (log.isInfoEnabled()) {
                log.info("TLS certificates will be renewed automatically [transports=" + users() +
                    ", at=" + Instant.ofEpochMilli(at) + ", expiry=" + Instant.ofEpochMilli(expiry) + ']');
            }
        }

        /** Puts a context with a renewed certificate in use, or plans another attempt if that fails. */
        private void attempt() {
            lastAttempt = U.currentTimeMillis();

            boolean renewed;

            try {
                renewed = provider.renew(planned);
            }
            catch (Throwable e) {
                // Anything may come out of a user-supplied factory, and an attempt that ends without planning the
                // next one would end the renewals for good.
                onFailure(e);

                return;
            }

            // A reload by the command put a certificate in use while this attempt waited for the provider, and has
            // planned the next renewal by it already. A stopping node drops whatever the attempt brought.
            if (!renewed || exec.isShutdown())
                return;

            failures = 0;

            provider.reloadState().onSuccess();

            if (log.isInfoEnabled()) {
                String desc = SslCertificates.describe(provider.servedCertificate());

                log.info("TLS certificates reloaded [transports=" + users() + (desc.isEmpty() ? "" : ", " + desc) +
                    ", initiator=" + INITIATOR + ']');
            }

            plan();
        }

        /**
         * @param e Why the attempt failed.
         */
        private void onFailure(Throwable e) {
            // Stopping the node interrupts the attempt, which is no failure of the certificate.
            if (exec.isShutdown())
                return;

            failures++;

            String reason = SslReloadState.reason(e);

            provider.reloadState().onFailure(reason);

            long now = U.currentTimeMillis();

            X509Certificate[] chain = provider.servedChain();

            long at = now + pause(chain);

            schedule(at);

            long expiry = chain == null ? 0 : SslCertificates.chainNotAfter(chain);

            boolean expired = chain != null && now >= expiry;

            // Also when the next attempt only comes after expiry, as it may with a window shorter than the pause.
            boolean late = chain != null && (now >= expiry - window(chain) / 2 || at >= expiry);

            String msg = "Failed to reload TLS certificates, the ones in use stay" +
                (expired ? ", though they have expired" : late ? " and expire soon" : "") +
                " [transports=" + users() + ", initiator=" + INITIATOR + ", reason=" + reason +
                (chain == null ? "" : ", expiry=" + Instant.ofEpochMilli(expiry)) +
                ", nextAttempt=" + Instant.ofEpochMilli(at) + ']';

            if (late)
                U.error(log, msg, e);
            else
                U.warn(log, msg, e);
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @return Pause before the next attempt after {@link #failures} failed ones.
         */
        private long pause(@Nullable X509Certificate[] chain) {
            long max = maxRetry;

            if (chain != null)
                max = Math.max(minRetry, Math.min(max, window(chain) / ATTEMPTS_IN_WINDOW));

            long pause = minRetry;

            for (int i = 1; i < failures && pause < max; i++)
                pause = pause > max / 2 ? max : pause * 2;

            pause = Math.min(pause, max);

            // Nodes that fail together would otherwise come back together.
            return pause + ThreadLocalRandom.current().nextLong(pause / 2 + 1);
        }

        /**
         * @param chain Chain in use.
         * @return How long before expiry the certificate is renewed.
         */
        private long window(X509Certificate[] chain) {
            long lifetime = Math.max(0, SslCertificates.chainNotAfter(chain) - chain[0].getNotBefore().getTime());

            long window = (long)(lifetime * fraction);

            return renewBefore > 0 ? Math.min(window, renewBefore) : window;
        }

        /**
         * @param at Time of the next attempt.
         */
        private void schedule(long at) {
            cancel();

            planned = provider.context();

            provider.reloadState().nextRenewalTime(at);

            try {
                next = exec.schedule(this::attempt, Math.max(0, at - U.currentTimeMillis()), TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException ignored) {
                // The node is stopping.
            }
        }

        /** Drops the planned attempt. */
        private void cancel() {
            if (next != null) {
                next.cancel(false);

                next = null;
            }
        }

        /**
         * @return Transports the context serves, as the node log names them.
         */
        private String users() {
            return String.join(", ", provider.users());
        }
    }
}
