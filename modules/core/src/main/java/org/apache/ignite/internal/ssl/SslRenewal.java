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
 * Renews the certificates of the contexts a {@link RenewableSslContextFactory} builds, before they expire. All the renewals of a node run
 * in one thread of its own, so that a request to the issuer holds up no thread of the node and no attempt races the planning that follows a
 * reload by the command.
 */
public class SslRenewal {
    /** How the node log names an automatic renewal among those who start a reload. */
    private static final String INITIATOR = "automatic renewal";

    /** The longest pause between failed attempts is cut to this share of the window, so that an outage leaves room for several. */
    private static final int ATTEMPTS_IN_WINDOW = 4;

    /** */
    private final @Nullable String igniteInstanceName;

    /** */
    private final IgniteLogger log;

    /** Thread the renewals run in, created with the first renewal. */
    private volatile ScheduledExecutorService exec;

    /** */
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
     * Renews the certificates of the provider from now on.
     *
     * @param provider Provider whose context the factory of the settings builds.
     * @param settings Renewal settings of the factory.
     */
    public synchronized void start(SslContextProvider provider, Settings settings) {
        if (stopped)
            return;

        if (exec == null)
            exec = newSingleThreadScheduledExecutor("ssl-renewal", igniteInstanceName);

        Renewal renewal = new Renewal(provider, settings);

        provider.onReload(renewal::replan);

        renewal.replan();
    }

    /** Stops the renewals, interrupting an attempt in progress. */
    public synchronized void stop() {
        stopped = true;

        if (exec != null)
            exec.shutdownNow();
    }

    /** Renewal settings of a factory, read once and checked, so that the factory cannot take the schedule out of range later. */
    public static class Settings {
        /** */
        private final double fraction;

        /** */
        private final long renewBefore;

        /** */
        private final double jitter;

        /** */
        private final long minRetry;

        /** */
        private final long maxRetry;

        /**
         * @param factory Factory.
         * @throws IgniteException If a setting is out of range.
         */
        public Settings(RenewableSslContextFactory factory) {
            fraction = factory.getRenewBeforeFraction();
            renewBefore = factory.getRenewBefore();
            jitter = factory.getRenewalJitter();
            minRetry = factory.getRenewalRetryMinInterval();
            maxRetry = factory.getRenewalRetryMaxInterval();

            String err = null;

            if (!(fraction > 0 && fraction < 1))
                err = "renewBeforeFraction must be greater than 0 and less than 1";
            else if (renewBefore < 0)
                err = "renewBefore must not be negative";
            else if (!(jitter >= 0 && jitter <= 1))
                err = "renewalJitter must be from 0 to 1";
            else if (fraction * (1 + jitter) >= 1)
                err = "renewBeforeFraction * (1 + renewalJitter) must be less than 1, or a renewal may fall before the certificate starts";
            else if (minRetry <= 0)
                err = "renewalRetryMinInterval must be positive";
            else if (maxRetry < minRetry)
                err = "renewalRetryMaxInterval must not be less than renewalRetryMinInterval";

            if (err != null) {
                throw new IgniteException("Invalid automatic renewal settings of the SSL context factory, " + err + " [factory=" +
                    factory.getClass().getName() + ", renewBeforeFraction=" + fraction + ", renewBefore=" + renewBefore +
                    ", renewalJitter=" + jitter + ", renewalRetryMinInterval=" + minRetry + ", renewalRetryMaxInterval=" + maxRetry + ']');
            }
        }
    }

    /** Renewal of one context. Everything but {@link #replan()} runs in the renewal thread. */
    private class Renewal {
        /** */
        private final SslContextProvider provider;

        /** */
        private final Settings settings;

        /** Next attempt, {@code null} if none is planned. */
        private ScheduledFuture<?> next;

        /** Context in use when the next attempt was planned. */
        private SSLContext planned;

        /** Time of the last attempt, {@code 0} if there was none. */
        private long lastAttempt;

        /**
         * @param provider Provider.
         * @param settings Settings.
         */
        private Renewal(SslContextProvider provider, Settings settings) {
            this.provider = provider;
            this.settings = settings;
        }

        /** Plans the next renewal by the certificate in use, in the renewal thread, as told of every new certificate. */
        private void replan() {
            // The renewals belong to the node: the executor would otherwise carry the operation context of whoever ran the reload, its
            // security subject included, into every renewal from then on.
            try (Scope clean = OperationContext.restoreSnapshot(null)) {
                exec.execute(this::plan);
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

                provider.nextRenewalTime(0);

                String msg = "Cannot tell when the TLS certificate expires, so it is not renewed automatically";

                // A failure, so that the status command and the metrics show that the renewal is off.
                provider.onFailure(new IgniteException(msg));

                U.warn(log, msg + " [transports=" + transports() + ']');

                return;
            }

            long expiry = SslCertificates.chainNotAfter(chain);
            long window = window(chain);
            long shift = (long)(ThreadLocalRandom.current().nextDouble() * settings.jitter * window);

            // A certificate due for renewal as soon as it is issued must not make the node come back for another one without a pause, and a
            // window open already means now.
            long at = Math.max(Math.max(expiry - window - shift, lastAttempt + settings.minRetry), U.currentTimeMillis());

            schedule(at);

            if (log.isInfoEnabled()) {
                log.info("TLS certificates will be renewed automatically [transports=" + transports() + ", at=" + Instant.ofEpochMilli(at) +
                    ", expiry=" + Instant.ofEpochMilli(expiry) + ']');
            }
        }

        /** Puts a context with a renewed certificate in use, which plans the next renewal, or plans another attempt. */
        private void attempt() {
            lastAttempt = U.currentTimeMillis();

            boolean renewed;

            try {
                renewed = provider.renew(planned);
            }
            catch (Throwable e) {
                // Anything may come out of a user-supplied factory, and an attempt that plans no next one ends the renewals for good.
                onFailure(e);

                return;
            }

            // A reload by the command put a certificate in use while this attempt waited, and planned the next renewal by it.
            if (renewed)
                provider.onReloaded(log, INITIATOR);
        }

        /** @param e Why the attempt failed. */
        private void onFailure(Throwable e) {
            // Stopping the node interrupts the attempt, which is no failure of the certificate.
            if (exec.isShutdown())
                return;

            String reason = provider.onFailure(e);

            long now = U.currentTimeMillis();

            X509Certificate[] chain = provider.servedChain();

            long at = now + pause(chain);

            schedule(at);

            long expiry = chain == null ? 0 : SslCertificates.chainNotAfter(chain);

            // Also when the next attempt only comes after expiry, as it may with a window shorter than the pause.
            boolean late = chain != null && (now >= expiry - window(chain) / 2 || at >= expiry);

            String state = chain != null && now >= expiry ? ", though they have expired" : late ? " and expire soon" : "";

            String msg = "Failed to reload TLS certificates, the ones in use stay" + state + " [transports=" + transports() +
                ", initiator=" + INITIATOR + ", reason=" + reason + (chain == null ? "" : ", expiry=" + Instant.ofEpochMilli(expiry)) +
                ", nextAttempt=" + Instant.ofEpochMilli(at) + ']';

            if (late)
                U.error(log, msg, e);
            else
                U.warn(log, msg, e);
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @return Pause before the next attempt after the failed ones.
         */
        private long pause(@Nullable X509Certificate[] chain) {
            long max = chain == null ? settings.maxRetry :
                Math.max(settings.minRetry, Math.min(settings.maxRetry, window(chain) / ATTEMPTS_IN_WINDOW));

            long pause = settings.minRetry;

            for (int i = 1; i < provider.failures() && pause < max; i++)
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
            long window = (long)(Math.max(0, SslCertificates.chainNotAfter(chain) - chain[0].getNotBefore().getTime()) * settings.fraction);

            return settings.renewBefore > 0 ? Math.min(window, settings.renewBefore) : window;
        }

        /** @param at Time of the next attempt. */
        private void schedule(long at) {
            cancel();

            planned = provider.context();

            provider.nextRenewalTime(at);

            try {
                next = exec.schedule(this::attempt, Math.max(0, at - U.currentTimeMillis()), TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException ignored) {
                // The node is stopping.
            }
        }

        /** */
        private void cancel() {
            if (next != null) {
                next.cancel(false);

                next = null;
            }
        }

        /** */
        private String transports() {
            return String.join(", ", provider.transports());
        }
    }
}
