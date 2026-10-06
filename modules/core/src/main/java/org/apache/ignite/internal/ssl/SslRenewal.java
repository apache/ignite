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
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslContextProvider.Renewed.RENEWED;
import static org.apache.ignite.internal.ssl.SslContextProvider.Renewed.UNCHANGED;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/**
 * Renews the certificates of the contexts built by a factory with {@link AbstractSslContextFactory#setRenewalEnabled(boolean) renewal
 * enabled}, before they expire. All the renewals of a node run in one thread of its own, so that a request to the issuer holds up no thread
 * of the node and no attempt races the planning that follows a reload by the command.
 */
public class SslRenewal {
    /** How the node log names an automatic renewal among those who start a reload. */
    private static final String INITIATOR = "automatic renewal";

    /** The longest pause between attempts that put nothing in use is cut to this share of the window, to leave room for several. */
    private static final int ATTEMPTS_IN_WINDOW = 4;

    /** How often the node logs that the factory still has no newer certificates, unless they come to expire soon in between. */
    private static final long WAIT_LOG_INTERVAL = TimeUnit.DAYS.toMillis(1);

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
         * @return Its renewal settings, {@code null} if it does not renew.
         * @throws IgniteException If a setting is out of range.
         */
        public static @Nullable Settings of(Factory<SSLContext> factory) {
            if (!(factory instanceof AbstractSslContextFactory) || !((AbstractSslContextFactory)factory).isRenewalEnabled())
                return null;

            return new Settings((AbstractSslContextFactory)factory);
        }

        /**
         * @param factory Factory.
         * @throws IgniteException If a setting is out of range.
         */
        private Settings(AbstractSslContextFactory factory) {
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

        /** Attempts in a row that put nothing in use since the certificate in use was planned for. */
        private int misses;

        /** When the node last logged that the factory has no newer certificates, {@code 0} if it did not since the planning. */
        private long waitLogTime;

        /** Whether that was logged as an error. */
        private boolean waitLoggedSoon;

        /**
         * @param provider Provider.
         * @param settings Settings.
         */
        private Renewal(SslContextProvider provider, Settings settings) {
            this.provider = provider;
            this.settings = settings;
        }

        /** Plans the next renewal in the renewal thread; called for every new certificate. */
        private void replan() {
            submit(this::plan, 0);
        }

        /** Plans the next renewal by the certificate in use. */
        private void plan() {
            if (exec.isShutdown())
                return;

            misses = 0;
            waitLogTime = 0;
            waitLoggedSoon = false;

            X509Certificate[] chain = provider.servedChain();

            if (chain == null) {
                cancel();

                provider.nextRenewalTime(0);

                String msg = "Cannot tell when the TLS certificate expires, so it is not renewed automatically";

                provider.onFailure(new IgniteException(msg));

                U.warn(log, msg + " [transports=" + transports() + ']');

                return;
            }

            long expiry = SslCertificates.chainNotAfter(chain);
            long window = window(chain);
            long due = expiry - window - (long)(ThreadLocalRandom.current().nextDouble() * settings.jitter * window);
            long earliest = lastAttempt + settings.minRetry;
            long at = Math.max(Math.max(due, earliest), U.currentTimeMillis());

            schedule(at);

            if (log.isInfoEnabled()) {
                log.info("TLS certificates will be renewed automatically [transports=" + transports() + ", at=" + Instant.ofEpochMilli(at) +
                    ", expiry=" + Instant.ofEpochMilli(expiry) + ']');
            }
        }

        /** Puts a renewed certificate in use; a failure, or the certificates in use handed back, plans another attempt. */
        private void attempt() {
            lastAttempt = U.currentTimeMillis();

            try {
                SslContextProvider.Renewed res = provider.renew(planned);

                if (res == RENEWED)
                    provider.onReloaded(log, INITIATOR);
                else if (res == UNCHANGED)
                    onUnchanged();
            }
            catch (Throwable e) {
                onFailure(e);
            }
        }

        /** @param e Why the attempt failed. */
        private void onFailure(Throwable e) {
            if (exec.isShutdown())
                return;

            String reason = provider.onFailure(e);

            long now = U.currentTimeMillis();

            X509Certificate[] chain = provider.servedChain();

            long at = retryLater(chain, now);

            boolean soon = expiresSoon(chain, now, at);

            String msg = "Failed to reload TLS certificates, the ones in use stay" + state(chain, now, soon) + " [transports=" +
                transports() + ", initiator=" + INITIATOR + ", reason=" + reason + times(chain, at) + ']';

            if (soon)
                U.error(log, msg, e);
            else
                U.warn(log, msg, e);
        }

        /**
         * Waits for the factory to hand back newer certificates, as when their files are not replaced yet, which is no failure: the node
         * logs it once a day, and at once when the certificates in use come to expire soon.
         */
        private void onUnchanged() {
            if (exec.isShutdown())
                return;

            long now = U.currentTimeMillis();

            X509Certificate[] chain = provider.servedChain();

            long at = retryLater(chain, now);

            boolean soon = expiresSoon(chain, now, at);

            if (now - waitLogTime < WAIT_LOG_INTERVAL && (waitLoggedSoon || !soon))
                return;

            waitLogTime = now;
            waitLoggedSoon = soon;

            String msg = "The SSL context factory has no newer TLS certificates yet, the ones in use stay" + state(chain, now, soon) +
                " [transports=" + transports() + times(chain, at) + ']';

            if (soon)
                U.error(log, msg);
            else
                U.warn(log, msg);
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @param now Current time.
         * @return Time of the next attempt, planned after a pause.
         */
        private long retryLater(@Nullable X509Certificate[] chain, long now) {
            misses++;

            long at = now + pause(chain);

            schedule(at);

            return at;
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @param now Current time.
         * @param at Time of the next attempt.
         * @return Whether less than half of the window is left or the next attempt comes after expiry.
         */
        private boolean expiresSoon(@Nullable X509Certificate[] chain, long now, long at) {
            if (chain == null)
                return false;

            long expiry = SslCertificates.chainNotAfter(chain);

            return now >= expiry - window(chain) / 2 || at >= expiry;
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @param now Current time.
         * @param soon Whether it expires soon.
         * @return How the log names the state of the certificates in use.
         */
        private String state(@Nullable X509Certificate[] chain, long now, boolean soon) {
            if (chain != null && now >= SslCertificates.chainNotAfter(chain))
                return ", though they have expired";

            return soon ? " and expire soon" : "";
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @param at Time of the next attempt.
         * @return Expiry and the next attempt, as the log gives them.
         */
        private String times(@Nullable X509Certificate[] chain, long at) {
            return (chain == null ? "" : ", expiry=" + Instant.ofEpochMilli(SslCertificates.chainNotAfter(chain))) +
                ", nextAttempt=" + Instant.ofEpochMilli(at);
        }

        /**
         * @param chain Chain in use, {@code null} if unknown.
         * @return Pause before the next attempt after those that put nothing in use, spread at random.
         */
        private long pause(@Nullable X509Certificate[] chain) {
            long max = chain == null ? settings.maxRetry :
                Math.max(settings.minRetry, Math.min(settings.maxRetry, window(chain) / ATTEMPTS_IN_WINDOW));

            long pause = settings.minRetry;

            for (int i = 1; i < misses && pause < max; i++)
                pause = pause > max / 2 ? max : pause * 2;

            pause = Math.min(pause, max);

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

            next = submit(this::attempt, Math.max(0, at - U.currentTimeMillis()));
        }

        /** */
        private void cancel() {
            if (next != null) {
                next.cancel(false);

                next = null;
            }
        }

        /**
         * Runs the task in the renewal thread in the operation context of the node, never of whoever caused it, such as an operator
         * running {@code --ssl reload} under their security subject.
         *
         * @param task Task.
         * @param delay Delay, in milliseconds.
         * @return Future of the task, {@code null} if the node is stopping.
         */
        private @Nullable ScheduledFuture<?> submit(Runnable task, long delay) {
            try (Scope nodeCtx = OperationContext.restoreSnapshot(null)) {
                return exec.schedule(task, delay, TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException stopping) {
                return null;
            }
        }

        /** */
        private String transports() {
            return String.join(", ", provider.transports());
        }
    }
}
