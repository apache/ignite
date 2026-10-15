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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.ssl.SslContextProvider.RenewalResult.RENEWED;
import static org.apache.ignite.internal.ssl.SslContextProvider.RenewalResult.UNCHANGED;

/**
 * Renews the certificate of one context built by a factory with {@link AbstractSslContextFactory#setRenewalEnabled(boolean) renewal
 * enabled}, before it expires. Everything but {@link #replan()} runs in the renewal thread.
 */
class SslRenewal {
    /** How the node log names an automatic renewal in {@code initiator=}. */
    private static final String INITIATOR = "automatic renewal";

    /** How often the node repeats that the factory has no newer certificates; it also logs at once when the ones in use expire soon. */
    private static final long WAIT_LOG_INTERVAL = TimeUnit.DAYS.toMillis(1);

    /** Pause after the first attempt that puts nothing in use, in milliseconds, and the shortest time between two attempts. */
    private static long minPause = 60_000L;

    /** Longest pause between attempts that put nothing in use, in milliseconds. */
    private static long maxPause = 3_600_000L;

    /** */
    private final SslContextProvider provider;

    /** Share of the certificate lifetime left when the renewal window opens. */
    private final double renewBeforeFraction;

    /** */
    private final ScheduledExecutorService exec;

    /** */
    private final IgniteLogger log;

    /** Last planned attempt, {@code null} before the first one. */
    private ScheduledFuture<?> next;

    /** Earliest expiry in the chain in use when the renewal was planned. */
    private long chainNotAfter;

    /** How long before {@link #chainNotAfter} the renewal window opens. */
    private long window;

    /** Time of the last attempt, {@code 0} if there was none. */
    private long lastAttemptTime;

    /** Pause after the last attempt that put nothing in use, without the random part; {@link #plan()} resets it to {@code 0}. */
    private long pause;

    /** When the node last logged that the factory has no newer certificates; {@link #plan()} resets it to {@code 0}. */
    private long waitLogTime;

    /** Whether that was logged as an error. */
    private boolean waitLoggedSoon;

    /**
     * @param provider Provider whose context the factory builds.
     * @param renewBeforeFraction Share of the certificate lifetime left when the renewal window opens.
     * @param exec Renewal thread.
     * @param log Logger.
     */
    SslRenewal(SslContextProvider provider, double renewBeforeFraction, ScheduledExecutorService exec, IgniteLogger log) {
        this.provider = provider;
        this.renewBeforeFraction = renewBeforeFraction;
        this.exec = exec;
        this.log = log;
    }

    /** Renews the certificate of the provider from now on. */
    void start() {
        provider.onReload(this::replan);

        replan();
    }

    /** Plans the next renewal in the renewal thread. */
    private void replan() {
        submit(this::plan, 0);
    }

    /** Plans the next renewal by the certificate in use. */
    private void plan() {
        pause = waitLogTime = 0;
        waitLoggedSoon = false;

        X509Certificate[] chain = provider.servedChain();

        if (chain == null) {
            U.warn(log, "Cannot tell when the TLS certificate expires, so it is not renewed automatically [transports=" +
                provider.transports() + ']');

            return;
        }

        chainNotAfter = SslCertificates.chainNotAfter(chain);

        long lifetime = Math.max(0, chainNotAfter - chain[0].getNotBefore().getTime());

        window = (long)(lifetime * renewBeforeFraction);

        long at = Math.max(chainNotAfter - window, Math.max(lastAttemptTime + minPause, U.currentTimeMillis()));

        schedule(at);

        U.log(log, "TLS certificates will be renewed automatically [transports=" + provider.transports() +
            ", nextRenewal=" + Instant.ofEpochMilli(at) + ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ']');
    }

    /**
     * Puts a renewed certificate in use; a failure, or the factory returning the certificates in use, plans another attempt.
     *
     * @param planned Context in use when the attempt was planned.
     */
    private void attempt(SSLContext planned) {
        lastAttemptTime = U.currentTimeMillis();

        try {
            SslContextProvider.RenewalResult res = provider.renew(planned);

            if (res == RENEWED) {
                U.log(log, "TLS certificates reloaded [transports=" + provider.transports() + ", " +
                    SslCertificates.describe(provider.servedCertificate()) + ", initiator=" + INITIATOR + ']');
            }
            else if (res == UNCHANGED)
                retry(null);
        }
        catch (Throwable e) {
            retry(e);
        }
    }

    /**
     * Plans another attempt after a pause that doubles with every attempt that puts nothing in use, and logs why. A failure is logged
     * every time. The factory returning the certificates in use, as when their files are not replaced yet, is no failure. It is logged
     * once a day, and at once when the certificates in use expire soon. Once the node stops, nothing is planned or logged: the stop
     * interrupts the attempt in progress, and that is no failure.
     *
     * @param e Why the attempt failed, {@code null} if the factory returned the certificates in use.
     */
    private void retry(@Nullable Throwable e) {
        if (exec.isShutdown())
            return;

        long now = U.currentTimeMillis();

        pause = pause == 0 ? minPause : Math.min(pause * 2, maxPause);

        long at = now + pause + ThreadLocalRandom.current().nextLong(pause / 2 + 1);

        schedule(at);

        boolean soon = now >= chainNotAfter - window / 2 || at >= chainNotAfter;

        if (e == null) {
            if (now - waitLogTime < WAIT_LOG_INTERVAL && (waitLoggedSoon || !soon))
                return;

            waitLogTime = now;
            waitLoggedSoon = soon;
        }

        String msg = (e == null ? "The SSL context factory has no newer TLS certificates yet, the ones in use stay" :
            "Failed to reload TLS certificates, the ones in use stay") +
            (now >= chainNotAfter ? ", though they have expired" : soon ? " and expire soon" : "") +
            " [transports=" + provider.transports() +
            (e == null ? "" : ", initiator=" + INITIATOR + ", reason=" + SslCertificates.reason(e)) +
            ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ", nextRenewal=" + Instant.ofEpochMilli(at) + ']';

        if (soon)
            U.error(log, msg, e);
        else
            U.warn(log, msg, e);
    }

    /** @param at Time of the next attempt. */
    private void schedule(long at) {
        if (next != null)
            next.cancel(false);

        SSLContext planned = provider.context();

        provider.nextRenewalTime(at);

        next = submit(() -> attempt(planned), Math.max(0, at - U.currentTimeMillis()));
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
        try (Scope ignored = OperationContext.restoreSnapshot(null)) {
            return exec.schedule(task, delay, TimeUnit.MILLISECONDS);
        }
        catch (RejectedExecutionException stopping) {
            return null;
        }
    }
}
