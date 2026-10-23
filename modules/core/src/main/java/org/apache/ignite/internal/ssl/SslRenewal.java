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

/**
 * Renews one factory's certificates before expiry when {@link AbstractSslContextFactory#setRenewalEnabled(boolean) renewal is enabled}.
 * Renewal planning and attempts run in the node's renewal thread.
 */
class SslRenewal {
    /** How the node log names an automatic renewal in {@code initiator=}. */
    private static final String INITIATOR = "automatic renewal";

    /**
     * Interval between warnings that the factory has no newer certificates. A change in expiry status bypasses this interval.
     */
    private static final long WAIT_LOG_INTERVAL = TimeUnit.DAYS.toMillis(1);

    /** Initial retry pause and minimum interval between renewal attempts, in milliseconds. */
    private static long minPause = 60_000L;

    /** Maximum retry pause before adding jitter, in milliseconds. */
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

    /** Time of the next automatic renewal, {@code 0} if none is planned. */
    private volatile long nextRenewalTime;

    /** Earliest expiry in the chain in use when the renewal was planned. */
    private long chainNotAfter;

    /** How long before {@link #chainNotAfter} the renewal window opens. */
    private long window;

    /** Current retry pause before adding jitter; {@link #plan()} resets it to {@code 0}. */
    private long pause;

    /** When the node last logged that the factory has no newer certificates; {@link #plan()} resets it to {@code 0}. */
    private long waitLogTime;

    /** How late the certificates in use were when that was logged: {@code 0} in time, {@code 1} expiring soon, {@code 2} expired. */
    private int waitLoggedLateness;

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

    /** @return Time of the next automatic renewal, {@code 0} if none is planned. */
    long nextRenewalTime() {
        return nextRenewalTime;
    }

    /** Plans the next renewal in the renewal thread. */
    void replan() {
        submit(this::plan, 0);
    }

    /** Plans the next renewal from the current certificate chain's expiry. */
    private void plan() {
        pause = waitLogTime = 0;

        SSLContext planned = provider.context();
        X509Certificate[] chain = SslContextProvider.certificateChain(planned);

        chainNotAfter = SslCertificates.chainNotAfter(chain);

        long lifetime = Math.max(0, chainNotAfter - chain[0].getNotBefore().getTime());

        window = (long)(lifetime * renewBeforeFraction);

        long at = Math.max(chainNotAfter - window, Math.max(provider.reloadOutcome().successTime() + minPause, U.currentTimeMillis()));

        schedule(at, planned);

        U.log(log, "TLS certificates will be renewed automatically [transports=" + provider.transports() +
            ", nextRenewal=" + Instant.ofEpochMilli(at) + ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ']');
    }

    /**
     * Attempts renewal and schedules a retry if the factory fails or returns unchanged certificates.
     *
     * @param planned Context in use when the attempt was planned.
     */
    private void attempt(SSLContext planned) {
        try {
            X509Certificate renewed = provider.renew(planned);

            if (renewed != null) {
                U.log(log, "TLS certificates reloaded [transports=" + provider.transports() + ", " +
                    SslCertificates.describe(renewed) + ", initiator=" + INITIATOR + ']');
            }
            else if (provider.context() == planned)
                retry(planned, null);
        }
        catch (Throwable e) {
            retry(planned, e);
        }
    }

    /**
     * Schedules a retry with exponential backoff and logs why renewal did not succeed. Exceptions are logged on every attempt.
     * Unchanged certificates are logged once a day, or sooner if an attempt finds they are close to expiry or expired.
     * Does nothing during shutdown, since stopping the node interrupts the current attempt.
     *
     * @param planned Context the attempt was planned for.
     * @param e Why the attempt failed, {@code null} if the factory returned the certificates in use.
     */
    private void retry(SSLContext planned, @Nullable Throwable e) {
        if (exec.isShutdown())
            return;

        long now = U.currentTimeMillis();

        pause = pause == 0 ? minPause : Math.min(pause * 2, maxPause);

        long at = now + pause + ThreadLocalRandom.current().nextLong(pause / 2 + 1);

        schedule(at, planned);

        boolean soon = now >= chainNotAfter - window / 2 || at >= chainNotAfter;
        int lateness = now >= chainNotAfter ? 2 : soon ? 1 : 0;

        if (e == null) {
            if (now - waitLogTime < WAIT_LOG_INTERVAL && lateness <= waitLoggedLateness)
                return;

            waitLogTime = now;
            waitLoggedLateness = lateness;
        }

        String msg = (e == null ? "The SSL context factory has no newer TLS certificates yet, the ones in use stay" :
            "Failed to reload TLS certificates, the ones in use stay") +
            (lateness == 2 ? ", though they have expired" : lateness == 1 ? " and expire soon" : "") +
            " [transports=" + provider.transports() +
            (e == null ? "" : ", initiator=" + INITIATOR + ", reason=" + SslCertificates.reason(e)) +
            ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ", nextRenewal=" + Instant.ofEpochMilli(at) + ']';

        if (soon)
            U.error(log, msg, e);
        else
            U.warn(log, msg, e);
    }

    /**
     * @param at Time of the next attempt.
     * @param planned Context the attempt is planned for.
     */
    private void schedule(long at, SSLContext planned) {
        if (next != null)
            next.cancel(false);

        nextRenewalTime = at;

        next = submit(() -> attempt(planned), Math.max(0, at - U.currentTimeMillis()));
    }

    /**
     * Runs the task in the renewal thread under the node's operation context. This prevents automatic renewal from inheriting
     * the security subject of an operator who ran {@code --ssl reload}.
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
