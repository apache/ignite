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
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslBundle;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.ssl.AbstractSslContextFactory.NODE_ACCESS;

/**
 * Renews one factory's certificates before expiry when {@link AbstractSslContextFactory#setRenewalEnabled(boolean) renewal is enabled}.
 * Renewal planning and attempts run in the node's renewal thread.
 */
public class SslRenewal {
    /** How the node log names an automatic renewal in {@code initiator=}. */
    private static final String INITIATOR = "automatic renewal";

    /** Initial retry pause and minimum interval between renewal attempts, in milliseconds. */
    private static final long MIN_PAUSE = TimeUnit.MINUTES.toMillis(1);

    /** Maximum retry pause before adding jitter, in milliseconds. */
    private static final long MAX_PAUSE = TimeUnit.HOURS.toMillis(1);

    /** */
    private final AbstractSslContextFactory factory;

    /** Transports using this factory, including those registered after renewal starts. */
    private final Supplier<String> transports;

    /** Share of the certificate lifetime left when the renewal window opens. */
    private final double renewBeforeFraction;

    /** */
    private final ScheduledExecutorService exec;

    /** */
    private final IgniteLogger log;

    /** Current time in milliseconds. */
    private final LongSupplier currentTimeMillis;

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

    /**
     * @param factory Factory that owns the accepted material.
     * @param renewBeforeFraction Share of the certificate lifetime left when the renewal window opens.
     * @param exec Renewal thread.
     * @param log Logger.
     * @param transports Names of the transports currently using this factory.
     */
    public SslRenewal(
        AbstractSslContextFactory factory,
        double renewBeforeFraction,
        ScheduledExecutorService exec,
        IgniteLogger log,
        Supplier<String> transports
    ) {
        this(factory, renewBeforeFraction, exec, log, transports, System::currentTimeMillis);
    }

    /**
     * @param factory Factory that owns the accepted material.
     * @param renewBeforeFraction Share of the certificate lifetime left when the renewal window opens.
     * @param exec Renewal thread.
     * @param log Logger.
     * @param transports Names of the transports currently using this factory.
     * @param currentTimeMillis Current time in milliseconds.
     */
    SslRenewal(
        AbstractSslContextFactory factory,
        double renewBeforeFraction,
        ScheduledExecutorService exec,
        IgniteLogger log,
        Supplier<String> transports,
        LongSupplier currentTimeMillis
    ) {
        this.factory = factory;
        this.transports = transports;
        this.renewBeforeFraction = renewBeforeFraction;
        this.exec = exec;
        this.log = log;
        this.currentTimeMillis = currentTimeMillis;
    }

    /** @return Time of the next automatic renewal, or {@code 0} if none is planned or the node has stopped. */
    public long nextRenewalTime() {
        return exec.isShutdown() ? 0 : nextRenewalTime;
    }

    /** Plans the next renewal in the renewal thread. */
    public void replan() {
        submit(this::plan, 0);
    }

    /** Plans renewal from the first chain to expire, with the earliest window breaking ties. */
    private void plan() {
        pause = 0;

        SslBundle planned = NODE_ACCESS.current(factory);
        List<X509Certificate> chain = SslCertificates.renewalChain(planned);

        chainNotAfter = SslCertificates.chainNotAfter(chain);

        long lifetime = Math.max(0, chainNotAfter - chain.get(0).getNotBefore().getTime());

        window = (long)(lifetime * renewBeforeFraction);

        long at = Math.max(chainNotAfter - window,
            Math.max(NODE_ACCESS.reloadOutcome(factory).successTime() + MIN_PAUSE, currentTimeMillis.getAsLong()));

        schedule(at, planned.context());

        CommonUtils.log(log, "TLS certificates will be renewed automatically [transports=" + transports.get() +
            ", nextRenewal=" + Instant.ofEpochMilli(at) + ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ']');
    }

    /**
     * Attempts renewal and schedules a retry if the factory fails or returns unchanged certificates.
     *
     * @param planned Context in use when the attempt was planned.
     */
    private void attempt(SSLContext planned) {
        try {
            SslBundle renewed = NODE_ACCESS.renew(factory, planned);

            if (renewed != null) {
                CommonUtils.log(log, "TLS certificates reloaded [transports=" + transports.get() + ", " +
                    SslCertificates.describeChains(renewed.certificateChains()) + ", initiator=" + INITIATOR + ']');
            }
            else if (factory.create() == planned)
                retry(planned, null);
        }
        catch (Throwable e) {
            retry(planned, e);
        }
    }

    /**
     * Schedules a retry with exponential backoff and logs why renewal did not succeed.
     * Does nothing during shutdown, since stopping the node interrupts the current attempt.
     *
     * @param planned Context the attempt was planned for.
     * @param e Why the attempt failed, {@code null} if the factory returned the certificates in use.
     */
    private void retry(SSLContext planned, @Nullable Throwable e) {
        if (exec.isShutdown())
            return;

        long now = currentTimeMillis.getAsLong();

        pause = pause == 0 ? MIN_PAUSE : Math.min(pause * 2, MAX_PAUSE);

        long at = now + pause + ThreadLocalRandom.current().nextLong(pause / 2 + 1);

        schedule(at, planned);

        boolean soon = now >= chainNotAfter - window / 2 || at >= chainNotAfter;

        String msg = (e == null ? "The SSL context factory has no newer TLS certificates yet, the ones in use stay" :
            "Failed to reload TLS certificates, the ones in use stay") +
            (now >= chainNotAfter ? ", though they have expired" : soon ? " and expire soon" : "") +
            " [transports=" + transports.get() +
            (e == null ? "" : ", initiator=" + INITIATOR + ", reason=" + SslCertificates.reason(e)) +
            ", chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter) + ", nextRenewal=" + Instant.ofEpochMilli(at) + ']';

        if (soon)
            CommonUtils.error(log, msg, e);
        else
            CommonUtils.warn(log, msg, e);
    }

    /**
     * @param at Time of the next attempt.
     * @param planned Context the attempt is planned for.
     */
    private void schedule(long at, SSLContext planned) {
        if (next != null)
            next.cancel(false);

        nextRenewalTime = at;

        next = submit(() -> attempt(planned), Math.max(0, at - currentTimeMillis.getAsLong()));

        if (next == null)
            nextRenewalTime = 0;
    }

    /**
     * Runs the renewal task with an empty operation context. This prevents automatic renewal from inheriting
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
