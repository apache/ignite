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

package org.apache.ignite.internal.processors.ssl;

import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** */
    private final Map<String, Factory<SSLContext>> factories = new LinkedHashMap<>();

    /** */
    private volatile Map<Factory<SSLContext>, Renewal> renewals = Collections.emptyMap();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private final AtomicLong manualReloadAttempts = new AtomicLong();

    /** */
    private final AtomicLong automaticRenewalAttempts = new AtomicLong();

    /** */
    public synchronized void register(String transport, Factory<SSLContext> factory) {
        if (factories.putIfAbsent(transport, factory) != null)
            throw new IllegalStateException("SSL factory is already registered [transport=" + transport + ']');
    }

    /** @return Snapshot of active transport factories. */
    public synchronized Map<String, Factory<SSLContext>> factories() {
        return new LinkedHashMap<>(factories);
    }

    /** */
    public synchronized void start(GridKernalContext ctx) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveRenewalFailures", () -> renewals.values().stream().mapToLong(r -> r.failures.get()).sum(),
            "Sum of consecutive automatic certificate renewal failures across SSL factories.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual SSL factory reload attempts.");
        metrics.register("AutomaticRenewalAttempts", automaticRenewalAttempts::get, "Number of automatic SSL factory renewal attempts.");

        Map<String, Factory<SSLContext>> factories = factories();
        Map<Factory<SSLContext>, Renewal> renewals = new IdentityHashMap<>();
        IgniteLogger log = ctx.log(getClass());

        for (Factory<SSLContext> factory : factories.values()) {
            if (factory instanceof SslContextFactory sslFactory && sslFactory.isRenewalEnabled() && !renewals.containsKey(factory)) {
                if (sslFactory.getRenewalTime() == 0)
                    sslFactory.init();

                String transports = factories.entrySet().stream()
                    .filter(entry -> entry.getValue() == factory)
                    .map(Map.Entry::getKey)
                    .collect(joining(", "));

                renewals.put(factory, new Renewal(sslFactory, transports, log));
            }
        }

        if (renewals.isEmpty() || ctx.isStopping())
            return;

        executor = newSingleThreadScheduledExecutor("ssl-renewal", ctx.igniteInstanceName());

        try (Scope ignored = OperationContext.restoreSnapshot(null)) {
            executor.execute(() -> renewals.values().forEach(Renewal::scheduleRenewal));
        }

        this.renewals = renewals;
    }

    /** */
    public synchronized void stop() {
        ScheduledExecutorService executor = this.executor;

        if (executor == null)
            return;

        executor.shutdownNow();

        boolean interrupted = false;

        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(Long.MAX_VALUE, MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Reloads the factory and resets its automatic renewal schedule and failure count.
     *
     * @param factory Factory to reload.
     */
    public void reload(AbstractSslContextFactory factory) {
        manualReloadAttempts.incrementAndGet();

        factory.init();

        Renewal renewal = renewals.get(factory);

        if (renewal != null)
            executor.execute(renewal::scheduleRenewal);
    }

    /** */
    private class Renewal implements Runnable {
        /** */
        private final SslContextFactory factory;

        /** */
        private final String transports;

        /** */
        private final IgniteLogger log;

        /** */
        private ScheduledFuture<?> pending;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private Renewal(SslContextFactory factory, String transports, IgniteLogger log) {
            this.factory = factory;
            this.transports = transports;
            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (executor.isShutdown())
                return;

            automaticRenewalAttempts.incrementAndGet();

            try {
                factory.renew();
            }
            catch (Exception e) {
                retry(e);

                return;
            }

            long renewalTime = factory.getRenewalTime();

            if (renewalTime != 0 && renewalTime <= System.currentTimeMillis())
                retry(new IgniteException("Loaded certificates are still due for renewal [renewalTime=" + new Date(renewalTime) + ']'));
            else
                scheduleRenewal();
        }

        /** */
        private void retry(Exception cause) {
            if (executor.isShutdown())
                return;

            long delay = Math.min(MINUTES.toMillis(1) << Math.min(failures.incrementAndGet() - 1, 6), MINUTES.toMillis(60));
            long nextAttempt = System.currentTimeMillis() + delay + ThreadLocalRandom.current().nextLong(delay / 2 + 1);

            log.warning("TLS renewal failed [transports=" + transports + ", nextAttempt=" + new Date(nextAttempt) + ']', cause);

            schedule(nextAttempt);
        }

        /** Ends the failure streak and schedules the next attempt at the renewal time. */
        private void scheduleRenewal() {
            failures.set(0);
            schedule(factory.getRenewalTime());
        }

        /** */
        private void schedule(long time) {
            if (pending != null)
                pending.cancel(false);

            if (time == 0)
                return;

            try (Scope ignored = OperationContext.restoreSnapshot(null)) {
                pending = executor.schedule(this, Math.max(0, time - System.currentTimeMillis()), MILLISECONDS);
            }
            catch (RejectedExecutionException e) {
                if (!executor.isShutdown())
                    throw e;

                return;
            }

            if (log.isInfoEnabled())
                log.info("TLS renewal scheduled [transports=" + transports + ", nextAttempt=" + new Date(time) + ']');
        }
    }
}
