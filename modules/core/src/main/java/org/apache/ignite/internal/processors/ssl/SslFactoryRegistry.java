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

import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** Delay after a successful load whose renewal window has already started. */
    private static final long RENEWAL_RETRY_DELAY = HOURS.toMillis(1);

    /** */
    private final Map<String, Factory<SSLContext>> factories = new LinkedHashMap<>();

    /** */
    private final Map<Factory<SSLContext>, Renewal> renewals = new IdentityHashMap<>();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong consecutiveFailures = new AtomicLong();

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
        this.ctx = ctx;
        log = ctx.log(getClass());
        ctx.metric().registry("ssl").register("ConsecutiveRenewalFailures", consecutiveFailures::get,
            "Sum of consecutive automatic certificate renewal failures across SSL factories.");

        Map<String, Factory<SSLContext>> factories = factories();

        for (Factory<SSLContext> factory : factories.values()) {
            if (factory instanceof SslContextFactory && ((SslContextFactory)factory).isRenewalEnabled() && !renewals.containsKey(factory)) {
                if (ctx.isStopping())
                    return;

                if (((SslContextFactory)factory).getRenewalTime() == 0)
                    ((SslContextFactory)factory).init();

                String transports = factories.entrySet().stream()
                    .filter(entry -> entry.getValue() == factory)
                    .map(Map.Entry::getKey)
                    .collect(joining(", "));

                renewals.put(factory, new Renewal((SslContextFactory)factory, transports));
            }
        }

        if (renewals.isEmpty() || ctx.isStopping())
            return;

        executor = newSingleThreadScheduledExecutor("ssl-renewal", ctx.igniteInstanceName());

        try (Scope ignored = OperationContext.restoreSnapshot(null)) {
            executor.execute(() -> renewals.values().forEach(Renewal::schedule));
        }
    }

    /** */
    public synchronized void stop() {
        ScheduledExecutorService executor = this.executor;

        if (executor == null)
            return;

        for (Runnable task : executor.shutdownNow()) {
            if (task instanceof Future)
                ((Future<?>)task).cancel(false);
        }

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

    /** Reloads the factory and, on success, resets its automatic renewal schedule and failure count. */
    public void reload(AbstractSslContextFactory factory) throws IgniteCheckedException {
        Future<?> reload;

        synchronized (this) {
            Renewal renewal = executor == null ? null : renewals.get(factory);

            reload = renewal == null ? null : executor.submit(renewal::reload);
        }

        if (reload == null)
            factory.init();
        else {
            try {
                U.get(reload);
            }
            catch (IgniteInterruptedCheckedException e) {
                reload.cancel(true);

                throw e;
            }
        }
    }

    /** */
    private boolean stopping() {
        return ctx.isStopping() || executor.isShutdown();
    }

    /** */
    private class Renewal implements Runnable {
        /** */
        private final SslContextFactory factory;

        /** */
        private final String transports;

        /** */
        private ScheduledFuture<?> pending;

        /** */
        private int failures;

        /** */
        private Renewal(SslContextFactory factory, String transports) {
            this.factory = factory;
            this.transports = transports;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (stopping())
                return;

            Exception failure = null;
            long nextAttempt = 0;

            try {
                factory.renew();
                nextAttempt = factory.getRenewalTime();
            }
            catch (Exception e) {
                failure = e;
            }

            if (stopping())
                return;

            if (failure == null) {
                resetFailures();
                nextAttempt = nextAttemptTime(nextAttempt);
            }
            else {
                failures++;
                consecutiveFailures.incrementAndGet();

                long delay = Math.min(MINUTES.toMillis(1) << Math.min(failures - 1, 6), MINUTES.toMillis(60));

                nextAttempt = System.currentTimeMillis() + delay + ThreadLocalRandom.current().nextLong(delay / 2 + 1);

                log.warning("TLS renewal failed [transports=" + transports + ", nextAttempt=" + new Date(nextAttempt) + ']', failure);
            }

            schedule(nextAttempt);
        }

        /** */
        private void reload() {
            factory.init();

            resetFailures();
            schedule(nextAttemptTime(factory.getRenewalTime()));
        }

        /** */
        private long nextAttemptTime(long renewalTime) {
            long now = System.currentTimeMillis();

            if (renewalTime == 0 || renewalTime > now)
                return renewalTime;

            long nextAttempt = now + RENEWAL_RETRY_DELAY;

            log.warning("TLS certificates are already in their renewal window [transports=" + transports +
                ", nextAttempt=" + new Date(nextAttempt) + ']');

            return nextAttempt;
        }

        /** */
        private void resetFailures() {
            consecutiveFailures.addAndGet(-failures);
            failures = 0;
        }

        /** */
        private void schedule() {
            schedule(factory.getRenewalTime());
        }

        /** */
        private void schedule(long time) {
            if (stopping())
                return;

            if (pending != null)
                pending.cancel(false);

            if (time == 0)
                return;

            try (Scope ignored = OperationContext.restoreSnapshot(null)) {
                pending = executor.schedule(this, Math.max(0, time - System.currentTimeMillis()), MILLISECONDS);
            }
            catch (RejectedExecutionException e) {
                if (!stopping())
                    throw e;

                return;
            }

            if (log.isInfoEnabled())
                log.info("TLS renewal scheduled [transports=" + transports + ", nextAttempt=" + new Date(time) + ']');
        }
    }
}
