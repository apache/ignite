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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry extends GridProcessorAdapter {
    /** Longest interval between two checks of a factory, and the delay of a retry after a failed check. */
    private static final long CHECK_PERIOD = HOURS.toMillis(1);

    /** */
    private final List<RegisteredFactory> factories = new CopyOnWriteArrayList<>();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private final AtomicLong manualReloadAttempts = new AtomicLong();

    /** */
    private final AtomicLong automaticHotReloadAttempts = new AtomicLong();

    /** @param ctx Kernal context. */
    public SslFactoryRegistry(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param transport Transport name.
     * @param factory SSL context factory of the transport. The registry reloads it if it implements
     *     {@link ReloadableSslFactory}.
     */
    public synchronized void register(String transport, Factory<SSLContext> factory) {
        for (int i = 0; i < factories.size(); i++) {
            RegisteredFactory registered = factories.get(i);

            if (registered.factory == factory) {
                factories.set(i, new RegisteredFactory(factory, registered.transports + ", " + transport));

                return;
            }
        }

        factories.add(new RegisteredFactory(factory, transport));
    }

    /** @return Registered factories in registration order. */
    public List<RegisteredFactory> factories() {
        return List.copyOf(factories);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveHotReloadFailures", this::consecutiveHotReloadFailures,
            "Number of consecutive automatic hot reload failures.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual reload attempts.");
        metrics.register("AutomaticHotReloadAttempts", automaticHotReloadAttempts::get,
            "Number of automatic hot reload attempts.");

        for (RegisteredFactory registered : factories) {
            if (!registered.supportsReload())
                continue;

            if (executor == null)
                executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

            registered.logHotReloadTime(registered.reloadable.getHotReloadTime());
            registered.scheduleCheck(0);
        }
    }

    /** @return Sum of consecutive automatic hot reload failures across factories. */
    private long consecutiveHotReloadFailures() {
        long sum = 0;

        for (RegisteredFactory registered : factories)
            sum += registered.failures.get();

        return sum;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (executor != null)
            executor.shutdownNow();
    }

    /** Registered factory with its transports and hot reload state. Its checks and manual reload hold its monitor. */
    public class RegisteredFactory {
        /** */
        private final Factory<SSLContext> factory;

        /** The factory if the registry can reload it, {@code null} otherwise. */
        @Nullable private final ReloadableSslFactory reloadable;

        /** */
        private final String transports;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        @Nullable private ScheduledFuture<?> nextCheck;

        /** */
        private RegisteredFactory(Factory<SSLContext> factory, String transports) {
            this.factory = factory;
            this.transports = transports;

            reloadable = factory instanceof ReloadableSslFactory sslFactory ? sslFactory : null;
        }

        /** @return The factory that the transports registered. */
        public Factory<SSLContext> factory() {
            return factory;
        }

        /** @return Transports that use the factory, joined by commas. */
        public String transports() {
            return transports;
        }

        /** @return Whether the registry can reload the factory. */
        public boolean supportsReload() {
            return reloadable != null;
        }

        /**
         * Checks the factory unless the registry has stopped, and schedules its next check. Counts and logs a failed
         * check and retries it after {@link #CHECK_PERIOD}, so a failure does not stop the checks. A check that fails
         * while the registry stops is logged at debug level and not counted.
         */
        private synchronized void check() {
            if (executor.isShutdown())
                return;

            if (log.isDebugEnabled())
                log.debug("TLS hot reload check started [transports=" + transports + ']');

            long nextCheckDelay;

            try {
                reloadIfDue();

                nextCheckDelay = nextCheckDelay();
            }
            catch (Throwable e) {
                if (executor.isShutdown()) {
                    if (log.isDebugEnabled())
                        log.debug("TLS hot reload failed during node stop [transports=" + transports + ", err=" + e + ']');

                    return;
                }

                failures.incrementAndGet();

                nextCheckDelay = CHECK_PERIOD;

                log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt="
                    + new Date(System.currentTimeMillis() + nextCheckDelay) + ']', e);
            }

            scheduleCheck(nextCheckDelay);
        }

        /**
         * Reloads the factory if its hot reload time has come, and ends the failure streak if it has not.
         *
         * @throws IgniteException If the reload fails or leaves certificates that are still due.
         */
        private void reloadIfDue() {
            if (!due(reloadable.getHotReloadTime())) {
                failures.set(0);

                return;
            }

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            reloadable.init();

            long hotReloadTime = reloadable.getHotReloadTime();

            if (due(hotReloadTime)) {
                throw new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']');
            }

            failures.set(0);

            if (log.isInfoEnabled())
                log.info("TLS hot reload succeeded [transports=" + transports + ']');

            logHotReloadTime(hotReloadTime);
        }

        /**
         * @return Delay until the hot reload time, but no longer than {@link #CHECK_PERIOD}. Certificates without a hot
         *     reload time, or that are already due, are checked again after the full period.
         */
        private long nextCheckDelay() {
            long delay = reloadable.getHotReloadTime() - System.currentTimeMillis();

            return delay > 0 ? Math.min(delay, CHECK_PERIOD) : CHECK_PERIOD;
        }

        /** Replaces the scheduled check of the factory with one after the given delay, once the registry has started. */
        private synchronized void scheduleCheck(long delay) {
            if (executor == null)
                return;

            if (nextCheck != null)
                nextCheck.cancel(false);

            try {
                nextCheck = executor.schedule(new OperationContextAwareRunnable(this::check, null), delay, MILLISECONDS);
            }
            catch (RejectedExecutionException ignored) {
                // The registry has stopped.
            }
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private static boolean due(long hotReloadTime) {
            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
        }

        /**
         * Reloads a factory that supports reload. Waits for its running check, resets its failure count and schedules
         * its next check for the loaded certificates.
         */
        public synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            reloadable.init();

            failures.set(0);

            logHotReloadTime(reloadable.getHotReloadTime());

            scheduleCheck(nextCheckDelay());
        }

        /** */
        private void logHotReloadTime(long hotReloadTime) {
            if (hotReloadTime == 0) {
                log.warning("TLS hot reload will not run: no private key with an X.509 certificate chain was found [transports="
                    + transports + ']');
            }
            else if (log.isInfoEnabled())
                log.info("TLS hot reload due [transports=" + transports + ", hotReloadTime=" + new Date(hotReloadTime) + ']');
        }
    }
}
