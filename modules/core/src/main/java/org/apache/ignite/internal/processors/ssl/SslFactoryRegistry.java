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
    /** */
    private static final long CHECK_PERIOD = HOURS.toMillis(1);

    /** */
    private final List<RegisteredFactory> factories = new CopyOnWriteArrayList<>();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private volatile ScheduledFuture<?> periodicCheck;

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

        boolean hotReload = false;

        for (RegisteredFactory registered : factories) {
            if (registered.supportsReload()) {
                registered.logHotReloadTime(registered.reloadable.getHotReloadTime());

                hotReload = true;
            }
        }

        if (!hotReload)
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        periodicCheck = executor.scheduleWithFixedDelay(new OperationContextAwareRunnable(this::checkFactories, null),
            0, CHECK_PERIOD, MILLISECONDS);
    }

    /** @return Delay until the next periodic check in milliseconds, or the full period while the periodic check runs. */
    private long nextPeriodicCheckDelay() {
        long delay = periodicCheck.getDelay(MILLISECONDS);

        return delay > 0 ? delay : CHECK_PERIOD;
    }

    /** @return Sum of consecutive automatic hot reload failures across factories. */
    private long consecutiveHotReloadFailures() {
        long sum = 0;

        for (RegisteredFactory registered : factories)
            sum += registered.failures.get();

        return sum;
    }

    /** Runs the periodic check of the factories that support reload. */
    private void checkFactories() {
        if (log.isDebugEnabled())
            log.debug("TLS hot reload check started");

        for (RegisteredFactory registered : factories) {
            if (registered.supportsReload())
                registered.check();
        }
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

        /** Check at the hot reload time if it comes before the next periodic check. */
        @Nullable private ScheduledFuture<?> dueCheck;

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
         * Checks the factory unless the registry has stopped, and schedules its next check at the hot reload time if
         * it comes before the next periodic check. Counts and logs a failed check, so it does not stop the other factories
         * or the schedule. A check that fails while the registry stops is logged at debug level and not counted.
         */
        private synchronized void check() {
            if (executor.isShutdown())
                return;

            try {
                reloadIfDue();
                scheduleDueCheck();
            }
            catch (Throwable e) {
                if (!executor.isShutdown()) {
                    failures.incrementAndGet();

                    log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt="
                        + new Date(System.currentTimeMillis() + nextPeriodicCheckDelay()) + ']', e);
                }
                else if (log.isDebugEnabled())
                    log.debug("TLS hot reload failed during node stop [transports=" + transports + ", err=" + e + ']');
            }
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
         * Schedules a check of this factory at the hot reload time if it comes before the next periodic check.
         * The check replaces the one scheduled before.
         */
        private void scheduleDueCheck() {
            if (dueCheck != null)
                dueCheck.cancel(false);

            dueCheck = null;

            long hotReloadTime = reloadable.getHotReloadTime();
            long delay = hotReloadTime - System.currentTimeMillis();

            if (hotReloadTime == 0 || delay >= nextPeriodicCheckDelay())
                return;

            try {
                dueCheck = executor.schedule(new OperationContextAwareRunnable(this::check, null), delay, MILLISECONDS);
            }
            catch (RejectedExecutionException ignored) {
                // The registry has stopped.
            }
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private static boolean due(long hotReloadTime) {
            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
        }

        /** Reloads a factory that supports reload. Waits for its running check and resets its failure count. */
        public synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            reloadable.init();

            failures.set(0);

            logHotReloadTime(reloadable.getHotReloadTime());

            if (periodicCheck != null)
                scheduleDueCheck();
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
