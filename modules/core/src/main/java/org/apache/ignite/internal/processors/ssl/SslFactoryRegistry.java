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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
import org.apache.ignite.ssl.AbstractSslContextFactory;
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
    private final AtomicLong manualReloadAttempts = new AtomicLong();

    /** */
    private final AtomicLong automaticHotReloadAttempts = new AtomicLong();

    /** @param ctx Kernal context. */
    public SslFactoryRegistry(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Registers the factory of an Ignite transport. The registry can reload the factory if it extends
     * {@link AbstractSslContextFactory}.
     */
    public void register(String transport, Factory<SSLContext> factory) {
        add(transport, factory, factory instanceof AbstractSslContextFactory sslFactory ? new ReloadableSslFactory() {
            /** {@inheritDoc} */
            @Override public void reload() {
                sslFactory.init();
            }

            /** {@inheritDoc} */
            @Override public long hotReloadTime() {
                return sslFactory.getHotReloadTime();
            }
        } : null);
    }

    /** Registers the TLS factory of a transport that does not use an Ignite SSL context factory, such as HTTP REST. */
    public void register(String transport, ReloadableSslFactory factory) {
        add(transport, factory, factory);
    }

    /** */
    private synchronized void add(String transport, Object factory, @Nullable ReloadableSslFactory reloadable) {
        for (int i = 0; i < factories.size(); i++) {
            RegisteredFactory registered = factories.get(i);

            if (registered.factory == factory) {
                factories.set(i, new RegisteredFactory(factory, registered.reloadable, registered.transports + ", " + transport));

                return;
            }
        }

        factories.add(new RegisteredFactory(factory, reloadable, transport));
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
                registered.logHotReloadTime(registered.reloadable.hotReloadTime());

                hotReload = true;
            }
        }

        if (!hotReload)
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        executor.scheduleWithFixedDelay(new OperationContextAwareRunnable(this::checkFactories, null),
            0, CHECK_PERIOD, MILLISECONDS);
    }

    /** @return Sum of consecutive automatic hot reload failures across factories. */
    private long consecutiveHotReloadFailures() {
        long sum = 0;

        for (RegisteredFactory registered : factories)
            sum += registered.failures.get();

        return sum;
    }

    /** Runs the hourly check of the factories that support reload. */
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

    /** Registered factory with its transports and hot reload state. The hourly check and manual reload hold its monitor. */
    public class RegisteredFactory {
        /** */
        private final Object factory;

        /** Reload operations of the factory, or {@code null} if the registry cannot reload it. */
        @Nullable private final ReloadableSslFactory reloadable;

        /** */
        private final String transports;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private RegisteredFactory(Object factory, @Nullable ReloadableSslFactory reloadable, String transports) {
            this.factory = factory;
            this.reloadable = reloadable;
            this.transports = transports;
        }

        /** @return The factory that the transports registered. */
        public Object factory() {
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
         * Checks the factory unless the registry has stopped. Counts and logs a failed check, so it does not stop
         * the other factories or the schedule. A check that fails while the registry stops is logged at debug level
         * and not counted.
         */
        private synchronized void check() {
            if (executor.isShutdown())
                return;

            try {
                reloadIfDue();
            }
            catch (Throwable e) {
                if (!executor.isShutdown()) {
                    failures.incrementAndGet();

                    log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt="
                        + new Date(System.currentTimeMillis() + CHECK_PERIOD) + ']', e);
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
            if (!due(reloadable.hotReloadTime())) {
                failures.set(0);

                return;
            }

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            reloadable.reload();

            long hotReloadTime = reloadable.hotReloadTime();

            if (due(hotReloadTime)) {
                throw new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']');
            }

            failures.set(0);

            if (log.isInfoEnabled())
                log.info("TLS hot reload succeeded [transports=" + transports + ']');

            logHotReloadTime(hotReloadTime);
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private static boolean due(long hotReloadTime) {
            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
        }

        /** Reloads a factory that supports reload. Waits for its running check and resets its failure count. */
        public synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            reloadable.reload();

            failures.set(0);

            logHotReloadTime(reloadable.hotReloadTime());
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
