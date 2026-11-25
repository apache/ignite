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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
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

    /** @return Registered factories in registration order, each with its transports joined by commas. */
    public List<Entry<Factory<SSLContext>, String>> factories() {
        List<Entry<Factory<SSLContext>, String>> res = new ArrayList<>(factories.size());

        for (RegisteredFactory registered : factories)
            res.add(Map.entry(registered.factory, registered.transports));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveHotReloadFailures", this::consecutiveHotReloadFailures,
            "Number of consecutive automatic hot reload failures.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual reload attempts.");
        metrics.register("AutomaticHotReloadAttempts", automaticHotReloadAttempts::get,
            "Number of automatic hot reload attempts.");

        if (!ctx.config().isSslHotReloadEnabled())
            return;

        for (RegisteredFactory registered : factories) {
            if (registered.reloadable == null)
                continue;

            if (executor == null)
                executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

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

    /**
     * Reloads a registered factory. If hot reload is enabled, waits for the running check of the factory, resets its failure
     * count and schedules its next check for the loaded certificates. If they are already due, the next check comes after
     * {@link #CHECK_PERIOD}, because a hot reload right away would load the same certificates from the source.
     *
     * @param factory Registered factory to reload.
     */
    public void reload(ReloadableSslFactory factory) {
        for (RegisteredFactory registered : factories) {
            if (registered.factory == factory)
                registered.reload();
        }
    }

    /** Registered factory with its transports and hot reload state. Its checks and manual reload hold its monitor. */
    private class RegisteredFactory {
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

        /** Hot reload time that the registry logged last, or {@code -1} before the first log. */
        private long loggedHotReloadTime = -1;

        /** */
        private RegisteredFactory(Factory<SSLContext> factory, String transports) {
            this.factory = factory;
            this.transports = transports;

            reloadable = factory instanceof ReloadableSslFactory sslFactory ? sslFactory : null;
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

            long nextCheckDelay = CHECK_PERIOD;

            try {
                nextCheckDelay = reloadIfDue();
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

            scheduleCheck(nextCheckDelay);
        }

        /**
         * Reloads the factory if its hot reload time has come, and ends the failure streak if it has not.
         *
         * @return Delay of the next check.
         * @throws IgniteException If the reload fails or leaves certificates that are still due.
         */
        private long reloadIfDue() {
            long hotReloadTime = hotReloadTime();

            logHotReloadTime(hotReloadTime);

            long nextCheckDelay = nextCheckDelay(hotReloadTime);

            if (nextCheckDelay != 0) {
                failures.set(0);

                return nextCheckDelay;
            }

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            reloadable.init();

            hotReloadTime = hotReloadTime();
            nextCheckDelay = nextCheckDelay(hotReloadTime);

            if (nextCheckDelay == 0) {
                throw new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']');
            }

            failures.set(0);

            if (log.isInfoEnabled())
                log.info("TLS hot reload succeeded [transports=" + transports + ']');

            logHotReloadTime(hotReloadTime);

            return nextCheckDelay;
        }

        /**
         * @return Time when the loaded certificates are due for hot reload: {@link IgniteConfiguration#getSslHotReloadBeforeExpiry()}
         *     before they expire, or {@code 0} if the factory gives no private key with an X.509 certificate chain.
         */
        private long hotReloadTime() {
            long expiry = reloadable.certificatesExpiry();

            return expiry == 0 ? 0 : expiry - ctx.config().getSslHotReloadBeforeExpiry();
        }

        /**
         * @param hotReloadTime Hot reload time of the loaded certificates, or {@code 0} if they have none.
         * @return Delay until the hot reload time, but no longer than {@link #CHECK_PERIOD}, or {@code 0} if the time has
         *     come. Certificates without a hot reload time are checked again after the full period.
         */
        private static long nextCheckDelay(long hotReloadTime) {
            if (hotReloadTime == 0)
                return CHECK_PERIOD;

            return Math.max(0, Math.min(hotReloadTime - System.currentTimeMillis(), CHECK_PERIOD));
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

        /** */
        private synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            reloadable.init();

            if (!ctx.config().isSslHotReloadEnabled())
                return;

            failures.set(0);

            long hotReloadTime = hotReloadTime();

            logHotReloadTime(hotReloadTime);

            long nextCheckDelay = nextCheckDelay(hotReloadTime);

            scheduleCheck(nextCheckDelay == 0 ? CHECK_PERIOD : nextCheckDelay);
        }

        /** Logs the hot reload time of the loaded certificates if it differs from the one logged last. */
        private void logHotReloadTime(long hotReloadTime) {
            if (hotReloadTime == loggedHotReloadTime)
                return;

            loggedHotReloadTime = hotReloadTime;

            if (hotReloadTime == 0) {
                log.warning("TLS hot reload will not run: no private key with an X.509 certificate chain was found [transports="
                    + transports + ']');
            }
            else if (log.isInfoEnabled())
                log.info("TLS hot reload due [transports=" + transports + ", hotReloadTime=" + new Date(hotReloadTime) + ']');
        }
    }
}
