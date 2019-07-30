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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;

/**
 * Convenience adapter for cache managers.
 */
public class GridCacheSharedManagerAdapter<K, V> implements GridCacheSharedManager<K, V> {
    /** Last data version metric name. */
    public static final String LAST_DATA_VER = "LastDataVersion";

    /** */
    private static final String DIAGNOSTIC_LOG_CATEGORY = "org.apache.ignite.internal.diagnostic";

    /** Context. */
    protected GridCacheSharedContext<K, V> cctx;

    /** Last version metric. */
    protected AtomicLongMetric lastDataVer;

    /** Logger. */
    protected IgniteLogger log;

    /** Diagnostic logger. */
    protected IgniteLogger diagnosticLog;

    /** Starting flag. */
    private final AtomicBoolean starting = new AtomicBoolean(false);

    /** */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override public final void start(GridCacheSharedContext<K, V> cctx) throws IgniteCheckedException {
        if (!starting.compareAndSet(false, true))
            assert false : "Method start is called more than once for manager: " + this;

        assert cctx != null;

        this.cctx = cctx;

        log = cctx.logger(getClass());

        diagnosticLog = cctx.logger(DIAGNOSTIC_LOG_CATEGORY);

        MetricRegistry sysreg = cctx.kernalContext().metric().registry(CACHE_METRICS);

        lastDataVer = sysreg.longMetric(LAST_DATA_VER, "The latest data version on the node.");

        start0();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger log() {
        return log;
    }

    /**
     * @return Context.
     */
    protected GridCacheSharedContext<K, V> context() {
        return cctx;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void start0() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void stop(boolean cancel) {
        if (!starting.get() || !stop.compareAndSet(false, true))
            // Ignoring attempt to stop manager that has never been started.
            return;

        stop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @return {@code true} If this component is stopping.
     */
    protected final boolean isStopping() {
        return stop.get();
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stop0(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void onKernalStop(boolean cancel) {
        if (!starting.get())
            // Ignoring attempt to stop manager that has never been started.
            return;

        onKernalStop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(kernalStopInfo());
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void onKernalStop0(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReconnected(boolean active) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /**
     * @return Start info.
     */
    protected String startInfo() {
        return "Cache manager started.";
    }

    /**
     * @return Stop info.
     */
    protected String stopInfo() {
        return "Cache manager stopped.";
    }

    /**
     * @return Start info.
     */
    protected String kernalStartInfo() {
        return "Cache manager received onKernalStart() callback.";
    }

    /**
     * @return Stop info.
     */
    protected String kernalStopInfo() {
        return "Cache manager received onKernalStop() callback.";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSharedManagerAdapter.class, this);
    }
}
