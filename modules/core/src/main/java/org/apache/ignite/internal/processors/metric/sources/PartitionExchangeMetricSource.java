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

package org.apache.ignite.internal.processors.metric.sources;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.TestOnly;

/**
 * Metric source for partition map exchange metrics.
 */
public class PartitionExchangeMetricSource extends AbstractMetricSource<PartitionExchangeMetricSource.Holder> {
    /** Partition map exchange metrics prefix. */
    public static final String PME_METRICS = "pme";

    /** PME duration metric name. */
    public static final String PME_DURATION = "Duration";

    /** PME cache operations blocked duration metric name. */
    public static final String PME_OPS_BLOCKED_DURATION = "CacheOperationsBlockedDuration";

    /** Histogram of PME durations metric name. */
    public static final String PME_DURATION_HISTOGRAM = "DurationHistogram";

    /** Histogram of blocking PME durations metric name. */
    public static final String PME_OPS_BLOCKED_DURATION_HISTOGRAM = "CacheOperationsBlockedDurationHistogram";

    /**
     * Cretates partition map exchange metric source.
     *
     * @param ctx Kernal context.
     */
    public PartitionExchangeMetricSource(GridKernalContext ctx) {
        super(PME_METRICS, ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.pmeDuration = bldr.register(PME_DURATION,
                () -> currentPMEDuration(false),
                "Current PME duration in milliseconds.");

        hldr.blockingPmeDuration = bldr.register(PME_OPS_BLOCKED_DURATION,
                () -> currentPMEDuration(true),
                "Current PME cache operations blocked duration in milliseconds.");

        long[] pmeBounds = new long[] {500, 1000, 5000, 30000};

        hldr.durationHistogram = bldr.histogram(PME_DURATION_HISTOGRAM, pmeBounds,
                "Histogram of PME durations in milliseconds.");

        hldr.blockingDurationHistogram = bldr.histogram(PME_OPS_BLOCKED_DURATION_HISTOGRAM, pmeBounds,
                "Histogram of cache operations blocked PME durations in milliseconds.");
    }

    /**
     * Returns execution duration for current partition map exchange in milliseconds.
     *
     * @return Execution duration for current partition map exchange in milliseconds.
     * {@code 0} if there is no running PME.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long currentPmeDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.pmeDuration.value() : -1;
    }

    /**
     * Returns duration for current blocking partition map exchange in milliseconds.
     *
     * @return Duration for current blocking partition map exchange in milliseconds.
     * {@code 0} if there is no running PME.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long currentBlockingPmeDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.blockingPmeDuration.value() : -1;
    }

    /**
     * Updates the {@link #PME_OPS_BLOCKED_DURATION_HISTOGRAM} and {@link #PME_DURATION_HISTOGRAM} metrics if needed.
     *
     * @param duration The total duration of the current PME.
     */
    public void updateDurationHistogram(long duration, boolean affChanged) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.durationHistogram.value(duration);

            if (affChanged)
                hldr.blockingDurationHistogram.value(duration);
        }
    }

    /** For tests. */
    @TestOnly
    public HistogramMetric durationHistogram() {
        Holder hldr = holder();

        return hldr.durationHistogram;
    }

    /** For tests. */
    @TestOnly
    public HistogramMetric blockingDurationHistogram() {
        Holder hldr = holder();

        return hldr.blockingDurationHistogram;
    }

    /**
     * @param blocked {@code True} if take into account only cache operations blocked PME.
     * @return Gets execution duration for current partition map exchange in milliseconds. {@code 0} If there is no
     * running PME or {@code blocked} was set to {@code true} and current PME don't block cache operations.
     */
    private long currentPMEDuration(boolean blocked) {
        GridDhtPartitionsExchangeFuture fut = ctx().cache().context().exchange().lastTopologyFuture();

        return fut == null ? 0 : fut.currentPMEDuration(blocked);
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Histogram of PME durations. */
        private HistogramMetricImpl durationHistogram;

        /** Histogram of blocking PME durations. */
        private HistogramMetricImpl blockingDurationHistogram;

        /** Current PME duration in milliseconds. */
        private LongMetric pmeDuration;

        /** */
        private LongMetric blockingPmeDuration;
    }
}
