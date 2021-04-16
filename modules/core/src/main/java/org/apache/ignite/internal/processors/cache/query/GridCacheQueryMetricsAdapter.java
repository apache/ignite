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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Adapter for {@link QueryMetrics}.
 */
public class GridCacheQueryMetricsAdapter implements QueryMetrics {
    /** Minimum time of execution. */
    private final AtomicLongMetric minTime;

    /** Maximum time of execution. */
    private final AtomicLongMetric maxTime;

    /** Sum of execution time for all completed queries. */
    private final LongAdderMetric sumTime;

    /** Number of executions. */
    private final LongAdderMetric execs;

    /** Number of completed executions. */
    private final LongAdderMetric completed;

    /** Number of fails. */
    private final LongAdderMetric fails;

    /**
     * @param mmgr Metrics manager.
     * @param cacheName Cache name.
     * @param isNear Is near flag.
     */
    public GridCacheQueryMetricsAdapter(GridMetricManager mmgr, String cacheName, boolean isNear) {
        MetricRegistry mreg = mmgr.registry(MetricUtils.cacheMetricsRegistryName(cacheName, isNear));

        minTime = mreg.longMetric("QueryMinimalTime", null);
        minTime.value(Long.MAX_VALUE);

        maxTime = mreg.longMetric("QueryMaximumTime", null);
        sumTime = mreg.longAdderMetric("QuerySumTime", null);
        execs = mreg.longAdderMetric("QueryExecuted", null);
        completed = mreg.longAdderMetric("QueryCompleted", null);
        fails = mreg.longAdderMetric("QueryFailed", null);
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        long min = minTime.value();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime.value();
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        double val = completed.value();

        return val > 0 ? sumTime.value() / val : 0.0;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return (int)execs.value();
    }

    /** {@inheritDoc} */
    @Override public int fails() {
        return (int)fails.value();
    }

    /**
     * Update metrics.
     *
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void update(long duration, boolean fail) {
        if (fail) {
            execs.increment();
            fails.increment();
        }
        else {
            execs.increment();
            completed.increment();

            MetricUtils.setIfLess(minTime, duration);
            MetricUtils.setIfGreater(maxTime, duration);

            sumTime.add(duration);
        }
    }

    /** @return Current metrics values. */
    public QueryMetrics snapshot() {
        long minTimeVal = minTime.value();

        return new QueryMetricsSnapshot(
            minTimeVal == Long.MAX_VALUE ? 0 : minTimeVal,
            maxTime.value(),
            averageTime(),
            (int)execs.value(),
            (int)fails.value());
    }

    /** Resets query metrics. */
    public void reset() {
        minTime.value(Long.MAX_VALUE);
        maxTime.reset();
        sumTime.reset();
        execs.reset();
        completed.reset();
        fails.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryMetricsAdapter.class, this);
    }

    /** Query metrics snapshot. */
    public static class QueryMetricsSnapshot implements QueryMetrics, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Minimal query execution time. */
        private long minTime;

        /** Maximum query execution time. */
        private long maxTime;

        /** Average query execution time. */
        private double avgTime;

        /** Count of executed queries. */
        private int execs;

        /** Count of failed queries. */
        private int fails;

        /** Required by {@link Externalizable}. */
        public QueryMetricsSnapshot() {
        }

        /**
         * @param minTime Minimal query execution time.
         * @param maxTime Maximum query execution time.
         * @param avgTime Average query execution time.
         * @param execs  Count of executed queries.
         * @param fails Count of failed queries.
         */
        public QueryMetricsSnapshot(long minTime, long maxTime, double avgTime, int execs, int fails) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.avgTime = avgTime;
            this.execs = execs;
            this.fails = fails;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(minTime);
            out.writeLong(maxTime);
            out.writeDouble(avgTime);
            out.writeInt(execs);
            out.writeInt(fails);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            minTime = in.readLong();
            maxTime = in.readLong();
            avgTime = in.readDouble();
            execs = in.readInt();
            fails = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public long minimumTime() {
            return minTime;
        }

        /** {@inheritDoc} */
        @Override public long maximumTime() {
            return maxTime;
        }

        /** {@inheritDoc} */
        @Override public double averageTime() {
            return avgTime;
        }

        /** {@inheritDoc} */
        @Override public int executions() {
            return execs;
        }

        /** {@inheritDoc} */
        @Override public int fails() {
            return fails;
        }
    }
}
