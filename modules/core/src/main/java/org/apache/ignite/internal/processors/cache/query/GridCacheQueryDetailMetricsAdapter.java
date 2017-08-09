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
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adapter for {@link QueryDetailMetrics}.
 */
public class GridCacheQueryDetailMetricsAdapter implements QueryDetailMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type to track metrics. */
    private GridCacheQueryType qryType;

    /** Textual query representation. */
    private String qry;

    /** Cache name. */
    private String cache;

    /** Number of executions. */
    private int execs;

    /** Number of completions executions. */
    private int completions;

    /** Number of failures. */
    private int failures;

    /** Minimum time of execution. */
    private long minTime = -1;

    /** Maximum time of execution. */
    private long maxTime;

    /** Sum of execution time of completions time. */
    private long totalTime;

    /** Sum of execution time of completions time. */
    private long lastStartTime;

    /** Cached metrics group key.*/
    private GridCacheQueryDetailMetricsKey key;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryDetailMetricsAdapter() {
        // No-op.
    }

    /**
     * Constructor with metrics.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     * @param cache Cache name where query was executed.
     * @param startTime Duration of queue execution.
     * @param duration Duration of queue execution.
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public GridCacheQueryDetailMetricsAdapter(GridCacheQueryType qryType, String qry, String cache, long startTime,
        long duration, boolean failed) {
        this.qryType = qryType;
        this.qry = qryType == GridCacheQueryType.SCAN && qry == null ? cache : qry;
        this.cache = cache;

        if (failed) {
            execs = 1;
            failures = 1;
        }
        else {
            execs = 1;
            completions = 1;
            totalTime = duration;
            minTime = duration;
            maxTime = duration;
        }

        lastStartTime = startTime;
    }

    /**
     * Copy constructor.
     *
     * @param qryType Query type.
     * @param qry Textual query representation.
     * @param cache Cache name where query was executed.
     */
    public GridCacheQueryDetailMetricsAdapter(GridCacheQueryType qryType, String qry, String cache,
        int execs, int completions, int failures, long minTime, long maxTime, long totalTime, long lastStartTime,
        GridCacheQueryDetailMetricsKey key) {
        this.qryType = qryType;
        this.qry = qry;
        this.cache = cache;
        this.execs = execs;
        this.completions = completions;
        this.failures = failures;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.totalTime = totalTime;
        this.lastStartTime = lastStartTime;
        this.key = key;
    }

    /**
     * @return Metrics group key.
     */
    public GridCacheQueryDetailMetricsKey key() {
        if (key == null)
            key = new GridCacheQueryDetailMetricsKey(qryType, qry);

        return key;
    }

    /**
     * Aggregate metrics.
     *
     * @param m Other metrics to take into account.
     * @return Aggregated metrics.
     */
    public GridCacheQueryDetailMetricsAdapter aggregate(QueryDetailMetrics m) {
        return new GridCacheQueryDetailMetricsAdapter(
            qryType,
            qry,
            m.cache(),
            execs + m.executions(),
            completions + m.completions(),
            failures + m.failures(),
            minTime < 0 || minTime > m.minimumTime() ? m.minimumTime() : minTime,
            maxTime < m.maximumTime() ? m.maximumTime() : maxTime,
            totalTime + m.totalTime(),
            lastStartTime < m.lastStartTime() ? m.lastStartTime() : lastStartTime,
            key
        );
    }

    /** {@inheritDoc} */
    @Override public String queryType() {
        return qryType.name();
    }

    /** {@inheritDoc} */
    @Override public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public String cache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return execs;
    }

    /** {@inheritDoc} */
    @Override public int completions() {
        return completions;
    }

    /** {@inheritDoc} */
    @Override public int failures() {
        return failures;
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        return minTime < 0 ? 0 : minTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime;
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        double val = completions;

        return val > 0 ? totalTime / val : 0;
    }

    /** {@inheritDoc} */
    @Override public long totalTime() {
        return totalTime;
    }

    /** {@inheritDoc} */
    @Override public long lastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, qryType);
        U.writeString(out, qry);
        U.writeString(out, cache);
        out.writeInt(execs);
        out.writeInt(completions);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeLong(totalTime);
        out.writeLong(lastStartTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = GridCacheQueryType.fromOrdinal(in.readByte());
        qry = U.readString(in);
        cache = U.readString(in);
        execs = in.readInt();
        completions = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        totalTime = in.readLong();
        lastStartTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryDetailMetricsAdapter.class, this);
    }
}
