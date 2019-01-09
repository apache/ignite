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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.query.QueryHistoryMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Adapter for {@link QueryHistoryMetrics}.
 */
public class QueryHistoryMetricsAdapter implements QueryHistoryMetrics, Externalizable {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Link to internal node in eviction deque. */
    private AtomicReference<ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter>> linkRef;

    /** Textual query representation. */
    private String qry;

    /** Schema name. */
    private String schema;

    /** Flag of local query. */
    private boolean loc;

    /** Number of executions. */
    private int execs;

    /** Number of failures. */
    private int failures;

    /** Minimum time of execution. */
    private long minTime = -1;

    /** Maximum time of execution. */
    private long maxTime;

    /** Last start time of execution. */
    private long lastStartTime;

    /** Query history metrics group key. */
    private QueryHistoryMetricsKey key;

    /**
     * Required by {@link Externalizable}.
     */
    public QueryHistoryMetricsAdapter() {
        // No-op.
    }

    /**
     * Constructor with metrics.
     *
     * @param qry Textual query representation.
     * @param schema Schema name.
     * @param loc {@code true} for local query.
     * @param startTime Duration of queue execution.
     * @param duration Duration of queue execution.
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public QueryHistoryMetricsAdapter(String qry, String schema, boolean loc, long startTime,
        long duration, boolean failed) {
        this.qry = qry;
        this.schema = schema;
        this.loc = loc;

        execs = 1;

        if (failed)
            failures = 1;
        else {
            minTime = duration;
            maxTime = duration;
        }

        lastStartTime = startTime;

        linkRef = new AtomicReference<>();
    }

    /**
     * Copy constructor.
     *
     * @param qry Textual query representation.
     * @param schema Schema.
     * @param loc Local flag of query execution.
     * @param failures Number of failures.
     * @param minTime Minimum of execution time.
     * @param maxTime Maximum of execution time.
     * @param lastStartTime Time of last start of execution.
     * @param key Key of query history metrics.
     */
    private QueryHistoryMetricsAdapter(String qry, String schema, boolean loc, int execs, int failures, long minTime,
        long maxTime, long lastStartTime, QueryHistoryMetricsKey key,
        AtomicReference<ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter>> linkRef) {
        this.qry = qry;
        this.schema = schema;
        this.loc = loc;
        this.execs = execs;
        this.failures = failures;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.lastStartTime = lastStartTime;
        this.key = key;
        this.linkRef = linkRef;
    }

    /**
     * @return Metrics group key.
     */
    public QueryHistoryMetricsKey key() {
        if (key == null)
            key = new QueryHistoryMetricsKey(qry, schema, loc);

        return key;
    }

    /**
     * Aggregate new metrics with already existen.
     *
     * @param m Other metrics to take into account.
     * @return Aggregated metrics.
     */
    public QueryHistoryMetricsAdapter aggregateWithNew(QueryHistoryMetricsAdapter m) {
        assert m.linkRef.get() == null;

        return new QueryHistoryMetricsAdapter(
            qry,
            schema,
            loc,
            execs + m.executions(),
            failures + m.failures(),
            Math.min(minTime, m.minTime),
            Math.max(maxTime, m.maxTime),
            Math.max(lastStartTime, m.lastStartTime),
            key,
            linkRef
        );
    }

    /** {@inheritDoc} */
    @Override public String query() {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public String schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return execs;
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
    @Override public long lastStartTime() {
        return lastStartTime;
    }

    /**
     * @return Link to internal node in eviction deque.
     */
    @Nullable public ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> link() {
        return linkRef.get();
    }

    /**
     * Atomically set link only if previous values was {@code null}.
     *
     * @param link Link to internal node in eviction deque.
     * @return {@code true} in case link has been set, {@code false} otherwise.
     */
    public boolean setLinkIfAbsent(ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> link) {
        return linkRef.compareAndSet(null, link);
    }

    /**
     * Atomically remove link.
     *
     * @param link Link to internal node in eviction deque.
     * @return {@code true} if given link has been removed, {@code false} otherwise.
     */
    public boolean unlink(ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> link) {
        return linkRef.compareAndSet(link, null);
    }

    /**
     * Atomically replace link to new.
     *
     * @param expLink Link which should be replaced.
     * @param updatedLink New link which should be set.
     * @return {@code true} If link has been updated.
     */
    public boolean replaceLink(ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> expLink,
        ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> updatedLink) {
        return linkRef.compareAndSet(expLink, updatedLink);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, qry);
        U.writeString(out, schema);
        out.writeBoolean(loc);
        out.writeInt(execs);
        out.writeInt(failures);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeLong(lastStartTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        qry = U.readString(in);
        schema = U.readString(in);
        loc = in.readBoolean();
        execs = in.readInt();
        failures = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        lastStartTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryHistoryMetricsAdapter.class, this);
    }
}
