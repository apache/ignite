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

package org.apache.ignite.internal.processors.query.running;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Query descriptor.
 */
public class GridRunningQueryInfo {
    /** */
    private final long id;

    /** Originating Node ID. */
    private final UUID nodeId;

    /** */
    private final String qry;

    /** Query type. */
    private final GridCacheQueryType qryType;

    /** Schema name. */
    private final String schemaName;

    /** */
    private final long startTime;

    /** Query start time in nanoseconds to measure duration. */
    private final long startTimeNanos;

    /** */
    private final GridQueryCancel cancel;

    /** */
    private final boolean loc;

    /** */
    @GridToStringExclude
    private final QueryRunningFuture fut = new QueryRunningFuture();

    /** Span of the running query. */
    private final Span span;

    /** Originator. */
    private final String qryInitiatorId;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Lazy flag. */
    private final boolean lazy;

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /** Subject ID. */
    private final UUID subjId;

    /** Query label. */
    private final String lbl;

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param nodeId Originating node ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param startTime Query start time.
     * @param startTimeNanos Query start time in nanoseconds.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     * @param qryInitiatorId Query's initiator identifier.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy flag.
     * @param distributedJoins Distributed joins flag.
     * @param subjId Subject ID.
     * @param lbl Query label.
     */
    public GridRunningQueryInfo(
        long id,
        UUID nodeId,
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        long startTime,
        long startTimeNanos,
        GridQueryCancel cancel,
        boolean loc,
        String qryInitiatorId,
        boolean enforceJoinOrder,
        boolean lazy,
        boolean distributedJoins,
        UUID subjId,
        @Nullable String lbl
    ) {
        this.id = id;
        this.nodeId = nodeId;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.startTimeNanos = startTimeNanos;
        this.cancel = cancel;
        this.loc = loc;
        this.span = MTC.span();
        this.qryInitiatorId = qryInitiatorId;
        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;
        this.distributedJoins = distributedJoins;
        this.subjId = subjId;
        this.lbl = lbl;
    }

    /**
     * @return Query ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Global query ID.
     */
    public String globalQueryId() {
        return QueryUtils.globalQueryId(nodeId, id);
    }

    /**
     * @return Query text.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * @return Query start time in nanoseconds.
     */
    public long startTimeNanos() {
        return startTimeNanos;
    }

    /**
     * Cancel query.
     */
    public void cancel() {
        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return Query running future.
     */
    public QueryRunningFuture runningFuture() {
        return fut;
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancelable() {
        return cancel != null;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean local() {
        return loc;
    }

    /**
     * @return Originating node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Span of the running query.
     */
    public Span span() {
        return span;
    }

    /**
     * @return Query's originator string (client host+port, user name,
     * job name or any user's information about query initiator).
     */
    public String queryInitiatorId() {
        return qryInitiatorId;
    }

    /**
     * @return Distributed joins.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Lazy flag.
     */
    public boolean lazy() {
        return lazy;
    }

    /** @return Subject ID. */
    public UUID subjectId() {
        return subjId;
    }

    /**{@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRunningQueryInfo.class, this);
    }

    /**
     * @return Query label.
     */
    public String label() {
        return lbl;
    }
}
