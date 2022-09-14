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

package org.apache.ignite.internal.processors.query;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Info about new started query.
 */
public class GridQueryStartedInfo {
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

    /** Query cancellable flag. */
    private boolean cancellable;

    /** */
    private final boolean loc;

    /** Enforce join order query flag. */
    private boolean enforceJoinOrder;

    /** Lazy query flag. */
    private boolean lazy;

    /** Distributed joins query flag. */
    private boolean distributedJoins;

    /** Originator. */
    private final String qryInitiatorId;

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param nodeId Originating node ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param startTime Query start time.
     * @param cancellable Query cancellable flag.
     * @param loc Local query flag.
     * @param enforceJoinOrder Local query flag.
     * @param lazy Local query flag.
     * @param distributedJoins Local query flag.
     * @param qryInitiatorId Query's initiator identifier.
     */
    public GridQueryStartedInfo(
        Long id,
        UUID nodeId,
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        long startTime,
        boolean cancellable,
        boolean loc,
        boolean enforceJoinOrder,
        boolean lazy,
        boolean distributedJoins,
        String qryInitiatorId
    ) {
        this.id = id;
        this.nodeId = nodeId;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.cancellable = cancellable;
        this.loc = loc;
        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;
        this.distributedJoins = distributedJoins;
        this.qryInitiatorId = qryInitiatorId;
    }

    /**
     * @return Query ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
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
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancellable() {
        return cancellable;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean local() {
        return loc;
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

    /**
     * @return Distributed joins.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Query's originator string (client host+port, user name,
     * job name or any user's information about query initiator).
     */
    public String queryInitiatorId() {
        return qryInitiatorId;
    }

    /**{@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryStartedInfo.class, this);
    }
}
