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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Unique key for the query context.
 */
public class QueryContextKey {
    /** */
    private final UUID locNodeId;

    /** */
    private final UUID nodeId;

    /** */
    private final long qryId;

    /** */
    private final int segmentId;

    /** */
    private final GridH2QueryType type;

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     */
    QueryContextKey(UUID locNodeId, UUID nodeId, long qryId, int segmentId, GridH2QueryType type) {
        assert locNodeId != null;
        assert nodeId != null;
        assert type != null;

        this.locNodeId = locNodeId;
        this.nodeId = nodeId;
        this.qryId = qryId;
        this.segmentId = segmentId;
        this.type = type;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return locNodeId;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @return Segment ID.
     */
    public int segmentId() {
        return segmentId;
    }

    /**
     * @return Type.
     */
    public GridH2QueryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryContextKey key = (QueryContextKey)o;

        return qryId == key.qryId && nodeId.equals(key.nodeId) && type == key.type &&
           locNodeId.equals(key.locNodeId) ;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = locNodeId.hashCode();

        res = 31 * res + nodeId.hashCode();
        res = 31 * res + (int)(qryId ^ (qryId >>> 32));
        res = 31 * res + type.hashCode();
        res = 31 * res + segmentId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryContextKey.class, this);
    }
}
