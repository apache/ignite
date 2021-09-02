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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Unique key for the query context.
 */
public class QueryContextKey {
    /** */
    private final UUID nodeId;

    /** */
    private final long qryId;

    /** */
    private final int segmentId;

    /**
     * Constructor.
     *
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     */
    public QueryContextKey(UUID nodeId, long qryId, int segmentId) {
        assert nodeId != null;

        this.nodeId = nodeId;
        this.qryId = qryId;
        this.segmentId = segmentId;
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryContextKey other = (QueryContextKey)o;

        return qryId == other.qryId && segmentId == other.segmentId && F.eq(nodeId, other.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();

        res = 31 * res + (int)(qryId ^ (qryId >>> 32));
        res = 31 * res + segmentId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryContextKey.class, this);
    }
}
