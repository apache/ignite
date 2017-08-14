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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Key to identify lazy worker.
 */
public class MapQueryLazyWorkerKey {
    /** Client node ID. */
    private final UUID nodeId;

    /** Query request ID. */
    private final long qryReqId;

    /** Segment. */
    private final int segment;

    /**
     * Constructor.
     *
     * @param nodeId Node ID.
     * @param qryReqId Query request ID.
     * @param segment Segment.
     */
    public MapQueryLazyWorkerKey(UUID nodeId, long qryReqId, int segment) {
        this.nodeId = nodeId;
        this.qryReqId = qryReqId;
        this.segment = segment;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Segment.
     */
    public int segment() {
        return segment;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();

        res = 31 * res + (int)(qryReqId ^ (qryReqId >>> 32));
        res = 31 * res + segment;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj != null && obj instanceof MapQueryLazyWorkerKey) {
            MapQueryLazyWorkerKey other = (MapQueryLazyWorkerKey)obj;

            return F.eq(qryReqId, other.qryReqId) && F.eq(nodeId, other.nodeId) && F.eq(segment, other.segment);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MapQueryLazyWorkerKey.class, this);
    }
}
