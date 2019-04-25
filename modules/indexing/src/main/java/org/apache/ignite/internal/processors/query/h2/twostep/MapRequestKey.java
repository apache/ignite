/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.util.typedef.F;

import java.util.UUID;

/**
 * Mapper request key.
 */
class MapRequestKey {
    /** Node ID. */
    private UUID nodeId;

    /** */
    private long reqId;

    /** */
    private int segmentId;

    /**
     * Constructor.
     *
     * @param nodeId Node ID.
     * @param reqId Request ID.
     * @param segmentId Segment ID.
     */
    MapRequestKey(UUID nodeId, long reqId, int segmentId) {
        this.nodeId = nodeId;
        this.reqId = reqId;
        this.segmentId = segmentId;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        MapRequestKey other = (MapRequestKey)o;

        return F.eq(nodeId, other.nodeId) && reqId == other.reqId && segmentId == other.segmentId;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId != null ? nodeId.hashCode() : 0;

        res = 31 * res + (int)(reqId ^ (reqId >>> 32));
        res = 31 * res + segmentId;

        return res;
    }
}
