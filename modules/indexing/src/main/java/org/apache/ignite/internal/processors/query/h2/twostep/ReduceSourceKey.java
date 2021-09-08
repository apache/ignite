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

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Reduce source key for a specific remote data source (remote node + specific segment).
 */
public class ReduceSourceKey {
    /** Node ID. */
    private final UUID nodeId;

    /** Segment. */
    private final int segment;

    /**
     * @param nodeId Node ID.
     * @param segment Segment.
     */
    public ReduceSourceKey(UUID nodeId, int segment) {
        this.nodeId = nodeId;
        this.segment = segment;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ReduceSourceKey other = (ReduceSourceKey)o;

        return F.eq(segment, other.segment) && F.eq(nodeId, other.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * nodeId.hashCode() + segment;
    }
}
