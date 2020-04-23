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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import org.apache.ignite.cluster.ClusterNode;

/**
 * Segment key.
 */
public class SegmentKey {
    /** */
    private final ClusterNode node;

    /** */
    private final int segmentId;

    /**
     * Constructor.
     *
     * @param node Node.
     * @param segmentId Segment ID.
     */
    public SegmentKey(ClusterNode node, int segmentId) {
        assert node != null;

        this.node = node;
        this.segmentId = segmentId;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
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

        SegmentKey key = (SegmentKey)o;

        return segmentId == key.segmentId && node.id().equals(key.node.id());

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = node.hashCode();

        res = 31 * res + segmentId;

        return res;
    }
}
