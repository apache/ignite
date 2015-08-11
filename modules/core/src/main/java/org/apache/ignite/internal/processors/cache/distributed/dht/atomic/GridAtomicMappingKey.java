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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Mapping Key.
 */
class GridAtomicMappingKey {
    /** Node ID. */
    private final UUID nodeId;

    /** Partition. */
    private final int part;

    /**
     * @param nodeId Node ID.
     * @param part Partition.
     */
    GridAtomicMappingKey(UUID nodeId, int part) {
        assert nodeId != null;
        assert part >= -1 : part;

        this.nodeId = nodeId;
        this.part = part;
    }

    /**
     * @return Node ID.
     */
    UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Partition.
     */
    int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridAtomicMappingKey key = (GridAtomicMappingKey)o;

        return nodeId.equals(key.nodeId) && part == key.part;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();

        res = 31 * res + part;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAtomicMappingKey.class, this);
    }
}
