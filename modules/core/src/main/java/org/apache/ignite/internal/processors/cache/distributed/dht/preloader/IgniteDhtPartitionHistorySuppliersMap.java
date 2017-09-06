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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteDhtPartitionHistorySuppliersMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final IgniteDhtPartitionHistorySuppliersMap EMPTY = new IgniteDhtPartitionHistorySuppliersMap();

    /** */
    private Map<UUID, Map<T2<Integer, Integer>, Long>> map;

    /**
     * @return Empty map.
     */
    public static IgniteDhtPartitionHistorySuppliersMap empty() {
        return EMPTY;
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Supplier UUID.
     */
    @Nullable public synchronized UUID getSupplier(int grpId, int partId) {
        if (map == null)
            return null;

        for (Map.Entry<UUID, Map<T2<Integer, Integer>, Long>> e : map.entrySet()) {
            if (e.getValue().containsKey(new T2<>(grpId, partId)))
                return e.getKey();
        }

        return null;
    }

    /**
     * @param nodeId Node ID to check.
     * @return Reservations for the given node.
     */
    @Nullable public synchronized Map<T2<Integer, Integer>, Long> getReservations(UUID nodeId) {
        if (map == null)
            return null;

        return map.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr Partition counter.
     */
    public synchronized void put(UUID nodeId, int grpId, int partId, long cntr) {
        if (map == null)
            map = new HashMap<>();

        Map<T2<Integer, Integer>, Long> nodeMap = map.get(nodeId);

        if (nodeMap == null) {
            nodeMap = new HashMap<>();

            map.put(nodeId, nodeMap);
        }

        nodeMap.put(new T2<>(grpId, partId), cntr);
    }

    /**
     * @return {@code True} if empty.
     */
    public synchronized boolean isEmpty() {
        return map == null || map.isEmpty();
    }

    /**
     * @param that Other map to put.
     */
    public synchronized void putAll(IgniteDhtPartitionHistorySuppliersMap that) {
        map = that.map;
    }
}
