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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Partition reload map.
 */
public class IgniteDhtPartitionsToReloadMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 513;

    /** */
    @Order(0)
    Map<UUID, Map<Integer, Set<Integer>>> map;

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @return Set of partitions to reload.
     */
    public synchronized Set<Integer> get(UUID nodeId, int cacheId) {
        if (map == null)
            return Collections.emptySet();

        Map<Integer, Set<Integer>> nodeMap = map.get(nodeId);

        return nodeMap == null ? Collections.emptySet() : (Set<Integer>)F.emptyIfNull(nodeMap.get(cacheId));
    }

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     */
    public synchronized void put(UUID nodeId, int cacheId, int partId) {
        if (map == null)
            map = new HashMap<>();

        Map<Integer, Set<Integer>> nodeMap = map.computeIfAbsent(nodeId, k -> new HashMap<>());

        Set<Integer> parts = nodeMap.computeIfAbsent(cacheId, k -> new HashSet<>());

        parts.add(partId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDhtPartitionsToReloadMap.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
