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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    @Order(value = 0, method = "partitionsToReload")
    private Map<UUID, CachePartitionsToReloadMap> map;

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @return Collection of partitions to reload.
     */
    public synchronized Collection<Integer> get(UUID nodeId, int cacheId) {
        if (map == null)
            return Collections.emptySet();

        CachePartitionsToReloadMap nodeMap = map.get(nodeId);

        if (nodeMap == null)
            return Collections.emptySet();

        PartitionsToReload partsToReload = nodeMap.get(cacheId);

        if (partsToReload == null)
            return Collections.emptySet();

        return F.emptyIfNull(partsToReload.partitions());
    }

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     */
    public synchronized void put(UUID nodeId, int cacheId, int partId) {
        if (map == null)
            map = new HashMap<>();

        CachePartitionsToReloadMap nodeMap = map.computeIfAbsent(nodeId, k -> new CachePartitionsToReloadMap());

        PartitionsToReload parts = nodeMap.get(cacheId);

        if (parts == null) {
            parts = new PartitionsToReload();

            nodeMap.put(cacheId, parts);
        }

        parts.add(partId);
    }

    /**
     * @return {@code True} if empty.
     */
    public synchronized boolean isEmpty() {
        return map == null || map.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDhtPartitionsToReloadMap.class, this);
    }

    /**
     * @return Partition reload map.
     */
    public Map<UUID, CachePartitionsToReloadMap> partitionsToReload() {
        return map;
    }

    /**
     * @param map Partition reload map.
     */
    public void partitionsToReload(Map<UUID, CachePartitionsToReloadMap> map) {
        this.map = map;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
