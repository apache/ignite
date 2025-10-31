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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Partition reload map for cache. */
public class CachePartitionsToReloadMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 507;

    /** Partition reload map for cache. */
    @Order(value = 0, method = "cachePartitions")
    private Map<Integer, PartitionsToReload> map;

    /**
     * @return Partition reload map for cache.
     */
    public Map<Integer, PartitionsToReload> cachePartitions() {
        return map;
    }

    /**
     * @param map Partition reload map for cache.
     */
    public void cachePartitions(Map<Integer, PartitionsToReload> map) {
        this.map = map;
    }

    /**
     * @param cacheId Cache id.
     * @return Partitions to reload for this cache.
     */
    public @Nullable PartitionsToReload get(int cacheId) {
        if (map == null)
            return null;

        return map.get(cacheId);
    }

    /**
     * @param cacheId Cache id.
     * @param parts Partitions to reload.
     */
    public void put(int cacheId, PartitionsToReload parts) {
        if (map == null)
            map = new HashMap<>();

        map.put(cacheId, parts);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
