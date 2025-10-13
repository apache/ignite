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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Partition reload map.
 */
public class IgniteDhtPartitionsToReloadMap implements Serializable, Message {
    /** Type code. */
    public static final short TYPE_CODE = 512;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    private Map<UUID, IgniteDhtPartitionsToReloadMapMessage> map;

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @return Collection of partitions to reload.
     */
    public synchronized Collection<Integer> get(UUID nodeId, int cacheId) {
        if (map == null)
            return Collections.emptySet();

        IgniteDhtPartitionsToReloadMapMessage nodeMsg = map.get(nodeId);

        if (nodeMsg == null || nodeMsg.map() == null)
            return Collections.emptySet();

        IgniteDhtPartitionsToReloadMessage msg = nodeMsg.map().get(cacheId);

        if (msg == null)
            return Collections.emptySet();

        return msg.partitions();
    }

    /**
     * @param nodeId Node ID.
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     */
    public synchronized void put(UUID nodeId, int cacheId, int partId) {
        if (map == null)
            map = new HashMap<>();

        IgniteDhtPartitionsToReloadMapMessage nodeMsg = map.computeIfAbsent(nodeId,
            k -> new IgniteDhtPartitionsToReloadMapMessage(new HashMap<>()));

        IgniteDhtPartitionsToReloadMessage msg = nodeMsg.map().computeIfAbsent(cacheId,
            k -> new IgniteDhtPartitionsToReloadMessage(new HashSet<>()));

        msg.partitions().add(partId);
    }

    /**
     * @return {@code True} if empty.
     */
    public synchronized boolean isEmpty() {
        return map == null || map.isEmpty();
    }

    /**
     * @return Partition reload map.
     */
    public Map<UUID, IgniteDhtPartitionsToReloadMapMessage> map() {
        return map;
    }

    /**
     * @param map Partition reload map.
     */
    public void map(Map<UUID, IgniteDhtPartitionsToReloadMapMessage> map) {
        this.map = map;
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
