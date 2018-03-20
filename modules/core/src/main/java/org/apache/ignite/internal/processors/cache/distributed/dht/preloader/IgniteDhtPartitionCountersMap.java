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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition counters map.
 */
public class IgniteDhtPartitionCountersMap implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Map<Integer, Map<Integer, T2<Long, Long>>> map;

    /**
     * @return {@code True} if map is empty.
     */
    public synchronized boolean empty() {
        return map == null || map.isEmpty();
    }

    /**
     * @param cacheId Cache ID.
     * @param cntrMap Counters map.
     */
    public synchronized void putIfAbsent(int cacheId, Map<Integer, T2<Long, Long>> cntrMap) {
        if (map == null)
            map = new HashMap<>();

        if (!map.containsKey(cacheId))
            map.put(cacheId, cntrMap);
    }

    /**
     * @param cacheId Cache ID.
     * @return Counters map.
     */
    public synchronized Map<Integer, T2<Long, Long>> get(int cacheId) {
        if (map == null)
            map = new HashMap<>();

        Map<Integer, T2<Long, Long>> cntrMap = map.get(cacheId);

        if (cntrMap == null)
            return Collections.emptyMap();

        return cntrMap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDhtPartitionCountersMap.class, this);
    }
}
