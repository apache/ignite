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

/** Map for storing GroupPartitionIdPair and their respective history counter values. */
public class PartitionReservationsMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 509;

    /** Mapping between GroupPartitionIdPair objects and their respective history counter values. */
    @Order(0)
    private Map<GroupPartitionIdPair, Long> map;

    /**
     * @return Map.
     */
    public Map<GroupPartitionIdPair, Long> map() {
        return map;
    }

    /**
     * @param map Map.
     */
    public void map(Map<GroupPartitionIdPair, Long> map) {
        this.map = map;
    }

    /**
     * @param pair Pair of group ID and partition ID.
     * @return History counter for this pair or null.
     */
    public @Nullable Long get(GroupPartitionIdPair pair) {
        if (map == null)
            return null;

        return map.get(pair);
    }

    /**
     * @param pair Pair of group ID and partition ID.
     * @param counter History counter for this pair.
     */
    public void put(GroupPartitionIdPair pair, Long counter) {
        if (map == null)
            map = new HashMap<>();

        map.put(pair, counter);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
