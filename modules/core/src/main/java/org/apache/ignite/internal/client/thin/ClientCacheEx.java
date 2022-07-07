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

package org.apache.ignite.internal.client.thin;

import java.util.function.ToIntBiFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.client.ClientCache;

/**
 * This is an internal thin client cache API. Please, note that it may change in a future releases without any notifications.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface ClientCacheEx<K, V> extends ClientCache<K, V> {
    /**
     * This cache key mapper configuration parameter is used only when the partition awareness thin client feature is enabled. By default,
     * on a new cache the RendezvousAffinityFunction will be used for calculating mappings 'key-to-partition' and 'partition-to-node'. The
     * thin client will keep all 'partitions-to-node' mappings up to date when each cache put/get request occurs and the 'key-to-partition'
     * mapping will also be calculated on the client side.
     *
     * The case described above will not be possible (and in turn partition awareness won't work) when a custom {@link AffinityFunction} or
     * a {@link AffinityKeyMapper} was previously used for a cache creation. The key mapper configuration parameter is used to solve this
     * issue. All 'partition-to-node' mappings will still be requested from a server node, however, if a custom affinity function was used
     * the key mapper will calculate mapping a key to a partition.
     *
     * This client cache key mapper will not be passed to a server node, it is used only for local calculations.
     *
     * @param mapper Mapper that accepts a cache key and total number of cache partitions.
     */
    public ClientCacheEx<K, V> withPartitionAwarenessKeyMapper(ToIntBiFunction<Object, Integer> mapper);
}
