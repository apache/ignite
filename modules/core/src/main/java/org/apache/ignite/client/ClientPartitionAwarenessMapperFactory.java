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

package org.apache.ignite.client;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;

/**
 * This factory is used on the client side and only when the partition awareness thin client feature is enabled. By default,
 * on a new cache the RendezvousAffinityFunction will be used for calculating mappings 'key-to-partition' and 'partition-to-node'.
 * The thin client will update all 'partitions-to-node' mappings on every cluster topology change and the 'key-to-partition'
 * mapping will be calculated on the client side.
 * <p>
 * The case described above will not be possible (and in turn partition awareness won't work) when a custom {@link AffinityFunction} or
 * a {@link AffinityKeyMapper} was previously used for a cache creation. The partition awareness mapper factory is used to solve this
 * issue. All 'partition-to-node' mappings will still be requested and received from a server node, however, if a custom AffinityFunction
 * or a custom AffinityKeyMapper was used a ClientPartitionAwarenessMapper produced by this factory will calculate mapping a key to
 * a partition.
 * <p>
 * These key to partition mapping functions produced by the factory are used only for local calculations, they will not be passed
 * to a server node.
 *
 * @see AffinityFunction
 * @since 2.14
 */
public interface ClientPartitionAwarenessMapperFactory {
    /**
     * @param cacheName Cache name to create a mapper for.
     * @param partitions Number of cache partitions received from a server node.
     * @return Key to a partition mapper function.
     */
    public ClientPartitionAwarenessMapper create(String cacheName, int partitions);
}
