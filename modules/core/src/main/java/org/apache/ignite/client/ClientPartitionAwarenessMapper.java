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
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * This function calculates the cache key to a partition mapping for each cache key. It is used only for local calculation on a client side.
 * <p>
 * When the {@link ClientConfiguration#isPartitionAwarenessEnabled()} and the cache was created with a custom {@link AffinityFunction}
 * or a {@link AffinityKeyMapper} this function will be used to calculate mappings. Be sure that a key maps to the same partition
 * produced by the {@link AffinityFunction#partition(Object)} method.
 *
 * @see AffinityFunction#partition(Object)
 * @since 2.14
 */
public interface ClientPartitionAwarenessMapper {
    /**
     * Gets a partition number for a given key starting from {@code 0}. Be sure that a key maps to the same partition
     * produced by the {@link AffinityFunction#partition(Object)} method.
     *
     * @param key Key to get partition for.
     * @return Partition number for a given key.
     */
    public int partition(Object key);
}
