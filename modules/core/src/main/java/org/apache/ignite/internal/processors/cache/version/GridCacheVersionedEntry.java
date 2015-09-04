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

package org.apache.ignite.internal.processors.cache.version;

import org.jetbrains.annotations.Nullable;

/**
 * Cache entry along with version information.
 */
public interface GridCacheVersionedEntry<K, V> {
    /**
     * Gets entry's key.
     *
     * @return Entry's key.
     */
    public K key();

    /**
     * Gets entry's value.
     *
     * @return Entry's value.
     */
    @Nullable public V value();

    /**
     * Gets entry's TTL.
     *
     * @return Entry's TTL.
     */
    public long ttl();

    /**
     * Gets entry's expire time.
     *
     * @return Entry's expire time.
     */
    public long expireTime();

    /**
     * Gets ID of initiator data center.
     *
     * @return ID of initiator data center.
     */
    public byte dataCenterId();

    /**
     * Gets entry's topology version in initiator data center.
     *
     * @return Entry's topology version in initiator data center.
     */
    public int topologyVersion();

    /**
     * Gets entry's order in initiator data center.
     *
     * @return Entry's order in initiator data center
     */
    public long order();

    /**
     * Gets entry's global time in initiator data center.
     *
     * @return Entry's global time in initiator data center
     */
    public long globalTime();
}