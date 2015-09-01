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

package org.apache.ignite.cache.eviction;

import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.random.RandomEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;

/**
 * Pluggable cache eviction policy. Usually, implementations will internally order
 * cache entries based on {@link #onEntryAccessed(boolean, EvictableEntry)} notifications and
 * whenever an element needs to be evicted, {@link EvictableEntry#evict()}
 * method should be called.
 * <p>
 * Ignite comes with following eviction policies out-of-the-box:
 * <ul>
 * <li>{@link LruEvictionPolicy}</li>
 * <li>{@link RandomEvictionPolicy}</li>
 * <li>{@link FifoEvictionPolicy}</li>
 * <li>{@link SortedEvictionPolicy}</li>
 * </ul>
 * <p>
 * The eviction policy thread-safety is ensured by Ignition. Implementations of this interface should
 * not worry about concurrency and should be implemented as they were only accessed from one thread.
 * <p>
 * Note that implementations of all eviction policies provided by Ignite are very
 * light weight in a way that they are all lock-free (or very close to it), and do not
 * create any internal tables, arrays, or other expensive structures.
 * The eviction order is preserved by attaching light-weight meta-data to existing
 * cache entries.
 */
public interface EvictionPolicy<K, V> {
    /**
     * Callback for whenever entry is accessed.
     *
     * @param rmv {@code True} if entry has been removed, {@code false} otherwise.
     * @param entry Accessed entry.
     */
    public void onEntryAccessed(boolean rmv, EvictableEntry<K, V> entry);
}