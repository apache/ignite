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

package org.apache.ignite.cache;

import org.apache.ignite.cache.eviction.EvictionPolicy;

/**
 * Defines set of memory modes. Memory modes help control whether cache entries are
 * stored on heap memory, offheap memory, or in swap space.
 */
public enum CacheMemoryMode {
    /**
     * Entries will be stored on-heap first. The onheap tiered storage works as follows:
     * <nl>
     * <li>Entries are cached on heap memory first.</li>
     * <li>
     *     If offheap memory is enabled and eviction policy evicts an entry from heap memory, entry will
     *     be moved to offheap memory. If offheap memory is disabled, then entry is simply discarded.
     * </li>
     * <li>
     *     If swap space is enabled and offheap memory fills up, then entry will be evicted into swap space.
     *     If swap space is disabled, then entry will be discarded. If swap is enabled and offheap memory
     *     is disabled, then entry will be evicted directly from heap memory into swap.
     * </li>
     * </nl>
     * <p>
     * <b>Note</b> that heap memory evictions are handled by configured {@link EvictionPolicy}
     * implementation. By default, no eviction policy is enabled, so entries never leave heap
     * memory space unless explicitly removed.
     */
    ONHEAP_TIERED,

    /**
     * Works the same as {@link #ONHEAP_TIERED}, except that entries never end up in heap memory and get
     * stored in offheap memory right away. Entries get cached in offheap memory first and then
     * get evicted to swap, if one is configured.
     */
    OFFHEAP_TIERED,

    /**
     * Entry keys will be stored on heap memory, and values will be stored in offheap memory. Note
     * that in this mode entries can be evicted only to swap. The evictions will happen according
     * to configured {@link EvictionPolicy}.
     */
    OFFHEAP_VALUES,
}