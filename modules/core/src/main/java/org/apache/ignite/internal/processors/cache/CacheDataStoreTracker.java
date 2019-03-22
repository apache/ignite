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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import org.apache.ignite.internal.util.GridLongList;

/**
 *
 */
public interface CacheDataStoreTracker {
    /**
     * @param size Size to init.
     * @param updCntr Update counter to init.
     * @param cacheSizes Cache sizes if store belongs to group containing multiple caches.
     */
    public void init(long size, long updCntr, Map<Integer, Long> cacheSizes);

    /**
     * @param cacheId Cache ID.
     * @return Size.
     */
    public long cacheSize(int cacheId);

    /**
     * @return Cache sizes if store belongs to group containing multiple caches.
     */
    public Map<Integer, Long> cacheSizes();

    /**
     * @return Total size.
     */
    public long fullSize();

    /**
     * @return {@code True} if there are no items in the store.
     */
    public boolean isEmpty();

    /**
     * Updates size metric for particular cache.
     *
     * @param cacheId Cache ID.
     * @param delta Size delta.
     */
    public void updateSize(int cacheId, long delta);

    /**
     * @return Update counter.
     */
    public long updateCounter();

    /**
     * @param val Update counter.
     */
    public void updateCounter(long val);

    /**
     * Updates counters from start value by delta value.
     *
     * @param start Start.
     * @param delta Delta
     */
    public void updateCounter(long start, long delta);

    /**
     * @return Next update counter.
     */
    public long nextUpdateCounter();

    /**
     * Returns current value and updates counter by delta.
     *
     * @param delta Delta.
     * @return Current value.
     */
    public long getAndIncrementUpdateCounter(long delta);

    /**
     * @return Initial update counter.
     */
    public long initialUpdateCounter();

    /**
     * @param cntr Counter.
     */
    public void updateInitialCounter(long cntr);

    /**
     * Flushes pending update counters closing all possible gaps.
     *
     * @return Even-length array of pairs [start, end] for each gap.
     */
    public GridLongList finalizeUpdateCounters();
}
