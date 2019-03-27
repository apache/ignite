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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;

/**
 *
 */
public class CacheDataStoreTrackerImpl implements CacheDataStoreTracker {
    /** Context */
    private final CacheGroupContext grp;

    /** Logger */
    private final IgniteLogger log;

    /** Update counter. */
    private final PartitionUpdateCounter pCntr;

    /** Partition size. */
    private final AtomicLong storageSize = new AtomicLong();

    /** The map of cache sizes per each cache id. */
    private final ConcurrentMap<Integer, AtomicLong> cacheSizes = new ConcurrentHashMap<>();

    /** Shows does tracker have been initialized. */
    private final AtomicBoolean init = new AtomicBoolean();

    /**
     * @param grp Cache group context.
     */
    public CacheDataStoreTrackerImpl(CacheGroupContext grp) {
        this.grp = grp;

        log = grp.shared().kernalContext().log(getClass());
        pCntr = new PartitionUpdateCounter(log);
    }

    /** {@inheritDoc} */
    @Override public void init(long size, long updCntr, Map<Integer, Long> cacheSizes) {
        if (init.compareAndSet(false, true)) {
            assert pCntr.get() == 0;
            assert storageSize.get() == 0;
            assert cacheSizes == null || cacheSizes.isEmpty();

            pCntr.init(updCntr);

            storageSize.set(size);

            if (cacheSizes != null) {
                for (Map.Entry<Integer, Long> e : cacheSizes.entrySet())
                    this.cacheSizes.put(e.getKey(), new AtomicLong(e.getValue()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return storageSize.get() == 0;
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        if (grp.sharedGroup()) {
            AtomicLong size = cacheSizes.get(cacheId);

            return size != null ? (int)size.get() : 0;
        }

        return storageSize.get();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        if (!grp.sharedGroup())
            return null;

        Map<Integer, Long> res = new HashMap<>();

        for (Map.Entry<Integer, AtomicLong> e : cacheSizes.entrySet())
            res.put(e.getKey(), e.getValue().longValue());

        return res;
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        return storageSize.get();
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        assert init.get();

        storageSize.addAndGet(delta);

        if (grp.sharedGroup()) {
            AtomicLong size = cacheSizes.get(cacheId);

            if (size == null) {
                AtomicLong old = cacheSizes.putIfAbsent(cacheId, size = new AtomicLong());

                if (old != null)
                    size = old;
            }

            size.addAndGet(delta);
        }
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        return pCntr.next();
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return pCntr.initial();
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long cntr) {
        pCntr.updateInitial(cntr);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return pCntr.getAndAdd(delta);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return pCntr.get();
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        pCntr.update(val);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long start, long delta) {
        pCntr.update(start, delta);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return pCntr.finalizeUpdateCounters();
    }
}
