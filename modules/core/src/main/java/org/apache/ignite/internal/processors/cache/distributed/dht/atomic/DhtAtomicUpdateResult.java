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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateAtomicResult;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class DhtAtomicUpdateResult {
    /** */
    private GridCacheReturn retVal;

    /** */
    private Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted;

    /** */
    private GridDhtAtomicAbstractUpdateFuture dhtFut;

    /** */
    private IgniteCacheExpiryPolicy expiry;

    /**
     * If batch update was interrupted in the middle, it should be continued from processedEntriesCount to avoid
     * extra update closure invocation.
     */
    private int processedEntriesCount;

    /**
     *
     */
    DhtAtomicUpdateResult() {
        // No-op.
    }

    /**
     * @param retVal Return value.
     * @param deleted Deleted entries.
     * @param dhtFut DHT update future.
     */
    DhtAtomicUpdateResult(GridCacheReturn retVal,
        Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted,
        GridDhtAtomicAbstractUpdateFuture dhtFut) {
        this.retVal = retVal;
        this.deleted = deleted;
        this.dhtFut = dhtFut;
    }

    /**
     * @param expiry Expiry policy.
     */
    void expiryPolicy(@Nullable IgniteCacheExpiryPolicy expiry) {
        this.expiry = expiry;
    }

    /**
     * @return Expiry policy.
     */
    @Nullable IgniteCacheExpiryPolicy expiryPolicy() {
        return expiry;
    }

    /**
     * @param entry Entry.
     * @param updRes Entry update result.
     * @param entries All entries.
     */
    void addDeleted(GridDhtCacheEntry entry,
        GridCacheUpdateAtomicResult updRes,
        Collection<GridDhtCacheEntry> entries) {
        if (updRes.removeVersion() != null) {
            if (deleted == null)
                deleted = new ArrayList<>(entries.size());

            deleted.add(F.t(entry, updRes.removeVersion()));
        }
    }

    /**
     * @return Deleted entries.
     */
    public Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted() {
        return deleted;
    }

    /**
     * Sets deleted entries.
     *
     * @param deleted deleted entries.
     */
    void deleted(Collection<IgniteBiTuple<GridDhtCacheEntry, GridCacheVersion>> deleted) {
        this.deleted = deleted;
    }

    /**
     * @return DHT future.
     */
    GridDhtAtomicAbstractUpdateFuture dhtFuture() {
        return dhtFut;
    }

    /**
     * @param retVal Result for operation.
     */
    void returnValue(GridCacheReturn retVal) {
        this.retVal = retVal;
    }

    /**
     * @return Result for invoke operation.
     */
    GridCacheReturn returnValue() {
        return retVal;
    }

    /**
     * @param dhtFut DHT future.
     */
    void dhtFuture(@Nullable GridDhtAtomicAbstractUpdateFuture dhtFut) {
        this.dhtFut = dhtFut;
    }

    /**
     * Sets processed entries count.
     * @param idx processed entries count.
     */
    public void processedEntriesCount(int idx) {
        processedEntriesCount = idx;
    }

    /**
     * Returns processed entries count.
     * @return processed entries count.
     */
    public int processedEntriesCount() {
        return processedEntriesCount;
    }
}
