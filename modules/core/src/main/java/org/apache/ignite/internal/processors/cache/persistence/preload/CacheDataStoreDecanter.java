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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl.treeName;

/**
 *
 */
public class CacheDataStoreDecanter extends CacheDataStoreAdapter {
    /** */
    private final CacheGroupContext grp;

    /** */
    private final int partId;

    /** */
    private final String name;

    /** */
    private final Queue<DataRecord> store = new ConcurrentLinkedQueue<>();

    /**
     * @param partId Partition id.
     */
    public CacheDataStoreDecanter(CacheGroupContext grp, int partId) {
        assert grp.persistenceEnabled();

        this.grp = grp;
        this.partId = partId;

        name = treeName(partId) + "-logging";
    }

    /** {@inheritDoc} */
    @Override public int partId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }
    
    /** {@inheritDoc} */
    @Override public void remove(
        GridCacheContext cctx,
        KeyCacheObject key,
        int partId
    ) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public CacheDataRow createRow(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        assert oldRow == null;

        DataRow dataRow = makeDataRow(key, val, ver, expireTime, cctx.cacheId());

        // Log to the temporary store.
        store.add(new DataRecord(new DataEntry(
            cctx.cacheId(),
            key,
            val,
            val == null ? DELETE : GridCacheOperation.UPDATE,
            null,
            ver,
            expireTime,
            partId,
            111
        )));

        return dataRow;
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure c
    ) throws IgniteCheckedException {

        // Assume we've performed an invoke operation on the B+ Tree and find nothing.
        // Emulating that always inserting/removing a new value.
        c.call(null);
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {

        return null;
    }

    /** {@inheritDoc} */
    @Override public PendingEntriesTree pendingTree() {

        return null;
    }

    /**
     * @param key Cache key.
     * @param val Cache value.
     * @param ver Version.
     * @param expireTime Expired time.
     * @param cacheId Cache id.
     * @return Made data row.
     */
    private DataRow makeDataRow(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        int cacheId
    ) {
        if (key.partition() < 0)
            key.partition(partId);

        return new DataRow(key, val, ver, partId, expireTime, cacheId);
    }
}