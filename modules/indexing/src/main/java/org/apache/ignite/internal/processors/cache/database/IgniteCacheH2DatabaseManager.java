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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.database.BPlusTreeRefIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.table.IndexColumn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class IgniteCacheH2DatabaseManager extends GridCacheManagerAdapter implements IgniteCacheDatabaseManager {
    /** Primary index. */
    private Index primaryIdx;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();
    }

    /**
     * @param name Index name.
     * @param tbl Table.
     * @param pk Primary key flag.
     * @param keyCol Key column.
     * @param valCol Value column.
     * @param cols Columns.
     * @return Index.
     */
    public Index createIndex(
        String name,
        GridH2Table tbl,
        boolean pk,
        int keyCol,
        int valCol,
        IndexColumn[] cols
    ) throws IgniteCheckedException {
        IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

        IgniteBiTuple<FullPageId, Boolean> page = dbMgr.meta().getOrAllocateForIndex(cctx.cacheId(), name);

        if (log.isInfoEnabled())
            log.info("Creating cache index [cacheId=" + cctx.cacheId() + ", idxName=" + name +
                ", rootPageId=" + page.get1() + ", allocated=" + page.get2() + ']');

        Index idx = new BPlusTreeRefIndex(
            cctx,
            dbMgr.pageMemory(),
            page.get1(),
            page.get2(),
            keyCol,
            valCol,
            tbl,
            name,
            pk,
            cols);

        if (pk) {
            if (primaryIdx != null)
                throw new IgniteCheckedException("Primary index already exists for cache " +
                    "(make sure only one key-value type pair is stored in the cache): " + cctx.name());

            primaryIdx = idx;
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override public void clear(boolean readers) {
        if (primaryIdx == null)
            throw new IllegalStateException("Primary index should be always created.");

        Cursor cur = primaryIdx.find((Session)null, null, null);

        Collection<KeyCacheObject> keys = new ArrayList<>();

        while (cur.next()) {

            GridH2Row row = (GridH2Row)cur.get();

            try {
                KeyCacheObject key = row.key instanceof KeyCacheObject ? (KeyCacheObject)row.key :
                    cctx.toCacheKeyObject(row.key.valueBytes(cctx.cacheObjectContext()));

                keys.add(key);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to obtain cache key: " + row.key, e);
            }
        }

        GridCacheVersion obsoleteVer = null;

        for (KeyCacheObject key : keys) {
            try {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                GridCacheEntryEx entry = cctx.cache().entryEx(key);

                entry.clear(obsoleteVer, readers, null);
            }
            catch (GridDhtInvalidPartitionException ignore) {
                // Ignore.
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear cache entry: " + key, e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long entriesCount(boolean primary, boolean backup, AffinityTopologyVersion topVer) {
        if (primaryIdx == null)
            throw new IllegalStateException("Primary index should be always created.");

        Cursor cur = primaryIdx.find((Session)null, null, null);

        ClusterNode locNode = cctx.localNode();

        long cnt = 0;

        while (cur.next()) {
            GridH2Row row = (GridH2Row)cur.get();

            if (primary) {
                if (cctx.affinity().primary(locNode, row.key, topVer)) {
                    cnt++;

                    continue;
                }
            }

            if (backup) {
                if (cctx.affinity().backup(locNode, cctx.affinity().partition(row.key), topVer))
                    cnt++;
            }
        }

        return cnt;
    }
}
