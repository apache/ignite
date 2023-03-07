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

package org.apache.ignite.util;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.EXPIRE_TIME_ETERNAL;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_ETERNAL;

/**
 * Test methods for storage manipulation.
 */
public class TestStorageUtils {
    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    public static void corruptDataEntry(
        GridCacheContext<?, ?> ctx,
        Object key,
        boolean breakCntr,
        boolean breakData
    ) throws IgniteCheckedException {
        int partId = ctx.affinity().partition(key);
        GridDhtLocalPartition locPart = ctx.topology().localPartition(partId);

        CacheEntry<Object, Object> e = ctx.cache().keepBinary().getEntry(key);

        KeyCacheObject keyCacheObj = e.getKey() instanceof BinaryObject ?
            (KeyCacheObject)e.getKey() :
            new KeyCacheObjectImpl(e.getKey(), null, partId);

        DataEntry dataEntry = new DataEntry(ctx.cacheId(),
            keyCacheObj,
            new CacheObjectImpl(breakData ? e.getValue().toString() + "brokenValPostfix" : e.getValue(), null),
            GridCacheOperation.UPDATE,
            new GridCacheVersion(),
            new GridCacheVersion(),
            TTL_ETERNAL,
            EXPIRE_TIME_ETERNAL,
            partId,
            breakCntr ? locPart.updateCounter() + 1 : locPart.updateCounter(),
            DataEntry.EMPTY_FLAGS);

        IgniteCacheDatabaseSharedManager db = ctx.shared().database();

        db.checkpointReadLock();

        try {
            assert dataEntry.op() == GridCacheOperation.UPDATE;

            ctx.offheap().update(ctx,
                dataEntry.key(),
                dataEntry.value(),
                dataEntry.writeVersion(),
                dataEntry.expireTime(),
                locPart,
                null);

            ctx.offheap().dataStore(locPart).updateInitialCounter(dataEntry.partitionCounter() - 1, 1);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }
}
