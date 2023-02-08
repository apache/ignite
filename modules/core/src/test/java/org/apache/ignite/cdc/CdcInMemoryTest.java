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

package org.apache.ignite.cdc;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_NOT_CHANGED;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcInMemoryTest extends AbstractCdcTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.FSYNC)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setCdcEnabled(true))
            .setWalArchivePath(DFLT_WAL_ARCHIVE_PATH + "/" + U.maskForFileName(igniteInstanceName)));

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ign = startGrid(0);
        startGrid(1);

        IgniteCache<Integer, User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        CdcSelfTest.addData(cache, 0, 10);

        UserCdcConsumer cnsmr = new UserCdcConsumer();

        IgniteInternalFuture<?> fut = runAsync(createCdc(cnsmr, getConfiguration(ign.name())));

        List<Integer> data = cnsmr.data(ChangeEventType.UPDATE, CU.cacheId(DEFAULT_CACHE_NAME));

        waitForCondition(() -> data.size() == 5, 2 * WAL_ARCHIVE_TIMEOUT);

        System.out.println("MY data before=" + data);

        data.clear();

        // cdc flush
        flushCdc(ign, DEFAULT_CACHE_NAME, true);

        waitForCondition(() -> data.size() == 5, 2 * WAL_ARCHIVE_TIMEOUT);
        System.out.println("MY data after=" + data);

        assertEquals(5, data.size());
    }

    /** */
    private void flushCdc(IgniteEx srv, String cacheName, boolean primary) throws IgniteCheckedException {
        GridKernalContext ctx = srv.context();

        IgniteInternalCache<Object, Object> cache = ctx.cache().cache(cacheName);

        if (cache == null)
            throw new IgniteException("Cache does not exist [name=" + cacheName + ']');

        GridCacheContext<Object, Object> cctx = cache.context();

        GridIterator<CacheDataRow> localRows = cctx.offheap()
            .cacheIterator(cctx.cacheId(), primary, !primary, AffinityTopologyVersion.NONE, null, null);

        IgniteWriteAheadLogManager wal = ctx.cache().context().wal(true);

        for (CacheDataRow row : localRows) {
            KeyCacheObject key = row.key();

            DataRecord rec = new DataRecord(new DataEntry(
                cctx.cacheId(),
                key,
                row.value(),
                GridCacheOperation.CREATE,
                null,
                row.version(),
                TTL_NOT_CHANGED,
                key.partition(),
                -1,
                DataEntry.flags(primary))
            );

            System.out.println("MY log111=" + rec);

            wal.log(rec);
        }

        // todo fsync?
        wal.flush(null, true);
    }
}
