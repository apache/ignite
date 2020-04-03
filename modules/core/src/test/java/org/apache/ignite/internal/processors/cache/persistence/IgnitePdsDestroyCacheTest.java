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

package org.apache.ignite.internal.processors.cache.persistence;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.mockito.Mockito;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDestroyCacheTest extends IgnitePdsDestroyCacheAbstractTest {
    /**
     *  Test destroy non grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *  Test destroy grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyGroupCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     * Test destroy caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyCachesAbruptly() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8717");
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy group caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyGroupCachesAbruptly() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8717");
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (DHT).
     */
    public void testDestroyCacheOperationNotBlockingCheckpointTest() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(false);
    }

    /**
     * Tests if a checkpoint is not blocked forever by concurrent cache destroying (local).
     */
    public void testDestroyCacheOperationNotBlockingCheckpointTest_LocalCache() throws Exception {
        doTestDestroyCacheOperationNotBlockingCheckpointTest(true);
    }

    /**
     * Tests cache destry with hudge dirty pages.
     */
    public void testDestroyCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setGroupName("gr1"));
        ignite.getOrCreateCache(new CacheConfiguration<>("cache2").setGroupName("gr1"));

        try (IgniteDataStreamer<Object, Object> streamer2 = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            PageMemoryEx pageMemory = (PageMemoryEx)ignite.cachex(DEFAULT_CACHE_NAME).context().dataRegion().pageMemory();

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                streamer2.addData(i, new byte[pageMemory.pageSize() / 2]);
        }

        ignite.destroyCache(DEFAULT_CACHE_NAME);
    }

    /**
     * Tests partitioned cache destry with hudge dirty pages.
     */
    public void testDestroyCacheNotThrowsOOMPartitioned() throws Exception {
        doTestDestroyCacheNotThrowsOOM(false);
    }

    /**
     * Tests local cache destry with hudge dirty pages.
     */
    public void testDestroyCacheNotThrowsOOMLocal() throws Exception {
        doTestDestroyCacheNotThrowsOOM(true);
    }

    /** */
    public void doTestDestroyCacheNotThrowsOOM(boolean loc) throws Exception {
        Field batchField = U.findField(IgniteCacheOffheapManagerImpl.class, "BATCH_SIZE");

        int batchSize = batchField.getInt(null);

        int pageSize = 1024;

        int partitions = 32;

        DataStorageConfiguration ds = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(batchSize * pageSize * partitions)
                .setPersistenceEnabled(true))
            .setPageSize(pageSize);

        int payLoadSize = pageSize * 3 / 4;

        IgniteConfiguration cfg = getConfiguration().setDataStorageConfiguration(ds);

        final IgniteEx ignite = startGrid(optimize(cfg));

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite, loc);

        PageMemoryEx pageMemory = (PageMemoryEx) ignite.cachex(cacheName(0)).context().dataRegion().pageMemory();

        IgniteInternalFuture<?> loaderFut = runAsync(() -> {
            IgniteCache<Object, byte[]> c1 = ignite.cache(cacheName(0));

            long totalPages = pageMemory.totalPages();

            for (int i = 0; i <= totalPages; i++)
                c1.put(i, new byte[payLoadSize]);
        });

        CountDownLatch cpStart = new CountDownLatch(1);

        GridCacheDatabaseSharedManager dbMgr = ((GridCacheDatabaseSharedManager)ignite.context()
            .cache().context().database());

        DbCheckpointListener lsnr = new DbCheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) {
                /* No-op. */
            }

            @Override public void onCheckpointBegin(Context ctx) {
                cpStart.countDown();
            }

            @Override public void beforeCheckpointBegin(Context ctx) {
                /* No-op. */
            }
        };

        dbMgr.addCheckpointListener(lsnr);

        loaderFut.get();

        cpStart.await();

        dbMgr.removeCheckpointListener(lsnr);

        IgniteInternalFuture<?> delFut = runAsync(() -> {
            if (loc)
                ignite.cache(cacheName(0)).close();
            else
                ignite.destroyCache(cacheName(0));
        });

        delFut.get(20_000);
    }

    /**
     *
     */
    private void doTestDestroyCacheOperationNotBlockingCheckpointTest(boolean loc) throws Exception {
        final IgniteEx ignite = startGrids(1);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite, loc);

        loadCaches(ignite, !loc);

        // It's important to clear cache in group having > 1 caches.
        final String cacheName = cacheName(0);
        final CacheGroupContext grp = ignite.cachex(cacheName).context().group();

        final IgniteCacheOffheapManager offheap = grp.offheap();

        IgniteCacheOffheapManager mgr = Mockito.spy(offheap);

        final CountDownLatch checkpointLocked = new CountDownLatch(1);
        final CountDownLatch cpFutCreated = new CountDownLatch(1);
        final CountDownLatch realMtdCalled = new CountDownLatch(1);
        final CountDownLatch checked = new CountDownLatch(1);

        Mockito.doAnswer(invocation -> {
            checkpointLocked.countDown();

            assertTrue(U.await(cpFutCreated, 30, TimeUnit.SECONDS));

            Object ret = invocation.callRealMethod();

            // After calling clearing code cp future must be eventually completed and cp read lock reacquired.
            realMtdCalled.countDown();

            // Wait for checkpoint future while holding lock.
            U.awaitQuiet(checked);

            return ret;
        }).when(mgr).stopCache(Mockito.anyInt(), Mockito.anyBoolean());

        final Field field = U.findField(CacheGroupContext.class, "offheapMgr");
        field.set(grp, mgr);

        final IgniteInternalFuture<Object> fut = runAsync(() -> {
            assertTrue(U.await(checkpointLocked, 30, TimeUnit.SECONDS));

            // Trigger checkpoint while holding checkpoint read lock on cache destroy.
            final IgniteInternalFuture cpFut = ignite.context().cache().context().database().wakeupForCheckpoint("test");

            assertFalse(cpFut.isDone());

            cpFutCreated.countDown();

            assertTrue(U.await(realMtdCalled, 30, TimeUnit.SECONDS));

            try {
                cpFut.get(3_000); // Future must be completed after cache clearing but before releasing checkpoint lock.
            }
            finally {
                checked.countDown();
            }

            return null;
        });

        if (loc)
            ignite.cache(cacheName).close();
        else
            ignite.destroyCache(cacheName);

        fut.get();
    }
}
