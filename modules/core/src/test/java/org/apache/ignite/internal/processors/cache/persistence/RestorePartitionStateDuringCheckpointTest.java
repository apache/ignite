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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * This test restarts a cache during checkpoint.
 */
public class RestorePartitionStateDuringCheckpointTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrids(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite0.createCache(DEFAULT_CACHE_NAME);

        int partId = ignite0.affinity(DEFAULT_CACHE_NAME).allPartitions(ignite0.localNode())[0];

        log.info("Local partition was determined [node= " + ignite0.name()
            + ", partId=" + partId + ']');

        int key = F.first(partitionKeys(cache, partId, 1, 0));

        cache.put(key, key);

        GridCacheProcessor cacheProcessor = ignite0.context().cache();

        cacheProcessor.dynamicDestroyCaches(Collections.singleton(DEFAULT_CACHE_NAME), false, false).get();

        assertNull(ignite0.cache(DEFAULT_CACHE_NAME));

        DataRegion region = cacheProcessor.context().database().dataRegion(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);

        PageMemoryEx pageMemorySpy = spy((PageMemoryEx)region.pageMemory());

        long partMetaId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);
        int grpId = CU.cacheId(DEFAULT_CACHE_NAME);

        AtomicBoolean checkpointTriggered = new AtomicBoolean(false);

        doAnswer(invocation -> {
            IgniteCacheOffheapManager.CacheDataStore partDataStore = cacheProcessor.cacheGroup(grpId).topology()
                .localPartition(partId).dataStore();

            assertNotNull(partDataStore);

            if (partDataStore.rowStore() != null && checkpointTriggered.compareAndSet(false, true)) {
                info("Before write lock will be gotten on the partition meta page [pageId=" + invocation.getArgument(2) + ']');

                GridTestUtils.runAsync(() -> {
                    try {
                        forceCheckpoint();
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Checkpoint failed", e);
                    }
                });

                doSleep(200);
            }

            return invocation.callRealMethod();
        }).when(pageMemorySpy).writeLock(eq(grpId), eq(partMetaId), anyLong());

        GridTestUtils.setFieldValue(region, "pageMem", pageMemorySpy);

        IgniteInternalFuture startCacheFut = GridTestUtils.runAsync(() -> {
            ignite0.createCache(DEFAULT_CACHE_NAME);
        });

        startCacheFut.get();

        assertTrue(checkpointTriggered.get());

        assertSame(GridDhtPartitionState.OWNING, cacheProcessor.cacheGroup(grpId).topology().localPartition(partId).state());
    }
}
