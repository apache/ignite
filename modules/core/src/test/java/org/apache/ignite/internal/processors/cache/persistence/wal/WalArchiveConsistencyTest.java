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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;

/**
 * Class for testing cases when WAL archive configuration was changed and the node was able to start.
 */
public class WalArchiveConsistencyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setWalSegments(10)
                    .setWalSegmentSize((int)U.MB)
                    .setMaxWalArchiveSize(20 * U.MB)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(U.GB)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx, Consumer<IgniteConfiguration> cfgOp) throws Exception {
        IgniteEx n = super.startGrid(idx, cfgOp);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        return n;
    }

    @Test
    public void test0() throws Exception {
        IgniteEx n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().setWalArchivePath(DFLT_WAL_PATH);
        });

        // Protection against deleting WAL segments.
        assertTrue(walMgr(n).reserve(new WALPointer(0, 0, 0)));

        AtomicInteger key = new AtomicInteger();

        dbMgr(n).checkpointReadLock();

        try {
            fill(n, 3, key);
        }
        finally {
            dbMgr(n).checkpointReadUnlock();
        }

        forceCheckpoint(n);

        // Guaranteed recovery from WAL segments.
        dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

        fill(n, 3, key);

        assertTrue(walMgr(n).currentSegment() >= 6);

        stopGrid(0);

        // Restarting the node with configuration change.
        n = startGrid(0, cfg -> {
            cfg.getDataStorageConfiguration().setWalSegments(4);
        });

        assertEquals(key.get(), n.cache(DEFAULT_CACHE_NAME).size());
    }

    /**
     * Filling the cache until N WAL segments are created.
     *
     * @param n Node.
     * @param segments Number of segments.
     * @param key Key counter.
     */
    private void fill(IgniteEx n, int segments, AtomicInteger key) {
        long end = walMgr(n).currentSegment() + segments;
        int i = 0;

        while (walMgr(n).currentSegment() < end) {
            int k = key.getAndIncrement();
            int[] arr = new int[64];

            Arrays.fill(arr, k);

            n.cache(DEFAULT_CACHE_NAME).put(key, arr);

            i++;
        }

        if (log.isInfoEnabled()) {
            log.info("Fill [keys=" + i + ", totalKeys=" + key.get() +
                ", segNum=" + segments + ", currSeg=" + walMgr(n).currentSegment() + ']');
        }
    }

    /**
     * Getting WAL manager of node.
     *
     * @param n Node.
     * @return WAL manager.
     */
    private FileWriteAheadLogManager walMgr(IgniteEx n) {
        return (FileWriteAheadLogManager)n.context().cache().context().wal();
    }

    /**
     * Getting db manager of node.
     *
     * @param n Node.
     * @return Db manager.
     */
    private GridCacheDatabaseSharedManager dbMgr(IgniteEx n) {
        return (GridCacheDatabaseSharedManager)n.context().cache().context().database();
    }
}
