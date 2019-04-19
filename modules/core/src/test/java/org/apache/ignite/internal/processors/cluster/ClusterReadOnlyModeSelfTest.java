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

package org.apache.ignite.internal.processors.cluster;

import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.GridServiceAssignmentsKey;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertDataStreamerReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;

/**
 *
 */
public class ClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Server nodes count. */
    private static final int SERVER_NODES_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setClientMode("client".equals(igniteInstanceName))
            .setCacheConfiguration(cacheConfigurations())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testChangeReadOnlyModeOnInactiveClusterFails() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrows(
            log,
            () -> {
                grid(0).cluster().readOnly(true);

                return null;
            },
            IgniteException.class,
            "Cluster not active!"
        );
    }

    /** */
    @Test
    public void testLocksNotAvaliableOnReadOnlyCluster() throws Exception {
        IgniteEx grid = startGrid(SERVER_NODES_COUNT);

        grid.cluster().active(true);

        final int key = 0;

        for (CacheConfiguration cfg : grid.configuration().getCacheConfiguration()) {
            if (cfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL && !CU.isSystemCache(cfg.getName()))
                grid.cache(cfg.getName()).put(key, cfg.getName().hashCode());
        }

        grid.cluster().readOnly(true);

        for (CacheConfiguration cfg : grid.configuration().getCacheConfiguration()) {
            if (cfg.getAtomicityMode() != CacheAtomicityMode.TRANSACTIONAL || CU.isSystemCache(cfg.getName()))
                continue;

            GridTestUtils.assertThrows(
                log,
                () -> {
                    grid.cache(cfg.getName()).lock(key).lock();

                    return null;
                },
                CacheException.class,
                "Failed to perform cache operation (cluster is in read only mode)"
            );

        }
    }

    /** */
    @Test
    public void testEnableReadOnlyModeInsideTransaction() throws Exception {
        IgniteEx grid = startGrids(SERVER_NODES_COUNT);

        grid.cluster().active(true);

        CacheConfiguration cfg = Stream.of(grid.configuration().getCacheConfiguration())
            .filter(c -> c.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
            .filter(c -> !CU.isSystemCache(c.getName())).findAny().get();

        final int key = 1;

        IgniteCache<Integer, Integer> cache = grid.cache(cfg.getName());

        cache.put(key, 0);

        CountDownLatch startTxLatch = new CountDownLatch(1);
        CountDownLatch clusterReadOnlyLatch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                startTxLatch.await();

                grid(1).cluster().readOnly(true);

                assertTrue(grid(1).cluster().readOnly());

                clusterReadOnlyLatch.countDown();
            }
            catch (InterruptedException e) {
                log.error("Thread interruped", e);

                fail("Thread interruped!");
            }
        });

        t.start();

        Transaction tx = grid(0).transactions().txStart();

        try {
            cache.put(key, 1);

            startTxLatch.countDown();

            tx.commit();
        }
        catch (Exception e) {
            log.error("TX Failed!", e);

            tx.rollback();
        }

        assertEquals(1, (int) cache.get(key));

        t.join();
    }

    /** */
    @Test
    public void testEnableReadOnlyModeInsideTransactionFailed() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);

        GridTestUtils.assertThrows(
            log,
            () -> {
                Transaction tx = grid.transactions().txStart();

                try {
                    grid.cluster().readOnly(true);

                    return null;
                }
                finally {
                    tx.commit();
                }
            },
            IgniteException.class,
            "Failed to activate read-only mode (must invoke the method outside of an active transaction)."
        );

    }

    /** */
    @Test
    public void testIgniteUtilityCacheAvaliableForUpdatesOnReadOnlyCluster() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);
        grid.cluster().readOnly(true);

        checkClusterInReadOnlyMode(true, grid);

        grid.utilityCache().put(new GridServiceAssignmentsKey("test"), "test");

        assertEquals("test", grid.utilityCache().get(new GridServiceAssignmentsKey("test")));
    }

    /** */
    @Test
    public void testReadOnlyFromClient() throws Exception {
        startGrids(1);
        startGrid("client");

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        IgniteEx client = grid("client");

        assertTrue("Should be client!", client.configuration().isClientMode());

        checkClusterInReadOnlyMode(false, client);

        client.cluster().readOnly(true);

        checkClusterInReadOnlyMode(true, client);

        client.cluster().readOnly(false);

        checkClusterInReadOnlyMode(false, client);
    }

    /** */
    private void checkClusterInReadOnlyMode(boolean readOnly, IgniteEx node) {
        assertEquals("Unexpected read-only mode", readOnly, node.cluster().readOnly());

        assertCachesReadOnlyMode(readOnly, cacheNames());

        assertDataStreamerReadOnlyMode(readOnly, cacheNames());
    }
}
