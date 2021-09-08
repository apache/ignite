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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 * Test scenario when entries are updates using non tx counter assignment.
 */
public class TxPartitionCounterStatePutTest extends GridCommonAbstractTest {
    /** */
    private static final String ATOMIC_CACHE = "atomic";

    /** */
    private static final String TX_CACHE = "tx";

    /** */
    private static final String ATOMIC_CACHE_MEMORY = "atomic_mem";

    /** */
    private static final String TX_CACHE_MEMORY = "tx_mem";

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Set mixed persistence + in-memory configuration.
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDataRegionConfigurations(new DataRegionConfiguration().setName("mem").setInitialSize(100 * MB).setMaxSize(100 * MB)).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setName("dflt").setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setCacheConfiguration(
            cacheConfiguration(TX_CACHE, false).setAtomicityMode(TRANSACTIONAL),
            cacheConfiguration(ATOMIC_CACHE, false).setAtomicityMode(ATOMIC),
            cacheConfiguration(TX_CACHE_MEMORY, true).setAtomicityMode(TRANSACTIONAL),
            cacheConfiguration(ATOMIC_CACHE_MEMORY, true).setAtomicityMode(ATOMIC));

        return cfg;
    }

    /**
     * @param name Name.
     * @param inMemory In memory.
     */
    private CacheConfiguration cacheConfiguration(String name, boolean inMemory) {
        return new CacheConfiguration(name).
            setDataRegionName(inMemory ? "mem" : "dflt").
            setCacheMode(PARTITIONED).
            setWriteSynchronizationMode(FULL_SYNC).
            setAtomicityMode(TRANSACTIONAL).
            setBackups(BACKUPS).
            setAffinity(new RendezvousAffinityFunction(false, 32));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testPutAtomicSequentialPersistent() throws Exception {
        doTestPutSequential(ATOMIC_CACHE);
    }

    /** */
    @Test
    public void testPutTxSequentialPersistent() throws Exception {
        doTestPutSequential(TX_CACHE);
    }

    /** */
    @Test
    public void testPutAtomicConcurrentPersistent() throws Exception {
        doTestPutConcurrent(ATOMIC_CACHE, true);
    }

    /** */
    @Test
    public void testPutTxConcurrentPersistent() throws Exception {
        doTestPutConcurrent(TX_CACHE, true);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11793")
    public void testPutAtomicConcurrentPersistentWithIsolatedMode() throws Exception {
        doTestPutConcurrent(ATOMIC_CACHE, false);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11793")
    public void testPutTxConcurrentPersistentWithIsolatedMode() throws Exception {
        doTestPutConcurrent(TX_CACHE, false);
    }

    /** */
    @Test
    public void testPutAtomicSequentialVolatile() throws Exception {
        doTestPutSequential(ATOMIC_CACHE_MEMORY);
    }

    /** */
    @Test
    public void testPutTxSequentialVolatile() throws Exception {
        doTestPutSequential(TX_CACHE_MEMORY);
    }

    /** */
    @Test
    public void testPutAtomicConcurrentVolatile() throws Exception {
        doTestPutConcurrent(ATOMIC_CACHE_MEMORY, true);
    }

    /** */
    @Test
    public void testPutTxConcurrentVolatile() throws Exception {
        doTestPutConcurrent(TX_CACHE_MEMORY, true);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11793")
    public void testPutAtomicConcurrentVolatileWithIsolatedMode() throws Exception {
        doTestPutConcurrent(ATOMIC_CACHE_MEMORY, false);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11793")
    public void testPutTxConcurrentVolatileWithIsolatedMode() throws Exception {
        doTestPutConcurrent(TX_CACHE_MEMORY, false);
    }

    /** */
    private void doTestPutSequential(String cache) throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(NODES);

            loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 0);

            assertCountersSame(cache);

            assertPartitionsSame(idleVerify(grid(0), cache));

            loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 1000, 1);

            assertCountersSame(cache);

            assertPartitionsSame(idleVerify(grid(0), cache));

            loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 2000, 2);

            assertCountersSame(cache);

            loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 3000, 3);

            assertCountersSame(cache);

            assertPartitionsSame(idleVerify(grid(0), cache));

            assertEquals(4000, grid(0).cache(cache).size());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void doTestPutConcurrent(String cache, boolean skipIsolatedMode) throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(3);

            AtomicInteger idx = new AtomicInteger();

            CyclicBarrier b = new CyclicBarrier(4);

            multithreadedAsync(() -> {
                switch (idx.getAndIncrement()) {
                    case 0:
                        U.awaitQuiet(b);

                        loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 0);

                        break;
                    case 1:
                        U.awaitQuiet(b);

                        loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 1000, 1);

                        break;
                    case 2:
                        U.awaitQuiet(b);

                        loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 2000, 2);

                        break;

                    case 3:
                        U.awaitQuiet(b);

                        if (!skipIsolatedMode)
                            loadDataToPartition(PARTITION_ID, ignite.name(), cache, 1000, 3000, 3);

                        break;
                }
            }, 4, "put-thread").get();

            assertCountersSame(cache);

            assertPartitionsSame(idleVerify(grid(0), cache));

            assertEquals(skipIsolatedMode ? 3000 : 4000, grid(0).cache(cache).size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cacheName Cache name.
     */
    private void assertCountersSame(String cacheName) throws AssertionFailedError {
        PartitionUpdateCounter c0 = null;

        for (Ignite ignite : G.allGrids()) {
            PartitionUpdateCounter c = counter(PARTITION_ID, cacheName, ignite.name());

            if (c0 == null)
                c0 = c;
            else {
                assertEquals(ignite.name(), c0, c);

                c0 = c;
            }
        }
    }
}
