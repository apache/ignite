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

package org.apache.ignite.internal.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THREAD_POOL_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THROTTLE;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Cache re-encryption tests.
 */
public class CacheGroupReencryptionTest extends AbstractEncryptionTest {
    /** */
    private static final String GRID_2 = "grid-2";

    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

    /** File IO fail flag. */
    private final AtomicBoolean failing = new AtomicBoolean();

    /** Count of cache backups. */
    private int backups;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_STOPPED);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024 * 1024 * 1024L)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(10 * 1024 * 1024)
            .setWalSegments(4)
            .setMaxWalArchiveSize(100 * 1024 * 1024L)
            .setCheckpointFrequency(30 * 1000L)
            .setWalMode(LOG_ONLY)
            .setFileIOFactory(new FailingFileIOFactory(new RandomAccessFileIOFactory(), failing));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 16)).setBackups(backups);
    }

    /**
     * Check physical recovery after checkpoint failure during re-encryption.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> loadData(100_000));

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        failing.set(true);

        nodes.get1().encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        fut.get();

        assertThrowsAnyCause(log, () -> {
            forceCheckpoint();

            return null;
        }, IgniteCheckedException.class, null);

        stopAllGrids(true);

        failing.set(false);

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THROTTLE, value = "10")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_BATCH_SIZE, value = "50")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THREAD_POOL_SIZE, value = "1")
    public void testPhysicalRecoveryWithUpdates() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        IgniteInternalFuture addFut = GridTestUtils.runAsync(() -> loadData(100_000));

        IgniteInternalFuture updateFut = GridTestUtils.runAsync(() -> {
            IgniteCache<Long, String> cache = grid(GRID_0).cache(cacheName());

            while (!Thread.currentThread().isInterrupted()) {
                for (long i = 50_000; i > 20_000; i--) {
                    String val = cache.get(i);

                    cache.put(i, val);
                }
            }
        });

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        nodes.get1().encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        forceCheckpoint();

        failing.set(true);

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        addFut.get();
        updateFut.cancel();

        assertThrowsAnyCause(log, () -> {
            forceCheckpoint();

            return null;
        }, IgniteCheckedException.class, null);

        stopAllGrids(true);

        failing.set(false);

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THROTTLE, value = "500")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_BATCH_SIZE, value = "100")
    public void testCacheStopDuringReencryption() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(100_000);

        IgniteEx node0 = nodes.get1();

        IgniteCache cache = node0.cache(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        IgniteInternalFuture<Void> fut0 = node0.context().encryption().encryptionTask(CU.cacheId(cacheName()));

        assertFalse(fut0.isDone());
        assertTrue(isReencryptionInProgress(node0, CU.cacheId(cacheName())));

        cache.destroy();

        // todo CacheStopException?
        assertThrowsAnyCause(log, () -> {
            fut0.get();

            return null;
        }, IgniteFutureCancelledCheckedException.class, null);
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THROTTLE, value = "500")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_BATCH_SIZE, value = "100")
    public void testPartitionEvictionDuringReencryption() throws Exception {
        backups = 1;

        CountDownLatch rebalanceFinished = new CountDownLatch(1);

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(100_000);

        IgniteEx node2 = startGrid(GRID_2);

        node2.events().localListen(evt -> {
            rebalanceFinished.countDown();

            return true;
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        resetBaselineTopology();

        rebalanceFinished.await();

        stopGrid(GRID_2);

        resetBaselineTopology();

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        stopAllGrids();

        System.setProperty(IGNITE_REENCRYPTION_THROTTLE, "0");

        startTestGrids(false);

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Test that partition files are reused correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THROTTLE, value = "50")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_BATCH_SIZE, value = "2")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THREAD_POOL_SIZE, value = "2")
    public void testPartitionFileDestroy() throws Exception {
        backups = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        forceCheckpoint();

        nodes.get1().encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        forceCheckpoint();

        checkGroupKey(CU.cacheId(cacheName()), 1, getTestTimeout());
    }

    /**
     * Test that partition files are reused correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THROTTLE, value = "50")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_BATCH_SIZE, value = "50")
    @WithSystemProperty(key = IGNITE_REENCRYPTION_THREAD_POOL_SIZE, value = "1")
    public void testPartitionFileDestroyAndRecreate() throws Exception {
        backups = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        forceCheckpoint();

        startGrid(GRID_2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        grid(GRID_0).encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        long walSegment = nodes.get1().context().cache().context().wal().currentSegment();

        for (long n = 0; n < walSegment; n++)
            nodes.get1().context().encryption().onWalSegmentRemoved(n);

        walSegment = nodes.get2().context().cache().context().wal().currentSegment();

        for (long n = 0; n < walSegment; n++)
            nodes.get2().context().encryption().onWalSegmentRemoved(n);

        // Trigger partitions re-create.
        stopGrid(GRID_2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        stopAllGrids();

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(CU.cacheId(cacheName()), 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Ensures that re-encryption continues after a restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null, true);

        loadData(100_000);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        assertEquals(1, node0.context().encryption().groupKey(grpId).id());
        assertEquals(1, node1.context().encryption().groupKey(grpId).id());

        stopAllGrids();

        info(">>> Start grids (iteration 1)");

        startTestGrids(false);

        enableCheckpoints(G.allGrids(), false);

        stopAllGrids();

        info(">>> Start grids (iteration 2)");

        startTestGrids(false);

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_REENCRYPTION_DISABLED, value = "true")
    public void testReencryptionStartsAfterNodeRestart() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        forceCheckpoint();

        stopAllGrids();

        nodes = startTestGrids(false);

        node0 = nodes.get1();
        node1 = nodes.get2();

        assertTrue(isReencryptionInProgress(node0, grpId));
        assertTrue(isReencryptionInProgress(node1, grpId));

        stopAllGrids();

        System.setProperty(IGNITE_REENCRYPTION_DISABLED, "false");

        startTestGrids(false);

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testKeyCleanup() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        Set<Long> walSegments = new HashSet<>();

        walSegments.add(node1.context().cache().context().wal().currentSegment());

        node0.encryption().changeGroupKey(Collections.singleton(cacheName())).get();

        walSegments.add(node1.context().cache().context().wal().currentSegment());

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        // Simulate that wal was removed.
        for (long segment : walSegments)
            node1.context().encryption().onWalSegmentRemoved(segment);

        stopGrid(GRID_1);

        node1 = startGrid(GRID_1);

        enableCheckpoints(G.allGrids(), true);

        node1.cluster().state(ClusterState.ACTIVE);

        node1.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

        checkEncryptedCaches(node0, node1);

        checkGroupKey(grpId, 1, MAX_AWAIT_MILLIS);
    }

    /** */
    static final class FailingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private final AtomicBoolean failFlag;

        /**
         * @param factory Delegate factory.
         */
        FailingFileIOFactory(FileIOFactory factory, AtomicBoolean failFlag) {
            delegateFactory = factory;

            this.failFlag = failFlag;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FailingFileIO(delegate);
        }

        /** */
        final class FailingFileIO extends FileIODecorator {
            /**
             * @param delegate File I/O delegate
             */
            public FailingFileIO(FileIO delegate) {
                super(delegate);
            }

            /** {@inheritDoc} */
            @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                if (failFlag.get())
                    throw new IOException("Test exception.");

                return delegate.writeFully(srcBuf, position);
            }
        }
    }
}
