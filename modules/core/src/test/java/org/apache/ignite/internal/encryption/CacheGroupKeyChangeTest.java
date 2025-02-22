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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.configuration.WALMode.NONE;
import static org.apache.ignite.internal.managers.encryption.GridEncryptionManager.INITIAL_KEY_ID;
import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cache group key change distributed process tests.
 */
public class CacheGroupKeyChangeTest extends AbstractEncryptionTest {
    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

    /** 1 megabyte in bytes. */
    private static final int MB = 1024 * 1024;

    /** */
    private static final String GRID_2 = "grid-2";

    /** Discovery hook for distributed process. */
    private InitMessageDiscoveryHook discoveryHook;

    /** Count of cache backups. */
    private int backups;

    /** Number of WAL segments. */
    private int walSegments = 10;

    /** WAL mode. */
    private WALMode walMode = LOG_ONLY;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (discoveryHook != null)
            ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(discoveryHook);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100 * MB)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(MB)
            .setWalSegments(walSegments)
            .setMaxWalArchiveSize(2 * walSegments * MB)
            .setCheckpointFrequency(30 * 1000L)
            .setWalMode(walMode);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 8)).setBackups(backups);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testRejectNodeJoinDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        assertEquals(0, grids.get1().context().encryption().getActiveKey(grpId).id());

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName()));

        commSpi.waitForBlocked();

        assertThrowsWithCause(() -> startGrid(3), IgniteCheckedException.class);

        commSpi.stopBlock();

        fut.get();

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        checkEncryptedCaches(grids.get1(), grids.get2());
    }

    /** @throws Exception If failed. */
    @Test
    public void testNotAllBltNodesPresent() throws Exception {
        startTestGrids(true);

        createEncryptedCache(grid(GRID_0), grid(GRID_1), cacheName(), null);

        stopGrid(GRID_1);

        grid(GRID_0).encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_1);

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsBeforePrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsBeforePerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(false, true, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsAfterPrepare() throws Exception {
        checkNodeFailsDuringRotation(true, true, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(false, false, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCrdFailsAfterPerform() throws Exception {
        checkNodeFailsDuringRotation(true, false, false);
    }

    /**
     * @param stopCrd {@code True} to stop coordinator.
     * @param prepare {@code True} to stop on the prepare phase. {@code False} to stop on the perform phase.
     * @param discoBlock  {@code True} to block discovery, {@code False} to block communication SPI.
     */
    private void checkNodeFailsDuringRotation(boolean stopCrd, boolean prepare, boolean discoBlock) throws Exception {
        cleanPersistenceDir();

        DistributedProcessType type = prepare ?
            DistributedProcessType.CACHE_GROUP_KEY_CHANGE_PREPARE : DistributedProcessType.CACHE_GROUP_KEY_CHANGE_FINISH;

        InitMessageDiscoveryHook locHook = new InitMessageDiscoveryHook(type);

        if (discoBlock && stopCrd)
            discoveryHook = locHook;

        IgniteEx grid0 = startGrid(GRID_0);

        if (discoBlock && !stopCrd)
            discoveryHook = locHook;

        IgniteEx grid1 = startGrid(GRID_1);

        grid0.cluster().state(ClusterState.ACTIVE);

        createEncryptedCache(grid0, grid1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        checkGroupKey(grpId, INITIAL_KEY_ID, MAX_AWAIT_MILLIS);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid1);

        if (!discoBlock) {
            AtomicBoolean preparePhase = new AtomicBoolean(true);

            spi.blockMessages((node, msg) -> {
                if (msg instanceof SingleNodeMessage) {
                    boolean isPrepare = preparePhase.compareAndSet(true, false);

                    return prepare || !isPrepare;
                }

                return false;
            });
        }

        String alive = stopCrd ? GRID_1 : GRID_0;
        String stopped = stopCrd ? GRID_0 : GRID_1;

        IgniteFuture<Void> changeFut = grid(alive).encryption().changeCacheGroupKey(Collections.singleton(cacheName()));

        IgniteInternalFuture<?> stopFut = new GridFinishedFuture<>();

        if (discoBlock) {
            locHook.waitForBlocked(MAX_AWAIT_MILLIS);

            stopGrid(stopped, true);

            locHook.stopBlock();
        }
        else {
            spi.waitForBlocked();

            stopFut = runAsync(() -> stopGrid(stopped, true));
        }

        changeFut.get(MAX_AWAIT_MILLIS);
        stopFut.get(MAX_AWAIT_MILLIS);

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        IgniteEx stoppedNode = startGrid(stopped);

        stoppedNode.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

        awaitPartitionMapExchange();

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        stoppedNode.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        checkGroupKey(grpId, INITIAL_KEY_ID + 2, MAX_AWAIT_MILLIS);
    }

    /**
     * Ensures that we can rotate the key more than 255 times.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testKeyIdentifierOverflow() throws Exception {
        IgniteEx node = startTestGrids(true).get1();

        createEncryptedCache(node, null, cacheName(), null, false);

        int grpId = CU.cacheId(cacheName());

        byte keyId = INITIAL_KEY_ID;

        do {
            node.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

            // Validates reencryption of index partition.
            checkGroupKey(grpId, ++keyId & 0xff, MAX_AWAIT_MILLIS);
        } while (keyId != INITIAL_KEY_ID);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMasterAndCacheGroupKeySimultaneousChange() throws Exception {
        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        createEncryptedCache(node0, node1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        Random rnd = ThreadLocalRandom.current();

        for (byte keyId = 1; keyId < 50; keyId++) {
            String currMkName = node0.context().config().getEncryptionSpi().getMasterKeyName();
            String newMkName = currMkName.equals(MASTER_KEY_NAME_2) ? MASTER_KEY_NAME_3 : MASTER_KEY_NAME_2;

            boolean changeGrpFirst = rnd.nextBoolean();

            IgniteFuture<Void> grpKeyFut;
            IgniteFuture<Void> masterKeyFut;

            if (changeGrpFirst) {
                grpKeyFut = node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName()));
                masterKeyFut = node0.encryption().changeMasterKey(newMkName);
            }
            else {
                masterKeyFut = node0.encryption().changeMasterKey(newMkName);
                grpKeyFut = node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName()));
            }

            masterKeyFut.get(MAX_AWAIT_MILLIS);
            assertTrue(checkMasterKeyName(newMkName));

            try {
                grpKeyFut.get(MAX_AWAIT_MILLIS);
                checkGroupKey(grpId, keyId, MAX_AWAIT_MILLIS);
            }
            catch (IgniteException e) {
                assertTrue(e.getMessage().contains("Cache group key change was rejected. Master key has been changed."));

                // Retry iteration.
                keyId -= 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName()));

        commSpi.waitForBlocked();

        IgniteCache<Integer, Integer> cache = grids.get1().createCache(cacheConfiguration("cache1", null));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        commSpi.stopBlock();

        fut.get();

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        checkGroupKey(CU.cacheId("cache1"), INITIAL_KEY_ID, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartSameGroupDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        String grpName = "shared";

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), grpName);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grids.get2());

        commSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<Void> fut = grids.get1().encryption().changeCacheGroupKey(Collections.singleton(grpName));

        commSpi.waitForBlocked();

        IgniteCache<Integer, Integer> cache =
            grids.get1().createCache(cacheConfiguration("cache1", grpName));

        commSpi.stopBlock();

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        fut.get();

        checkGroupKey(CU.cacheId(grpName), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChangeKeyDuringRebalancing() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx node0 = grids.get1();
        IgniteEx node1 = grids.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(500_000);

        IgniteEx node2 = startGrid(GRID_2);

        resetBaselineTopology();

        int grpId = CU.cacheId(cacheName());

        node2.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        awaitPartitionMapExchange();

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeWithOlderKeyBecameCoordinator() throws Exception {
        backups = 1;

        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        createEncryptedCache(node0, node1, cacheName(), null);

        int grpId = CU.cacheId(cacheName());

        stopGrid(GRID_0);

        // Changing encryption key on one node.
        node1.context().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);
        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        stopGrid(GRID_1);

        // The node with only the old key ID has become the coordinator.
        node0 = startGrid(GRID_0);
        assertTrue(Collections.singleton(INITIAL_KEY_ID).containsAll(node0.context().encryption().groupKeyIds(grpId)));

        node1 = startGrid(GRID_1);
        node1.cluster().state(ClusterState.ACTIVE);

        // Wait until cache will be reencrypted with the old key.
        checkGroupKey(grpId, INITIAL_KEY_ID, MAX_AWAIT_MILLIS);

        GridEncryptionManager encrMgr0 = node0.context().encryption();
        GridEncryptionManager encrMgr1 = node1.context().encryption();

        // Changing the encryption key is not possible until the WAL segment,
        // encrypted (probably) with the previous key, is deleted.
        assertThrowsAnyCause(log,
            () -> encrMgr1.changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS),
            IgniteException.class,
            "Cache group key change was rejected. Cannot add new key identifier, it's already present.");

        long walIdx = node1.context().cache().context().wal().currentSegment();

        // Simulate WAL segment deletion.
        for (long n = 0; n <= walIdx; n++)
            node1.context().encryption().onWalSegmentRemoved(walIdx);

        encrMgr1.changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);
        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkEncryptedCaches(node0, node1);

        walIdx = Math.max(node0.context().cache().context().wal().currentSegment(),
            node1.context().cache().context().wal().currentSegment());

        // Simulate WAL segment deletion.
        for (long n = 0; n <= walIdx; n++) {
            encrMgr0.onWalSegmentRemoved(walIdx);
            encrMgr1.onWalSegmentRemoved(walIdx);
        }

        // Make sure the previous key has been removed.
        checkKeysCount(node0, grpId, 1, MAX_AWAIT_MILLIS);
        assertEquals(encrMgr1.groupKeyIds(grpId), encrMgr0.groupKeyIds(grpId));
    }

    /**
     * Ensures that a node cannot join the cluster if it cannot replace an existing encryption key.
     * <p>
     * If the joining node has a different encryption key than the coordinator, but with the same identifier, it should
     * not perform key rotation to a new key (recevied from coordinator) until the previous key is deleted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinRejectedIfKeyCannotBeReplaced() throws Exception {
        backups = 2;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        startGrid(GRID_2);

        resetBaselineTopology();

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        forceCheckpoint();

        stopGrid(GRID_0);
        stopGrid(GRID_1);

        grid(GRID_2).encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        int grpId = CU.cacheId(cacheName());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        grid(GRID_2).encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        checkGroupKey(grpId, INITIAL_KEY_ID + 2, MAX_AWAIT_MILLIS);

        stopGrid(GRID_2);

        startTestGrids(false);

        checkGroupKey(grpId, INITIAL_KEY_ID, MAX_AWAIT_MILLIS);

        grid(GRID_0).encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

        assertThrowsAnyCause(log,
            () -> startGrid(GRID_2),
            IgniteSpiException.class,
            "Cache key differs! Node join is rejected.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testKeyChangeWithNodeFilter() throws Exception {
        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        Object nodeId0 = node0.localNode().consistentId();
        Object nodeId1 = node1.localNode().consistentId();

        String cache1 = cacheName();
        String cache2 = "cache2";

        node0.createCache(cacheConfiguration(cache1, null)
            .setNodeFilter(node -> !node.consistentId().equals(nodeId0)));

        node0.createCache(cacheConfiguration(cache2, null)
            .setNodeFilter(node -> !node.consistentId().equals(nodeId1)));

        loadData(10_000);

        forceCheckpoint();

        int grpId1 = CU.cacheId(cache1);
        int grpId2 = CU.cacheId(cache2);

        node0.encryption().changeCacheGroupKey(Arrays.asList(cache1, cache2)).get();

        List<Integer> keys0 = node0.context().encryption().groupKeyIds(grpId1);
        List<Integer> keys1 = node1.context().encryption().groupKeyIds(grpId1);

        assertEquals(2, keys0.size());
        assertEquals(2, keys1.size());

        assertTrue(keys0.containsAll(keys1));

        keys0 = node0.context().encryption().groupKeyIds(grpId2);
        keys1 = node1.context().encryption().groupKeyIds(grpId2);

        assertEquals(2, keys0.size());
        assertEquals(2, keys1.size());

        assertTrue(keys0.containsAll(keys1));

        checkGroupKey(grpId1, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkGroupKey(grpId2, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        stopAllGrids();

        startTestGrids(false);

        node0 = grid(GRID_0);
        node1 = grid(GRID_1);

        IgniteCache<Object, Object> allNodesCache = node0.createCache("cacheX");

        // Previous keys must be deleted when the corresponding WAL segment is deleted, so we adding data on all nodes.
        long endTime = U.currentTimeMillis() + 30_000;
        int cntr = 0;

        do {
            allNodesCache.put(cntr, String.valueOf(cntr));

            if (node0.context().encryption().groupKeyIds(grpId1).size() == 1 &&
                node1.context().encryption().groupKeyIds(grpId1).size() == 1 &&
                node0.context().encryption().groupKeyIds(grpId2).size() == 1 &&
                node1.context().encryption().groupKeyIds(grpId2).size() == 1)
                break;

            ++cntr;
        } while (U.currentTimeMillis() < endTime);

        assertEquals(1, node0.context().encryption().groupKeyIds(grpId1).size());
        assertEquals(1, node0.context().encryption().groupKeyIds(grpId2).size());

        assertEquals(node0.context().encryption().groupKeyIds(grpId1), node1.context().encryption().groupKeyIds(grpId1));
        assertEquals(node0.context().encryption().groupKeyIds(grpId2), node1.context().encryption().groupKeyIds(grpId2));

        checkGroupKey(grpId1, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkGroupKey(grpId2, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        checkEncryptedCaches(node0, node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicChangeWithConstantLoad() throws Exception {
        walSegments = 20;

        startTestGrids(true);

        IgniteEx node0 = grid(GRID_0);
        IgniteEx node1 = grid(GRID_1);

        GridEncryptionManager encrMgr0 = node0.context().encryption();
        GridEncryptionManager encrMgr1 = node1.context().encryption();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        IgniteInternalFuture<?> loadFut = loadDataAsync(node0);

        try {
            IgniteCache<Object, Object> cache = node0.cache(cacheName());

            boolean success = waitForCondition(() -> cache.size() > 2000, MAX_AWAIT_MILLIS);
            assertTrue(success);

            node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS);

            awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

            waitForCondition(() ->
                encrMgr0.groupKeyIds(grpId).size() == 1 && encrMgr1.groupKeyIds(grpId).size() == 1, MAX_AWAIT_MILLIS);
        }
        finally {
            loadFut.cancel();
        }

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        assertEquals(node0.cluster().localNode().id().toString(), 1, encrMgr0.groupKeyIds(grpId).size());
        assertEquals(node1.cluster().localNode().id().toString(), 1, encrMgr1.groupKeyIds(grpId).size());
    }

    /**
     * Ensures that unused key will be removed even if user cleaned wal archive folder manually.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWalArchiveCleanup() throws Exception {
        IgniteEx node = startGrid(GRID_0);

        node.cluster().state(ClusterState.ACTIVE);

        createEncryptedCache(node, null, cacheName(), null);

        node.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        IgniteWriteAheadLogManager walMgr = node.context().cache().context().wal();

        long reservedIdx = walMgr.currentSegment();
        assertTrue(walMgr.reserve(new WALPointer(reservedIdx, 0, 0)));

        while (walMgr.lastArchivedSegment() < reservedIdx) {
            long val = ThreadLocalRandom.current().nextLong();

            node.cache(cacheName()).put(val, String.valueOf(val));
        }

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        assertEquals(2, node.context().encryption().groupKeyIds(grpId).size());

        NodeFileTree ft = node.context().pdsFolderResolver().fileTree();

        stopAllGrids();

        // Cleanup WAL arcive folder.
        assertTrue(U.delete(ft.walArchive().getParentFile()));

        node = startGrid(GRID_0);

        node.cluster().state(ClusterState.ACTIVE);

        while (node.context().encryption().groupKeyIds(grpId).size() != 1) {
            long val = ThreadLocalRandom.current().nextLong();

            node.cache(cacheName()).put(val, String.valueOf(val));
        }

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @param grid Grid.
     * @return Future for this operation.
     */
    private IgniteInternalFuture<?> loadDataAsync(Ignite grid) {
        return runAsync(() -> {
            long cntr = grid.cache(cacheName()).size();

            try (IgniteDataStreamer<Long, String> streamer = grid.dataStreamer(cacheName())) {
                while (!Thread.currentThread().isInterrupted()) {
                    streamer.addData(cntr, String.valueOf(cntr));

                    ++cntr;
                }
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartOnClientDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        IgniteEx client = startClientGrid(getConfiguration("client"));

        node0.cluster().state(ClusterState.ACTIVE);

        String grpName = "shared";

        createEncryptedCache(client, null, cacheName(), grpName);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(node1);

        commSpi.blockMessages((node, message) -> message instanceof SingleNodeMessage);

        IgniteFuture<Void> changeKeyFut = node0.encryption().changeCacheGroupKey(Collections.singleton(grpName));

        commSpi.waitForBlocked();

        String cacheName = "userCache";

        IgniteInternalFuture<?> cacheStartFut = runAsync(() -> {
            client.getOrCreateCache(cacheConfiguration(cacheName, grpName));
        });

        commSpi.stopBlock();

        changeKeyFut.get(MAX_AWAIT_MILLIS);
        cacheStartFut.get(MAX_AWAIT_MILLIS);

        IgniteCache<Integer, String> cache = client.cache(cacheName);

        for (int i = 0; i < 200; i++)
            cache.put(i, String.valueOf(i));

        checkEncryptedCaches(node0, client);

        checkGroupKey(CU.cacheId(grpName), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        checkEncryptedCaches(node0, node1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientJoinDuringRotation() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        node0.cluster().state(ClusterState.ACTIVE);

        createEncryptedCache(node0, node1, cacheName(), null);

        awaitPartitionMapExchange();

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(node1);

        commSpi.blockMessages((node, message) -> message instanceof SingleNodeMessage);

        IgniteFuture<Void> changeKeyFut = node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName()));

        commSpi.waitForBlocked();

        IgniteEx client = startClientGrid(getConfiguration("client"));

        assertTrue(!changeKeyFut.isDone());

        commSpi.stopBlock();

        changeKeyFut.get(MAX_AWAIT_MILLIS);

        checkEncryptedCaches(node0, client);

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Ensures that node can join after rotation of encryption key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinAfterRotation() throws Exception {
        backups = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        forceCheckpoint();

        stopGrid(GRID_1);
        resetBaselineTopology();

        nodes.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_1);
        resetBaselineTopology();
        awaitPartitionMapExchange();

        int grpId = CU.cacheId(cacheName());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkEncryptedCaches(grid(GRID_0), grid(GRID_1));

        GridEncryptionManager encrMgr0 = grid(GRID_0).context().encryption();
        GridEncryptionManager encrMgr1 = grid(GRID_1).context().encryption();

        long maxWalIdx = Math.max(nodes.get1().context().cache().context().wal().currentSegment(),
            nodes.get2().context().cache().context().wal().currentSegment());

        for (long idx = 0; idx <= maxWalIdx; idx++) {
            encrMgr0.onWalSegmentRemoved(maxWalIdx);
            encrMgr1.onWalSegmentRemoved(maxWalIdx);
        }

        checkKeysCount(grid(GRID_1), grpId, 1, MAX_AWAIT_MILLIS);
        checkKeysCount(grid(GRID_0), grpId, 1, MAX_AWAIT_MILLIS);

        startGrid(GRID_2);

        resetBaselineTopology();
        awaitPartitionMapExchange();

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkEncryptedCaches(grid(GRID_2), nodes.get1());

        assertEquals(encrMgr0.groupKeyIds(grpId), grid(GRID_2).context().encryption().groupKeyIds(grpId));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongCacheGroupSpecified() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        IgniteEx node0 = grids.get1();
        IgniteEx node1 = grids.get2();

        assertThrowsAnyCause(log,
            () -> node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS),
            IgniteException.class,
            "Cache group key change was rejected. Cache or group \"" + cacheName() + "\" doesn't exists");

        node0.createCache(new CacheConfiguration<>(cacheName()).setNodeFilter(node -> node.equals(node0.localNode())));

        assertThrowsAnyCause(log,
            () -> node1.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS),
            IgniteException.class,
            "Cache group key change was rejected. Cache or group \"" + cacheName() + "\" is not encrypted.");

        node0.destroyCache(cacheName());

        awaitPartitionMapExchange();

        String grpName = "cacheGroup1";

        createEncryptedCache(node0, node1, cacheName(), grpName);

        assertThrowsAnyCause(log,
            () -> node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get(MAX_AWAIT_MILLIS),
            IgniteException.class,
            "Cache group key change was rejected. Cache or group \"" + cacheName() + "\" is a part of group \"" +
            grpName + "\". Provide group name instead of cache name for shared groups.");
    }

    /** @throws Exception If failed. */
    @Test
    public void testChangeCacheGroupKeyWithoutWAL() throws Exception {
        walMode = NONE;
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        createEncryptedCache(grids.get1(), grids.get2(), cacheName(), null);

        IgniteEx node0 = grids.get1();

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        int grpId = CU.cacheId(cacheName());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        assertEquals(1, node0.context().encryption().groupKeyIds(grpId).size());
        assertEquals(1, grids.get2().context().encryption().groupKeyIds(grpId).size());
    }

    /**
     * Custom discovery hook to block distributed process.
     */
    private static class InitMessageDiscoveryHook extends DiscoveryHook {
        /**
         * Latch to sync execution.
         */
        private final CountDownLatch unlockLatch = new CountDownLatch(1);

        /**
         * Latch to sync execution.
         */
        private final CountDownLatch blockedLatch = new CountDownLatch(1);

        /**
         * Distributed process type.
         */
        private final DistributedProcessType type;

        /**
         * @param type Distributed process type.
         */
        private InitMessageDiscoveryHook(DistributedProcessType type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
            if (!(customMsg instanceof InitMessage))
                return;

            InitMessage<Serializable> msg = (InitMessage<Serializable>)customMsg;

            if (msg.type() != type.ordinal())
                return;

            try {
                blockedLatch.countDown();

                unlockLatch.await(MAX_AWAIT_MILLIS, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * @param timeout Timeout in milliseconds.
         * @throws InterruptedException If interrupted.
         */
        public void waitForBlocked(long timeout) throws InterruptedException {
            blockedLatch.await(timeout, TimeUnit.MILLISECONDS);
        }

        /** */
        public void stopBlock() {
            unlockLatch.countDown();
        }
    }
}
