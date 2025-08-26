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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.util.CommonUtils.getIgniteHome;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class IgniteLostPartitionsRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int SERVER_NODES_CNT = 4;

    /** */
    private static final int CACHE_KEYS_CNT = 100;
    
    /** */
    private static final String CACHE_0 = "partitioned-0";

    /** */
    private static final String CACHE_1 = "partitioned-1";

    /** */
    private final ListeningTestLogger listeningLogger = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setWorkDirectory(workDirectory(getTestIgniteInstanceIndex(igniteInstanceName)).getAbsolutePath())
            .setClusterStateOnStart(INACTIVE)
            .setUserAttributes(singletonMap("CELL", "CELL" + (getTestIgniteInstanceIndex(igniteInstanceName)) % 2))
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setGridLogger(listeningLogger)
            .setCacheConfiguration(
                createCacheConfiguration(CACHE_0),
                createCacheConfiguration(CACHE_1))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        for (int i = 0; i < SERVER_NODES_CNT; i++)
            U.delete(workDirectory(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        for (int i = 0; i < SERVER_NODES_CNT; i++)
            U.delete(workDirectory(i));

        super.afterTest();
    }

    /** */
    @Test
    public void testPartitionLossDetectionOnActivation() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        grid(0).cluster().state(INACTIVE);

        stopGrid(1);
        stopGrid(3);

        startNodeWithPdsCleared(1);
        startNodeWithPdsCleared(3);

        LogListener logLsnr = LogListener.matches("Cache group partitions were not restored from the PDS during cluster activation")
            .times(2)
            .build();

        listeningLogger.registerListener(logLsnr);

        logLsnr.reset();

        grid(0).cluster().state(ACTIVE);

        logLsnr.check(getTestTimeout());

        Collection<Integer> lostParts0 = checkCacheLostParitionedDetected(CACHE_0);
        Collection<Integer> lostParts1 = checkCacheLostParitionedDetected(CACHE_1);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        checkKeysAvailableAfterLostPartitionsReset(CACHE_0, lostParts0);
        checkKeysAvailableAfterLostPartitionsReset(CACHE_1, lostParts1);

        grid(0).cluster().state(INACTIVE);
    }

    /** */
    @Test
    public void testLostPartitionsRestoredOnActivation() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        stopGrid(1);
        stopGrid(3);

        grid(0).cluster().state(INACTIVE);

        startGrid(1);
        startGrid(3);

        grid(0).cluster().state(ACTIVE);

        grid(0).cacheNames().forEach(this::checkCacheLostParitionedDetected);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        for (int nodeIdx = 0; nodeIdx < SERVER_NODES_CNT + 1; nodeIdx++) {
            for (String cacheName : grid(0).cacheNames()) {
                for (int i = 0; i < CACHE_KEYS_CNT; i++)
                    assertEquals(i, grid(nodeIdx).cache(cacheName).get(i));
            }
        }
    }

    /** */
    @Test
    public void testNodeJoinAfterActivation() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        grid(0).cluster().state(INACTIVE);

        stopGrid(2);

        grid(0).cluster().state(ACTIVE);
        grid(0).cluster().state(INACTIVE);

        stopGrid(1);
        stopGrid(3);

        startNodeWithPdsCleared(1);
        startNodeWithPdsCleared(3);

        grid(0).cluster().state(ACTIVE);

        startGrid(2);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        Collection<Integer> lostParts0 = checkCacheLostParitionedDetected(CACHE_0);
        Collection<Integer> lostParts1 = checkCacheLostParitionedDetected(CACHE_1);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        checkKeysAvailableAfterLostPartitionsReset(CACHE_0, lostParts0);
        checkKeysAvailableAfterLostPartitionsReset(CACHE_1, lostParts1);
    }

    /** */
    @Test
    public void testNodeJoinDuringActivationStateTransition() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        grid(0).cluster().state(INACTIVE);

        stopGrid(2);

        grid(0).cluster().state(ACTIVE);
        grid(0).cluster().state(INACTIVE);

        stopGrid(1);
        stopGrid(3);

        startNodeWithPdsCleared(1);
        startNodeWithPdsCleared(3);

        spi(grid(1)).blockMessages(GridDhtPartitionsSingleMessage.class, grid(0).name());

        CountDownLatch node2JoinedEvtListenedLatch = new CountDownLatch(3);

        for (Integer nodeIdx : Arrays.asList(0, 1, 3)) {
            grid(nodeIdx).events().localListen(event -> {
                if (Objects.equals(getTestIgniteInstanceName(2), ((DiscoveryEvent)event).eventNode().attribute(ATTR_IGNITE_INSTANCE_NAME)))
                    node2JoinedEvtListenedLatch.countDown();

                return true;
            }, EVT_NODE_JOINED);
        }

        runAsync(() -> grid(0).cluster().state(ACTIVE));

        spi(grid(1)).waitForBlocked();

        runAsync(() -> startGrid(2));

        assertTrue(node2JoinedEvtListenedLatch.await(getTestTimeout(), MILLISECONDS));

        spi(grid(1)).stopBlock();

        grid(0).resetLostPartitions(grid(0).cacheNames());

        Collection<Integer> lostParts0 = checkCacheLostParitionedDetected(CACHE_0);
        Collection<Integer> lostParts1 = checkCacheLostParitionedDetected(CACHE_1);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        checkKeysAvailableAfterLostPartitionsReset(CACHE_0, lostParts0);
        checkKeysAvailableAfterLostPartitionsReset(CACHE_1, lostParts1);
    }

    /** */
    @Test
    public void testClusterRestartWthEmptyPartitions() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        writeKeyToParition(0, CACHE_0, 0);
        writeKeyToParition(0, CACHE_0, 2);

        writeKeyToParition(0, CACHE_1, 1);
        writeKeyToParition(0, CACHE_1, 3);

        forceCheckpoint();

        grid(0).cluster().state(INACTIVE);

        stopAllGrids();

        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        checkNoLostPartitions();
    }

    /** */
    @Test
    public void testNodeJoinWithStaleCacheGroupRecoveryData() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        grid(0).cluster().state(INACTIVE);

        stopGrid(2);

        stopGrid(1);
        stopGrid(3);

        startNodeWithPdsCleared(1);
        startNodeWithPdsCleared(3);

        grid(0).cluster().state(ACTIVE);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        grid(0).cluster().state(INACTIVE);

        startGrid(2);

        grid(0).cluster().state(ACTIVE);

        checkNoLostPartitions();
    }

    /** */
    @Test
    public void testCoordinatorWithEmptyCacheGroupRecoveryData() throws Exception {
        startGrids(SERVER_NODES_CNT);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        fillCaches();

        stopGrid(0);

        grid(1).cluster().state(INACTIVE);

        stopAllGrids();

        startGrid(0);
        startGrid(2);
        startNodeWithPdsCleared(1);
        startNodeWithPdsCleared(3);
        startClientGrid(SERVER_NODES_CNT);

        grid(0).cluster().state(ACTIVE);

        checkCacheLostParitionedDetected(CACHE_0);
        checkCacheLostParitionedDetected(CACHE_1);

        grid(0).resetLostPartitions(grid(0).cacheNames());

        grid(0).cluster().state(INACTIVE);
        grid(0).cluster().state(ACTIVE);

        checkNoLostPartitions();
    }

    /** */
    private void checkNoLostPartitions() {
        for (int nodeIdx = 0; nodeIdx < SERVER_NODES_CNT + 1; nodeIdx++) {
            for (String cacheName : grid(nodeIdx).cacheNames())
                assertTrue(F.isEmpty(grid(nodeIdx).context().cache().cache(cacheName).lostPartitions()));
        }
    }

    /** */
    private void checkKeysAvailableAfterLostPartitionsReset(String cacheName, Collection<Integer> parts) {
        for (int nodeIdx = 0; nodeIdx < SERVER_NODES_CNT + 1; nodeIdx++) {
            for (Integer part : parts)
                assertNull(readKeyFromPartition(nodeIdx, cacheName, part));
        }
    }

    /** */
    private Collection<Integer> checkCacheLostParitionedDetected(String cacheName) {
        List<Collection<Integer>> nodeLostParts = new ArrayList<>();

        for (int i = 0; i < SERVER_NODES_CNT + 1; i++)
            nodeLostParts.add(grid(i).context().cache().cache(cacheName).lostPartitions());

        assertFalse(F.isEmpty(nodeLostParts));

        Collection<Integer> lostPars = nodeLostParts.get(0);

        for (int i = 1; i < nodeLostParts.size(); i++)
            assertEquals(lostPars, nodeLostParts.get(i));

        for (int nodeIdx = 0; nodeIdx < SERVER_NODES_CNT + 1; nodeIdx++) {
            for (int part : lostPars) {
                int finalNodeIdx = nodeIdx;

                assertThrowsAnyCause(
                    log,
                    () -> readKeyFromPartition(finalNodeIdx, cacheName, part),
                    CacheInvalidStateException.class,
                    "Failed to execute the cache operation (all partition owners have left the grid");
            }
        }

        return lostPars;
    }

    /** */
    private CacheConfiguration<Integer, Integer> createCacheConfiguration(String name) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(name)
            .setPartitionLossPolicy(READ_ONLY_SAFE)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction()
                .setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter("CELL"))
                .setPartitions(4));
    }

    /** */
    private void writeKeyToParition(int nodeIdx, String cacheName, int part) {
        for (int key = 0; key < CACHE_KEYS_CNT; key++) {
            if (grid(0).affinity(cacheName).partition(key) == part) {
                grid(nodeIdx).<Integer, Integer>cache(cacheName).put(key, key);

                return;
            }
        }

        throw new IllegalStateException();
    }

    /** */
    private Integer readKeyFromPartition(int nodeIdx, String cacheName, int part) {
        for (int key = 0; key < CACHE_KEYS_CNT; key++) {
            if (grid(0).affinity(cacheName).partition(key) == part)
                return grid(nodeIdx).<Integer, Integer>cache(cacheName).get(key);
        }

        throw new IllegalStateException();
    }

    /** */
    private void startNodeWithPdsCleared(int idx) throws Exception {
        U.delete(workDirectory(idx));

        startGrid(idx);
    }

    /** */
    private void fillCaches() throws Exception {
        for (String cacheName : grid(0).cacheNames()) {
            for (int i = 0; i < CACHE_KEYS_CNT; i++)
                grid(0).cache(cacheName).put(i, i);
        }

        forceCheckpoint();
    }

    /** */
    private File workDirectory(int idx) {
        return new File(getIgniteHome() + "/work_" + getTestIgniteInstanceName(idx));
    }
}
