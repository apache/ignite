/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.HashSet;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 * The tets demonstrate the synchronization of shifting between topology versions on clusters during rebalance.
 * Two nodes join toopology simultaneously, and the rebalancing topology on the entire cluster will be switched
 * only when both rebalancings for the nodes finish.
 */
public class ReplicationCacheConsistencyOnUnstableTopologyTest extends GridCommonAbstractTest {
    /**
     * Cache mode.
     */
    private CacheMode cacheMode;

    /**
     * Cache write synchronization mode.
     */
    private CacheWriteSynchronizationMode writeSynchronizationMode;

    /**
     * True if the cache read operation can execute on backup replicas.
     */
    private boolean readFromBackup;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 3))
                .setCacheMode(cacheMode)
                .setBackups(2)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(writeSynchronizationMode)
                .setReadFromBackup(readFromBackup));
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Test
    public void testReplicatedFullSync() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    @Test
    public void testReplicatedFullSyncReadFromBackup() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    @Test
    public void testReplicatedPrimarySync() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.PRIMARY_SYNC, false);
    }

    @Test
    public void testReplicatedPrimarySyncReadFromBackup() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.PRIMARY_SYNC, true);
    }

    @Test
    public void testReplicatedFullAsync() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_ASYNC, false);
    }

    @Test
    public void testReplicatedFullAsyncReadFromBackup() throws Exception {
        process(CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_ASYNC, true);
    }

    @Test
    public void testPartitionedFullSync() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    @Test
    public void testPartitionedFullSyncReadFromBackup() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    @Test
    public void testPartitionedPrimarySync() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.PRIMARY_SYNC, false);
    }

    @Test
    public void testPartitionedPrimarySyncReadFromBackup() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.PRIMARY_SYNC, true);
    }

    @Test
    public void testPartitionedFullAsync() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_ASYNC, false);
    }

    @Test
    public void testPartitionedFullAsyncReadFromBackup() throws Exception {
        process(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_ASYNC, true);
    }

    /**
     * Executes a test scenario.
     *
     * @param cacheMode                Cache mode.
     * @param writeSynchronizationMode Cache write synchronization mode.
     * @param readFromBackup           True if the cache read operation can execute on backup replicas.
     * @throws Exception If fail.
     */
    private void process(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSynchronizationMode,
        boolean readFromBackup
    ) throws Exception {
        this.cacheMode = cacheMode;
        this.writeSynchronizationMode = writeSynchronizationMode;
        this.readFromBackup = readFromBackup;

        IgniteEx ignite = startGrids(3);

        ignite.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        assertEquals(0, ignite.cache(DEFAULT_CACHE_NAME).size());

        IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME);

        streamer.allowOverwrite(false);

        for (int i = 0; i < 20; i++) {
            streamer.addData(i, i);
        }

        streamer.flush();

        ignite(1).close();

        for (int i = 20; i < 40; i++) {
            streamer.addData(i, i);
        }

        streamer.flush();

        ignite(2).close();

        for (int i = 40; i < 60; i++) {
            streamer.addData(i, i);
        }

        streamer.close();

        spi(ignite).blockMessages((node, message) -> {
            if (message instanceof GridDhtPartitionSupplyMessage && testNodeName(2).equals(node.consistentId())) {
                GridDhtPartitionSupplyMessage supplyMessage = ((GridDhtPartitionSupplyMessage)message);

                return supplyMessage.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
            }

            return false;
        });

        startGrid(1);
        startGrid(2);

        AffinityTopologyVersion rebTopVer = getRebalancedTopVer(ignite);

        assertEquals(rebTopVer, getRebalancedTopVer(ignite(1)));
        assertEquals(rebTopVer, getRebalancedTopVer(ignite(2)));

        HashSet<Integer> keysToUpdate = new HashSet<>(9);

        // The keys were loaded on the nodes.
        keysToUpdate.add(partitionKeys(0, 0, 20));
        keysToUpdate.add(partitionKeys(1, 0, 20));
        keysToUpdate.add(partitionKeys(2, 0, 20));

        //The keys were loaded on topology without one node.
        keysToUpdate.add(partitionKeys(0, 20, 40));
        keysToUpdate.add(partitionKeys(1, 20, 40));
        keysToUpdate.add(partitionKeys(2, 20, 40));

        // The keys were loaded on topology without two nodes.
        keysToUpdate.add(partitionKeys(0, 40, 60));
        keysToUpdate.add(partitionKeys(1, 40, 60));
        keysToUpdate.add(partitionKeys(2, 40, 60));

        for (Integer key : keysToUpdate) {
            info("Intention to invike [key: " + key +
                " part: " + ignite.affinity(DEFAULT_CACHE_NAME).partition(key) +
                " primary: " + ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key) + ']');
        }

        HashMap<Integer, EntryProcessor<Integer, Integer, Void>> invokes = new HashMap<>(keysToUpdate.size());

        for (Integer key : keysToUpdate) {
            invokes.put(key, new TestEntryProcessor(100));
        }

        checkTopology(3);

        ignite.<Integer, Integer>cache(DEFAULT_CACHE_NAME).invokeAll(invokes);

        spi(ignite).stopBlock();

        awaitPartitionMapExchange();

        rebTopVer = getRebalancedTopVer(ignite);

        assertEquals(rebTopVer, getRebalancedTopVer(ignite(1)));
        assertEquals(rebTopVer, getRebalancedTopVer(ignite(2)));

        assertPartitionsSame(idleVerify(ignite, DEFAULT_CACHE_NAME));
    }

    /**
     * Finds a partition key.
     *
     * @param part Partiton.
     * @param from Left search bound.
     * @param to   Right search bound.
     * @return A keyu.
     */
    protected Integer partitionKeys(int part, int from, int to) {
        Affinity<Integer> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

        for (int k = from; k < to; k++) {
            if (aff.partition(k) == part) {
                return k;
            }
        }

        throw new AssertionError("Key was not found [pat=" + part + ", from=" + from + ", to=" + to + ']');
    }

    /**
     * Gets rebalance topology version for the Ignite instance.
     *
     * @param instance Ignite instance.
     * @return Topologyy version.
     */
    private static AffinityTopologyVersion getRebalancedTopVer(IgniteEx instance) {
        return ((GridDhtPartitionTopologyImpl)instance.context().cache()
            .cache(DEFAULT_CACHE_NAME).context().topology()).getRebalancedTopVer();
    }

    /**
     * The entry processor is intended to update a value when the previous one exists.
     */
    private static class TestEntryProcessor implements EntryProcessor<Integer, Integer, Void> {
        @IgniteInstanceResource
        Ignite ignite;

        private final Integer val;

        public TestEntryProcessor(Integer val) {
            this.val = val;
        }

        @Override public Void process(
            MutableEntry<Integer, Integer> mutableEntry,
            Object... objects
        ) throws EntryProcessorException {
            log.info("Updating entry [from=" + mutableEntry.getValue() + ", to=" + val + ']');

            if (!mutableEntry.exists())
                return null;

            Integer entryVal = mutableEntry.getValue();

            if (entryVal == null)
                return null;

            mutableEntry.setValue(val);

            log.info("Updated entry [from=" + entryVal + ", to=" + val + ']');

            return null;
        }
    }
}
