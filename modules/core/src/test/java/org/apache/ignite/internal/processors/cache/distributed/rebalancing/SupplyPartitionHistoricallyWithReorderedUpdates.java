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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks a historical rebalance on atomic cache, when demand node has re-ordered updates.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class SupplyPartitionHistoricallyWithReorderedUpdates extends GridCommonAbstractTest {
    /** Historical iterator problem. */
    private static String HISTORICAL_ITERATOR_PROBLEM = "Historical iterator tries to iterate WAL out of reservation";

    /** Listening logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(600_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setAffinity(new TestAffinity(getTestIgniteInstanceName(0), getTestIgniteInstanceName(1))));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog = new ListeningTestLogger(log);
    }

    /**
     * Backup node loses one entry, after this it will restore state through historical rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLoosingCreateSupplyLatestUpdates() throws Exception {
        restartsBackupWithReorderedUpdate(false, false);
    }

    /**
     * Backup node loses one update entry, after this it will restore state through historical rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLoosingUpdateSupplyLatestUpdates() throws Exception {
        restartsBackupWithReorderedUpdate(true, false);
    }

    /**
     * Checks scenario, where a WAL pointer with an additional margin goes out of the bound of a preloading reservation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMarginGoesOutOfReservation() throws Exception {
        restartsBackupWithReorderedUpdate(true, true);
    }

    /**
     * Starts two nodes (first primary, second backup), reorders update on a backup and after restarts the backup node.
     *
     * @param updateExistedKys True for reordered update existing key, false reordered creating new entries.
     * @param isCpRequiredBeforeStopNode If the parameter is true then checkpoint required, otherwise not.
     * @throws Exception If failed.
     */
    private void restartsBackupWithReorderedUpdate(boolean updateExistedKys, boolean isCpRequiredBeforeStopNode) throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        AtomicBoolean blocked = new AtomicBoolean();

        TestRecordingCommunicationSpi.spi(ignite0).blockMessages((node, msg) ->
            msg instanceof GridDhtAtomicSingleUpdateRequest &&
                getTestIgniteInstanceName(1).equals(node.consistentId()) &&
                blocked.compareAndSet(false, true));

        if (updateExistedKys) {
            for (int i = 5; i < 10; i++)
                cache.put(i, i + 1);
        }
        else {
            for (int i = 15; i < 20; i++)
                cache.put(i, i);
        }

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        if (isCpRequiredBeforeStopNode)
            forceCheckpoint();

        stopGrid(1);

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        for (int i = 20; i < 30; i++)
            cache.put(i, i);

        forceCheckpoint(ignite0);

        LogListener lsnr = LogListener.matches(HISTORICAL_ITERATOR_PROBLEM).build();

        listeningLog.registerListener(lsnr);

        startGrid(1);

        awaitPartitionMapExchange();

        assertFalse(lsnr.check());

        if (!isCpRequiredBeforeStopNode)
            assertPartitionsSame(idleVerify(ignite0, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests affinity function with one partition. This implementation maps primary partition to first node and backup
     * partition to second.
     */
    public static class TestAffinity extends RendezvousAffinityFunction {
        /** Nodes consistence ids. */
        String[] nodeConsistentIds;

        /**
         * @param nodes Nodes consistence ids.
         */
        public TestAffinity(String... nodes) {
            super(false, 1);

            this.nodeConsistentIds = nodes;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            int nodes = affCtx.currentTopologySnapshot().size();

            if (nodes != 2)
                return super.assignPartitions(affCtx);

            List<List<ClusterNode>> assignment = new ArrayList<>();

            assignment.add(new ArrayList<>(2));

            assignment.get(0).add(null);
            assignment.get(0).add(null);

            for (ClusterNode node : affCtx.currentTopologySnapshot())
                if (nodeConsistentIds[0].equals(node.consistentId()))
                    assignment.get(0).set(0, node);
                else if (nodeConsistentIds[1].equals(node.consistentId()))
                    assignment.get(0).set(1, node);
                else
                    throw new AssertionError("Unexpected node consistent id is " + node.consistentId());

            return assignment;
        }
    }
}
