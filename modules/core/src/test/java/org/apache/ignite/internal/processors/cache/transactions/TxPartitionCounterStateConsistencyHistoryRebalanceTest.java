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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Test partitions consistency in various scenarios.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class TxPartitionCounterStateConsistencyHistoryRebalanceTest extends TxPartitionCounterStateConsistencyTest {
    /**
     * Tests reproduces the problem: if partition is selected for full rebalancing for some reason and it was
     * rebalancing before it should always be cleared first OR deletes on supplier will not be visible on demander
     * causing partition desync.
     *
     * TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11799
     *
     * @throws Exception
     */
    @Test
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_MovingPartitionNotCleared() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(SERVER_NODES);

        int[] primaryParts = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.cluster().localNode());

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        List<Integer> p1Keys = partitionKeys(cache, primaryParts[0], 2, 0);
        List<Integer> p2Keys = partitionKeys(cache, primaryParts[1], 2, 0);

        cache.put(p1Keys.get(0), 0); // Will be historically rebalanced.
        cache.put(p1Keys.get(1), 1);

        forceCheckpoint();

        Ignite backup = backupNode(p1Keys.get(0), DEFAULT_CACHE_NAME);

        assertTrue(backupNodes(p2Keys.get(0), DEFAULT_CACHE_NAME).contains(backup));

        stopGrid(true, backup.name());

        cache.remove(p1Keys.get(1));
        cache.put(p2Keys.get(1), 1); // Will be fully rebalanced.

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        // Prevent rebalance completion.
        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                String name = (String)node.attributes().get(ATTR_IGNITE_INSTANCE_NAME);

                if (name.equals(backup.name()) && msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;

                    Map<Integer, CacheEntryInfoCollection> infos = U.field(msg0, "infos");

                    return infos.keySet().contains(primaryParts[0]); // Delay historical rebalance.
                }

                return false;
            }
        });

        IgniteEx backup2 = startGrid(backup.name());

        doSleep(5000);

        spi.waitForBlocked();
        spi.stopBlock();

        // While message is delayed topology version shouldn't change to ideal.
        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * Same as {@link #testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_MovingPartitionNotCleared} but
     * new coordinator is demander.
     *
     * @throws Exception
     */
    @Test
    public void testPartitionConsistencyCancelledRebalanceCoordinatorIsDemander() throws Exception {
        backups = 2;

        Ignite crd = startGrids(SERVER_NODES);

        crd.cluster().active(true);

        int[] primaryParts = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.cluster().localNode());

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        List<Integer> p1Keys = partitionKeys(cache, primaryParts[0], 2, 0);
        List<Integer> p2Keys = partitionKeys(cache, primaryParts[1], 2, 0);

        assertTrue(crd.affinity(DEFAULT_CACHE_NAME).isPrimary(crd.cluster().localNode(), p1Keys.get(0)));
        assertTrue(crd.affinity(DEFAULT_CACHE_NAME).isPrimary(crd.cluster().localNode(), p2Keys.get(0)));

        final String primName = crd.name();

        cache.put(p1Keys.get(0), 0); // Will be historically rebalanced.
        cache.put(p1Keys.get(1), 1);

        forceCheckpoint();

        List<Ignite> backups = Arrays.asList(grid(1), grid(2));

        assertFalse(backups.contains(crd));

        final String demanderName = backups.get(0).name();

        stopGrid(true, demanderName);

        // Create counters delta.
        cache.remove(p1Keys.get(1));
        cache.put(p2Keys.get(1), 1); // Will be fully rebalanced.

        stopAllGrids();

        crd = startGrid(0);
        startGrid(1);
        startGrid(2);

        TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(crd);

        // Block all rebalance from crd.
        crdSpi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

        crd.cluster().active(true);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    crdSpi.waitForBlocked();

                    // Stop before supplying rebalance. New rebalance must start with second backup as supplier
                    // doing full rebalance.
                    stopGrid(primName);
                }
                catch (InterruptedException e) {
                    fail();
                }
            }
        });

        fut.get();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(demanderName), DEFAULT_CACHE_NAME));
    }
}
