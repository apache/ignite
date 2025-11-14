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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test partition awareness of thin client in multi data-center environment.
 */
public class ThinClientPartitionAwarenessMultiDcTest extends ThinClientAbstractPartitionAwarenessTest {
    /** */
    private static final String DC1 = "dc1";

    /** */
    private static final String DC2 = "dc2";

    /** */
    private static final String DC3 = "dc3";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0, DC1);
        startGrid(1, DC1);

        startGrid(2, DC2);
        startGrid(3, DC2);
    }

    /** */
    private IgniteEx startGrid(int idx, String dcId) throws Exception {
        return startGrid(getConfiguration(getTestIgniteInstanceName(idx))
            .setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId)));
    }

    /** */
    private void initClient(
        ClientConfiguration clientCfg,
        String dcId,
        int... chIdxs
    ) throws IgniteInterruptedCheckedException {
        initClient(clientCfg.setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId)), chIdxs);
    }

    /** */
    @Test
    public void testPartitionAwarenessRequests() throws Exception {
        List<IgniteEx> curDcNodes = F.asList(grid(0), grid(1));
        List<IgniteEx> anotherDcNodes = F.asList(grid(2), grid(3));

        initClient(getClientConfiguration(0), DC1, 0, 1, 2, 3);

        checkDcAwarePaRequests("test0", CacheWriteSynchronizationMode.PRIMARY_SYNC, true,
            curDcNodes, anotherDcNodes, false);

        checkDcAwarePaRequests("test1", CacheWriteSynchronizationMode.FULL_SYNC, false,
            curDcNodes, anotherDcNodes, false);

        checkDcAwarePaRequests("test2", CacheWriteSynchronizationMode.FULL_SYNC, true,
            curDcNodes, anotherDcNodes, true);

        checkDcAwarePaRequests("test3", CacheWriteSynchronizationMode.FULL_ASYNC, true,
            curDcNodes, anotherDcNodes, true);
    }

    /** */
    @Test
    public void testPartitionAwarenessRequestsNoNodesInDc() throws Exception {
        initClient(getClientConfiguration(0), DC3, 0, 1, 2, 3);

        checkDcAwarePaRequests("test4", CacheWriteSynchronizationMode.FULL_SYNC, true,
            Collections.emptyList(), F.asList(grid(0), grid(1), grid(2), grid(3)), false);
    }

    /** */
    private void checkDcAwarePaRequests(
        String cacheName,
        CacheWriteSynchronizationMode mode,
        boolean readFromBackup,
        List<IgniteEx> curDcNodes,
        List<IgniteEx> otherDcNodes,
        boolean expReqToBackup
    ) throws Exception {
        grid(0).createCache(new CacheConfiguration<>(cacheName)
            .setBackups(1)
            .setWriteSynchronizationMode(mode)
            .setReadFromBackup(readFromBackup)
            .setAffinity(new RendezvousAffinityFunction().setAffinityBackupFilter(
                new ClusterNodeAttributeAffinityBackupFilter(IgniteSystemProperties.IGNITE_DATA_CENTER_ID)))
        );

        ClientCache<Object, Object> cache = client.cache(cacheName);

        // Init partitions request.
        cache.put(0, 0);

        // Ignore first put and partitions request.
        opsQueue.clear();

        Set<Integer> curDcNodeIdxs = new HashSet<>();

        for (IgniteEx primaryNode : curDcNodes) {
            curDcNodeIdxs.add(getTestIgniteInstanceIndex(primaryNode.name()));

            List<Integer> keys = primaryKeys(primaryNode.cache(cacheName), 10);

            for (Integer key : keys) {
                int primaryNodeIdx = getTestIgniteInstanceIndex(primaryNode.name());

                // If primary in current DC, read and write requests always sent to primary node.
                cache.put(key, 0);

                assertOpOnChannel(channels[primaryNodeIdx], ClientOperation.CACHE_PUT);

                cache.get(key);

                assertOpOnChannel(channels[primaryNodeIdx], ClientOperation.CACHE_GET);
            }
        }

        for (IgniteEx primaryNode : otherDcNodes) {
            List<Integer> keys = primaryKeys(primaryNode.cache(cacheName), 10);

            for (Integer key : keys) {
                int primaryNodeIdx = getTestIgniteInstanceIndex(primaryNode.name());

                cache.put(key, 0);

                // If primary in another DC, write requests always sent to primary node.
                assertOpOnChannel(channels[primaryNodeIdx], ClientOperation.CACHE_PUT);

                int backupNodeIdx = getTestIgniteInstanceIndex(backupNode(key, cacheName).name());

                assertTrue(curDcNodeIdxs.isEmpty() || curDcNodeIdxs.contains(backupNodeIdx));

                // But read requests can be sent to current DC backup node.
                cache.get(key);

                assertOpOnChannel(channels[expReqToBackup ? backupNodeIdx : primaryNodeIdx], ClientOperation.CACHE_GET);

                cache.containsKey(key);

                assertOpOnChannel(channels[expReqToBackup ? backupNodeIdx : primaryNodeIdx], ClientOperation.CACHE_CONTAINS_KEY);

                int partIdx = primaryNode.context().affinity().partition(cacheName, key);

                cache.query(new ScanQuery<>().setPartition(partIdx)).getAll();

                assertOpOnChannel(channels[expReqToBackup ? backupNodeIdx : primaryNodeIdx], ClientOperation.QUERY_SCAN);
            }
        }
    }

    /** */
    @Test
    public void testNonPartitionAwarenessRequests() throws Exception {
        String cacheName = "txCache0";

        grid(0).createCache(new CacheConfiguration<>(cacheName).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        checkDcAwareNonPaRequests(cacheName, DC1, 0, 1);
        checkDcAwareNonPaRequests(cacheName, DC2, 2, 3);
    }

    /** */
    @Test
    public void testNonPartitionAwarenessRequestsNoNodesInDc() throws Exception {
        String cacheName = "txCache1";

        grid(0).createCache(new CacheConfiguration<>(cacheName).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        checkDcAwareNonPaRequests(cacheName, DC3, 0, 1, 2, 3);
    }

    /** */
    private void checkDcAwareNonPaRequests(String cacheName, String dcId, int... expNodeIdxs) throws Exception {
        initClient(getClientConfiguration(0), dcId, 0, 1, 2, 3);

        ClientCache<Object, Object> cache = client.cache(cacheName);

        cache.put(0, 0);

        opsQueue.clear();

        for (int i = 0; i < 10; i++) {
            cache.query(new ScanQuery<>()).getAll();

            int channelIdx = nextOpChannelIdx();

            assertTrue(F.contains(expNodeIdxs, channelIdx));

            assertTrue("Ops queue not empty: " + opsQueue, F.isEmpty(opsQueue));

            client.cluster().state();

            channelIdx = nextOpChannelIdx();

            assertTrue(F.contains(expNodeIdxs, channelIdx));

            assertTrue("Ops queue not empty: " + opsQueue, F.isEmpty(opsQueue));

            try (ClientTransaction tx = client.transactions().txStart()) {
                cache.put(ThreadLocalRandom.current().nextInt(10), 0);
                cache.get(ThreadLocalRandom.current().nextInt(10));

                tx.commit();
            }

            while (true) {
                channelIdx = nextOpChannelIdx();

                if (channelIdx < 0)
                    break;

                assertTrue(F.contains(expNodeIdxs, channelIdx));
            }
        }
    }
}
