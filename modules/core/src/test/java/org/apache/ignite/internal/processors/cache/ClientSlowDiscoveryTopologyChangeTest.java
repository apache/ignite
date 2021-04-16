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

package org.apache.ignite.internal.processors.cache;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ClientSlowDiscoveryTopologyChangeTest extends ClientSlowDiscoveryAbstractTest {
    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test check that client join works well if cache configured on it stopped on server nodes
     * but discovery event about cache stop is not delivered to client node immediately.
     * When client node joins to cluster it sends SingleMessage to coordinator.
     * During this time topology on server nodes can be changed,
     * because client exchange doesn't require acknowledgement for SingleMessage on coordinator.
     * Delay is simulated by blocking sending this SingleMessage and resume sending after topology is changed.
     */
    @Test
    public void testClientJoinAndCacheStop() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(3);

        awaitPartitionMapExchange();

        for (int k = 0; k < 64; k++)
            crd.cache(CACHE_NAME).put(k, k);

        TestRecordingCommunicationSpi clientCommSpi = new TestRecordingCommunicationSpi();

        // Delay client join process.
        clientCommSpi.blockMessages((node, msg) -> {
            if (!(msg instanceof GridDhtPartitionsSingleMessage))
                return false;

            GridDhtPartitionsSingleMessage singleMsg = (GridDhtPartitionsSingleMessage) msg;

            return Optional.ofNullable(singleMsg.exchangeId())
                .map(GridDhtPartitionExchangeId::topologyVersion)
                .filter(topVer -> topVer.equals(new AffinityTopologyVersion(4, 0)))
                .isPresent();
        });

        communicationSpiSupplier = () -> clientCommSpi;

        CustomMessageInterceptingDiscoverySpi clientDiscoSpi = new CustomMessageInterceptingDiscoverySpi();

        CountDownLatch clientDiscoSpiBlock = new CountDownLatch(1);

        // Delay cache destroying on client node.
        clientDiscoSpi.interceptor = (msg) -> {
            if (!(msg instanceof DynamicCacheChangeBatch))
                return;

            DynamicCacheChangeBatch cacheChangeBatch = (DynamicCacheChangeBatch) msg;

            boolean hasCacheStopReq = cacheChangeBatch.requests().stream()
                .anyMatch(req -> req.stop() && req.cacheName().equals(CACHE_NAME));

            if (hasCacheStopReq)
                U.awaitQuiet(clientDiscoSpiBlock);
        };

        discoverySpiSupplier = () -> clientDiscoSpi;

        IgniteInternalFuture<IgniteEx> clientStartFut = GridTestUtils.runAsync(() -> startClientGrid(3));

        // Wait till client node starts join process.
        clientCommSpi.waitForBlocked();

        // Destroy cache on server nodes.
        crd.destroyCache(CACHE_NAME);

        // Resume client join.
        clientCommSpi.stopBlock();

        // Client join should succeed.
        IgniteEx client = clientStartFut.get();

        IgniteCache<Object, Object> clientCache = client.cache(CACHE_NAME);

        Assert.assertNotNull("Cache should exists on client node", clientCache);

        IgniteInternalFuture<?> cacheGet = GridTestUtils.runAsync(() -> clientCache.get(0));

        try {
            cacheGet.get(5_000); // Reasonable timeout.

            fail("Cache get operation should throw " + CacheStoppedException.class);
        }
        catch (Exception e) {
            assertTrue("Got unexpected exception during cache get " + e,
                X.hasCause(e, CacheStoppedException.class));
        }
        finally {
            // Resume processing cache destroy on client node.
            clientDiscoSpiBlock.countDown();
        }

        // Wait till cache destroyed on client node.
        GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion topVer = client.context().cache().context().exchange().lastFinishedFuture()
                .topologyVersion();

            // Cache destroy version.
            return topVer.equals(new AffinityTopologyVersion(4, 1));
        }, 5_000); // Reasonable timeout.

        Assert.assertNull("Cache should be destroyed on client node", client.cache(CACHE_NAME));
    }
}
