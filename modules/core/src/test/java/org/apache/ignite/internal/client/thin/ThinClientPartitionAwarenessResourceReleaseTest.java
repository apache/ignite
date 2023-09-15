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

import java.lang.management.ThreadInfo;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientPartitionAwarenessMapper;
import org.apache.ignite.client.ClientPartitionAwarenessMapperFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test resource releasing by thin client.
 */
public class ThinClientPartitionAwarenessResourceReleaseTest extends ThinClientAbstractPartitionAwarenessTest {
    /** Worker thread prefix. */
    private static final String THREAD_PREFIX = "thin-client-channel";

    /**
     * Test that resources are correctly released after closing client with partition awareness.
     */
    @Test
    public void testResourcesReleasedAfterClientClosed() throws Exception {
        startGrids(2);

        initClient(getClientConfiguration(0, 1), 0, 1);

        ClientCache<Integer, Integer> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        assertFalse(channels[0].isClosed());
        assertFalse(channels[1].isClosed());
        assertEquals(1, threadsCount(THREAD_PREFIX));

        client.close();

        assertTrue(channels[0].isClosed());
        assertTrue(channels[1].isClosed());
        assertTrue(GridTestUtils.waitForCondition(() -> threadsCount(THREAD_PREFIX) == 0, 1_000L));
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testResourcesReleasedAfterCacheDestroyed() throws Exception {
        int cacheId = CU.cacheId(PART_CUSTOM_AFFINITY_CACHE_NAME);
        startGrids(2);

        initClient(getClientConfiguration(0, 1)
            .setPartitionAwarenessMapperFactory(new ClientPartitionAwarenessMapperFactory() {
                /** {@inheritDoc} */
                @Override public ClientPartitionAwarenessMapper create(String cacheName, int partitions) {
                    assertEquals(cacheName, PART_CUSTOM_AFFINITY_CACHE_NAME);

                    AffinityFunction aff = new RendezvousAffinityFunction(false, partitions);

                    return aff::partition;
                }
            }), 0, 1);

        ClientCache<Object, Object> clientCache = client.cache(PART_CUSTOM_AFFINITY_CACHE_NAME);
        IgniteInternalCache<Object, Object> gridCache = grid(0).context().cache().cache(PART_CUSTOM_AFFINITY_CACHE_NAME);

        clientCache.put(0, 0);
        TestTcpClientChannel opCh = affinityChannel(0, gridCache);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++)
            clientCache.put(i, i);

        ClientCacheAffinityContext affCtx = ((TcpIgniteClient)client).reliableChannel().affinityContext();
        AffinityTopologyVersion ver = affCtx.currentMapping().topologyVersion();

        grid(0).destroyCache(PART_CUSTOM_AFFINITY_CACHE_NAME);
        awaitPartitionMapExchange();

        // Cache destroyed, but mappings still exist on the client side.
        assertEquals(opCh.serverNodeId(), affCtx.affinityNode(cacheId, Integer.valueOf(0)));

        client.cache(PART_CACHE_NAME).put(1, 1);

        // await mappings updated.
        assertTrue(GridTestUtils.waitForCondition(() -> {
            ClientCacheAffinityMapping m = affCtx.currentMapping();

            if (m == null)
                return false;

            return m.topologyVersion().equals(ver.nextMinorVersion());
        }, 5_000L));

        // Mapping for previous caches become outdated and will be updated on the next request.
        assertNull(affCtx.currentMapping().affinityNode(cacheId, 0));

        // Trigger the next affinity mappings update. The outdated cache with custom affinity was added
        // to pending caches list and will be processed and cleared.
        client.cache(REPL_CACHE_NAME).put(2, 2);

        assertTrue(GridTestUtils.waitForCondition(affCtx.cacheKeyMapperFactoryMap::isEmpty, 5000L));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        if (client != null)
            client.close();
    }

    /**
     * Gets threads count with a given name.
     */
    private static int threadsCount(String name) {
        int cnt = 0;

        long[] threadIds = U.getThreadMx().getAllThreadIds();

        for (long id : threadIds) {
            ThreadInfo info = U.getThreadMx().getThreadInfo(id);

            if (info != null && info.getThreadState() != Thread.State.TERMINATED && info.getThreadName().contains(name))
                cnt++;
        }

        return cnt;
    }
}
