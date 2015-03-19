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

package org.apache.ignite.loadtests.client;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.client.*;
import org.apache.ignite.internal.client.impl.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Makes a long run to ensure stability and absence of memory leaks.
 */
public class ClientTcpSslLoadTest extends ClientTcpSslMultiThreadedSelfTest {
    /** Test duration. */
    private static final long TEST_RUN_TIME = 8 * 60 * 60 * 1000;

    /** Statistics output interval. */
    private static final long STATISTICS_PRINT_INTERVAL = 5 * 60 * 1000;

    /** Time to let connections closed by idle. */
    private static final long RELAX_INTERVAL = 60 * 1000;

    /** Thread count to run tests. */
    private static final int THREAD_CNT = 20;

    /**
     * @throws Exception If failed.
     */
    public void testLongRun() throws Exception {
        long start = System.currentTimeMillis();

        long lastPrint = start;

        do {
            clearCaches();

            testMultithreadedTaskRun();

            testMultithreadedCachePut();

            long now = System.currentTimeMillis();

            if (now - lastPrint > STATISTICS_PRINT_INTERVAL) {
                info(">>>>>>> Running test for " + ((now - start) / 1000) + " seconds.");

                lastPrint = now;
            }

            // Let idle check work.
            U.sleep(RELAX_INTERVAL);
        }
        while (System.currentTimeMillis() - start < TEST_RUN_TIME);
    }

    /** {@inheritDoc} */
    @Override protected int topologyRefreshFrequency() {
        return 5000;
    }

    /** {@inheritDoc} */
    @Override protected int maxConnectionIdleTime() {
        return topologyRefreshFrequency() / 5;
    }

    /**
     * Clears caches on all nodes.
     */
    @SuppressWarnings("ConstantConditions")
    private void clearCaches() {
        for (int i = 0; i < NODES_CNT; i++)
            try {
                grid(i).jcache(PARTITIONED_CACHE_NAME).clear();
            } catch (IgniteException e) {
                log.error("Cache clear failed.", e);
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void test6Affinity() throws Exception {
        GridClientData cache = client.data(PARTITIONED_CACHE_NAME);
        UUID nodeId = cache.affinity("6");

        info("Affinity node: " + nodeId);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedCachePut() throws Exception {
        final AtomicLong keyCnt = new AtomicLong();

        final AtomicReference<Exception> err = new AtomicReference<>();

        final ConcurrentMap<String, T2<UUID, String>> puts = new ConcurrentHashMap<>();

        final Map<UUID, Ignite> gridMap = new HashMap<>();

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = grid(i);

            gridMap.put(g.cluster().localNode().id(), g);
        }

        final Ignite ignite = F.first(gridMap.values());

        assertEquals(NODES_CNT, client.compute().refreshTopology(false, false).size());

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings("OverlyStrongTypeCast")
            @Override public void run() {
                try {
                    GridClientData cache = client.data(PARTITIONED_CACHE_NAME);

                    assertEquals(NODES_CNT, ((GridClientDataImpl)cache).projectionNodes().size());

                    long rawKey;

                    while ((rawKey = keyCnt.getAndIncrement()) < cachePutCount()) {
                        String key = String.valueOf(rawKey);

                        UUID nodeId = cache.affinity(key);

                        String val = "val" + rawKey;

                        if (cache.put(key, val)) {
                            T2<UUID, String> old = puts.putIfAbsent(key, new T2<>(nodeId, val));

                            assert old == null : "Map contained entry [key=" + rawKey + ", entry=" + old + ']';
                        }
                    }
                }
                catch (Exception e) {
                    err.compareAndSet(null, e);
                }
            }
        }, THREAD_CNT, "client-cache-put");

        fut.get();

        if (err.get() != null)
            throw new Exception(err.get());

        assertEquals(cachePutCount(), puts.size());

        // Now check that all puts went to primary nodes.
        for (long i = 0; i < cachePutCount(); i++) {
            String key = String.valueOf(i);

            ClusterNode node = ignite.cluster().mapKeyToNode(PARTITIONED_CACHE_NAME, key);

            if (!puts.get(key).get2().equals(gridMap.get(node.id()).jcache(PARTITIONED_CACHE_NAME).localPeek(key, CachePeekMode.ONHEAP))) {
                // printAffinityState(gridMap.values());

                failNotEquals("Node don't have value for key [nodeId=" + node.id() + ", key=" + key + "]",
                    puts.get(key).get2(), gridMap.get(node.id()).jcache(PARTITIONED_CACHE_NAME).localPeek(key, CachePeekMode.ONHEAP));
            }


            UUID curAffNode = client.data(PARTITIONED_CACHE_NAME).affinity(key);

            // Check that no other nodes see this key.
            for (UUID id : gridMap.keySet()) {
                if (!id.equals(curAffNode) && !id.equals(node.id()))
                    assertNull("Got value in near cache.", gridMap.get(id).jcache(PARTITIONED_CACHE_NAME).localPeek(key, CachePeekMode.ONHEAP));
            }
        }

        for (Ignite g : gridMap.values())
            g.jcache(PARTITIONED_CACHE_NAME).clear();
    }
}
