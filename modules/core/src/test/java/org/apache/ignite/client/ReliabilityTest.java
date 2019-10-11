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

package org.apache.ignite.client;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * High Availability tests.
 */
public class ReliabilityTest extends GridCommonAbstractTest {
    /**
     * Thin clint failover.
     */
    @Test
    public void testFailover() throws Exception {
        final int CLUSTER_SIZE = 3;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(CLUSTER_SIZE);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                 .setAddresses(cluster.clientAddresses().toArray(new String[CLUSTER_SIZE]))
             )
        ) {
            final Random rnd = new Random();

            final ClientCache<Integer, String> cache = client.getOrCreateCache(
                new ClientCacheConfiguration().setName("testFailover").setCacheMode(CacheMode.REPLICATED)
            );

            // Simple operation failover: put/get
            assertOnUnstableCluster(cluster, () -> {
                Integer key = rnd.nextInt();
                String val = key.toString();

                cache.put(key, val);

                String cachedVal = cache.get(key);

                assertEquals(val, cachedVal);
            });

            cache.clear();

            // Composite operation failover: query
            Map<Integer, String> data = IntStream.rangeClosed(1, 1000).boxed()
                .collect(Collectors.toMap(i -> i, i -> String.format("String %s", i)));

            assertOnUnstableCluster(cluster, () -> {
                cache.putAll(data);

                Query<Cache.Entry<Integer, String>> qry =
                    new ScanQuery<Integer, String>().setPageSize(data.size() / 10);

                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                    List<Cache.Entry<Integer, String>> res = cur.getAll();

                    assertEquals("Unexpected number of entries", data.size(), res.size());

                    Map<Integer, String> act = res.stream()
                        .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

                    assertEquals("Unexpected entries", data, act);
                }
            });

            // Client fails if all nodes go down
            cluster.close();

            boolean igniteUnavailable = false;

            try {
                cache.put(1, "1");
            }
            catch (ClientConnectionException ex) {
                igniteUnavailable = true;

                Throwable[] suppressed = ex.getSuppressed();

                assertEquals(suppressed.length, CLUSTER_SIZE - 1);

                assertTrue(Stream.of(suppressed).allMatch(t -> t instanceof ClientConnectionException));
            }

            assertTrue(igniteUnavailable);
        }
    }

    /**
     * Test single server failover.
     */
    @Test
    public void testSingleServerFailover() throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                 .setAddresses(cluster.clientAddresses().iterator().next()))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cache.put(0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            try {
                cache.put(0, 0);
            }
            catch (Exception expected) {
                // No-op.
            }

            // Recover after fail.
            cache.put(0, 0);
        }
    }

    /**
     * Test that failover doesn't lead to silent query inconsistency.
     */
    @Test
    public void testQueryConsistencyOnFailover() throws Exception {
        int CLUSTER_SIZE = 2;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(CLUSTER_SIZE);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                 .setAddresses(cluster.clientAddresses().toArray(new String[CLUSTER_SIZE])))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            cache.put(0, 0);
            cache.put(1, 1);

            Query<Cache.Entry<Integer, String>> qry = new ScanQuery<Integer, String>().setPageSize(1);

            try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                int cnt = 0;

                for (Iterator<Cache.Entry<Integer, String>> it = cur.iterator(); it.hasNext(); it.next()) {
                    cnt++;

                    if (cnt == 1) {
                        for (int i = 0; i < CLUSTER_SIZE; i++)
                            dropAllThinClientConnections(Ignition.allGrids().get(i));
                    }
                }

                fail("ClientReconnectedException must be thrown");
            }
            catch (ClientReconnectedException expected) {
                // No-op.
            }
        }
    }

    /**
     * Drop all thin client connections on given Ignite instance.
     *
     * @param ignite Ignite.
     */
    private void dropAllThinClientConnections(Ignite ignite) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(ignite.name(), "Clients",
            ClientListenerProcessor.class.getSimpleName());

        ClientProcessorMXBean mxBean = MBeanServerInvocationHandler.newProxyInstance(
            ManagementFactory.getPlatformMBeanServer(), mbeanName, ClientProcessorMXBean.class, true);

        mxBean.dropAllConnections();
    }

    /**
     * Run the closure while Ignite nodes keep failing/recovering several times.
     */
    private void assertOnUnstableCluster(LocalIgniteCluster cluster, Runnable clo) throws Exception {
        // Keep changing Ignite cluster topology by adding/removing nodes.
        final AtomicBoolean stopFlag = new AtomicBoolean(false);

        Future<?> topChangeFut = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                for (int i = 0; i < 5 && !stopFlag.get(); i++) {
                    while (cluster.size() != 1)
                        cluster.failNode();

                    while (cluster.size() != cluster.getInitialSize())
                        cluster.restoreNode();

                    awaitPartitionMapExchange();
                }
            }
            catch (InterruptedException ignore) {
                // No-op.
            }

            stopFlag.set(true);
        });

        // Use Ignite while nodes keep failing.
        try {
            while (!stopFlag.get())
                clo.run();

            topChangeFut.get();
        }
        finally {
            stopFlag.set(true);
        }
    }
}
