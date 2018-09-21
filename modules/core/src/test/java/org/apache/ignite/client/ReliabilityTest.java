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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * High Availability tests.
 */
public class ReliabilityTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

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

    /** */
    @FunctionalInterface
    private interface Assertion {
        /** */
        void call() throws Exception;
    }

    /**
     * Run the assertion while Ignite nodes keep failing/recovering 10 times.
     */
    private static void assertOnUnstableCluster(LocalIgniteCluster cluster, Assertion assertion) {
        // Keep changing Ignite cluster topology by adding/removing nodes
        final AtomicBoolean isTopStable = new AtomicBoolean(false);

        final AtomicReference<Throwable> err = new AtomicReference<>(null);

        Future<?> topChangeFut = Executors.newSingleThreadExecutor().submit(() -> {
            for (int i = 0; i < 10 && err.get() == null; i++) {
                while (cluster.size() != 1)
                    cluster.failNode();

                while (cluster.size() != cluster.getInitialSize())
                    cluster.restoreNode();
            }

            isTopStable.set(true);
        });

        // Use Ignite while the nodes keep failing
        try {
            while (err.get() == null && !isTopStable.get()) {
                try {
                    assertion.call();
                }
                catch (ClientServerError ex) {
                    // TODO: fix CACHE_DOES_NOT_EXIST server error and remove this exception handler
                    if (ex.getCode() != ClientStatus.CACHE_DOES_NOT_EXIST)
                        throw ex;
                }
            }
        }
        catch (Throwable e) {
            err.set(e);
        }

        try {
            topChangeFut.get();
        }
        catch (Exception e) {
            err.set(e);
        }

        Throwable ex = err.get();

        String msg = ex == null ? "" : ex.getMessage();

        assertNull(msg, ex);
    }
}
