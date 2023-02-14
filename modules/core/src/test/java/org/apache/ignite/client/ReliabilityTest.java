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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.client.thin.ClientOperation;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVTS_CACHE;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * High Availability tests.
 */
@RunWith(Parameterized.class)
public class ReliabilityTest extends AbstractThinClientTest {
    /** Service name. */
    private static final String SERVICE_NAME = "svc";

    /** Partition awareness. */
    @Parameterized.Parameter
    public boolean partitionAware;

    /** Async operations. */
    @Parameterized.Parameter(1)
    public boolean async;

    /**
     * @return List of parameters to test.
     */
    @Parameterized.Parameters(name = "partitionAware={0}, async={1}")
    public static Collection<Object[]> testData() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {false, false});
        res.add(new Object[] {false, true});
        res.add(new Object[] {true, false});
        res.add(new Object[] {true, true});

        return res;
    }

    /**
     * Thin clint failover.
     */
    @Test
    public void testFailover() throws Exception {
        Assume.assumeFalse(partitionAware);

        final int CLUSTER_SIZE = 3;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(CLUSTER_SIZE);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setReconnectThrottlingRetries(0) // Disable throttling.
                 // Disable endpoints discovery, since in this test it reduces attempts count and sometimes one extra
                 // attempt is required to complete operation without failure.
                 .setAddressesFinder(new StaticAddressFinder(cluster.clientAddresses().toArray(new String[CLUSTER_SIZE])))
             )
        ) {
            final Random rnd = new Random();

            final ClientCache<Integer, String> cache = client.getOrCreateCache(
                new ClientCacheConfiguration()
                    .setName("testFailover")
                    .setCacheMode(CacheMode.REPLICATED)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            );

            // Simple operation failover: put/get
            assertOnUnstableCluster(cluster, () -> {
                Integer key = rnd.nextInt();
                String val = key.toString();

                cachePut(cache, key, val);

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

                try {
                    try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                        List<Cache.Entry<Integer, String>> res = cur.getAll();

                        assertEquals("Unexpected number of entries", data.size(), res.size());

                        Map<Integer, String> act = res.stream()
                                .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

                        assertEquals("Unexpected entries", data, act);
                    }
                }
                catch (ClientConnectionException ignored) {
                    // QueryCursor.getAll always executes on the same channel where the cursor is open,
                    // so failover is not possible, and the call will fail when connection drops.
                }
            });

            // Client fails if all nodes go down
            cluster.close();

            boolean igniteUnavailable = false;

            try {
                cachePut(cache, 1, "1");
            }
            catch (ClientConnectionException ex) {
                igniteUnavailable = true;

                Throwable[] suppressed = ex.getSuppressed();

                assertEquals(CLUSTER_SIZE - 1, suppressed.length);

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
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setAddresses(cluster.clientAddresses().iterator().next()))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            GridTestUtils.assertThrowsWithCause(() -> cachePut(cache, 0, 0), ClientConnectionException.class);

            // Recover after fail.
            cachePut(cache, 0, 0);
        }
    }

    /**
     * Test single server can be used multiple times in configuration.
     */
    @Test
    public void testSingleServerDuplicatedFailover() throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setAddressesFinder(new StaticAddressFinder(
                     F.first(cluster.clientAddresses()),
                     F.first(cluster.clientAddresses())
                 )))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            // Reuse second address without fail.
            cachePut(cache, 0, 0);
        }
    }

    /**
     * Test single server can be used multiple times in configuration.
     */
    @Test
    public void testRetryReadPolicyRetriesCacheGet() {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setRetryPolicy(new ClientRetryReadPolicy())
                 .setAddressesFinder(new StaticAddressFinder(
                     F.first(cluster.clientAddresses()),
                     F.first(cluster.clientAddresses())
                 )))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            // Reuse second address without fail.
            cache.get(0);
        }
    }

    /**
     * Tests retry policy exception handling.
     */
    @Test
    public void testExceptionInRetryPolicyPropagatesToCaller() {
        Assume.assumeFalse(partitionAware);

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setRetryPolicy(new ExceptionRetryPolicy())
                 .setAddressesFinder(new StaticAddressFinder(
                     F.first(cluster.clientAddresses()),
                     F.first(cluster.clientAddresses())
                 )))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            Throwable asyncEx = GridTestUtils.assertThrows(null, () -> cache.getAsync(0).get(),
                    ExecutionException.class, "Channel is closed");

            dropAllThinClientConnections(Ignition.allGrids().get(0));

            Throwable syncEx = GridTestUtils.assertThrows(null, () -> cache.get(0),
                    ClientConnectionException.class, "Channel is closed");

            for (Throwable t : new Throwable[] {asyncEx.getCause(), syncEx}) {
                assertEquals("Error in policy.", t.getSuppressed()[0].getMessage());
            }
        }
    }

    /**
     * Tests that retry limit of 1 effectively disables retry/failover.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testRetryLimitDisablesFailover() {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setRetryLimit(1)
                 .setAddressesFinder(new StaticAddressFinder(
                     F.first(cluster.clientAddresses()),
                     F.first(cluster.clientAddresses())
                 )))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            // Reuse second address without fail.
            GridTestUtils.assertThrows(null, () -> cachePut(cache, 0, 0), IgniteException.class,
                    "Channel is closed");
        }
    }

    /**
     * Tests that setting retry policy to null effectively disables retry/failover.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNullRetryPolicyDisablesFailover() {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setRetryPolicy(null)
                 .setAddresses(
                     cluster.clientAddresses().iterator().next(),
                     cluster.clientAddresses().iterator().next()))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            // Reuse second address without fail.
            GridTestUtils.assertThrows(null, () -> cachePut(cache, 0, 0), IgniteException.class,
                    "Channel is closed");
        }
    }

    /**
     * Tests that retry limit of 1 effectively disables retry/failover.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testRetryNonePolicyDisablesFailover() {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setRetryPolicy(new ClientRetryNonePolicy())
                 .setAddresses(
                     cluster.clientAddresses().iterator().next(),
                     cluster.clientAddresses().iterator().next()))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            // Before fail.
            cachePut(cache, 0, 0);

            // Fail.
            dropAllThinClientConnections(Ignition.allGrids().get(0));

            // Reuse second address without fail.
            GridTestUtils.assertThrows(null, () -> cachePut(cache, 0, 0), IgniteException.class,
                    "Channel is closed");
        }
    }

    /**
     * Tests that {@link ClientOperationType} is updated accordingly when {@link ClientOperation} is added.
     */
    @Test
    public void testRetryPolicyConvertOpAllOperationsSupported() {
        List<ClientOperation> nullOps = Arrays.stream(ClientOperation.values())
                .filter(o -> o.toPublicOperationType() == null)
                .collect(Collectors.toList());

        String nullOpsNames = nullOps.stream().map(Enum::name).collect(Collectors.joining(", "));

        long expectedNullCnt = 21;

        String msg = nullOps.size()
                + " operation codes do not have public equivalent. When adding new codes, update ClientOperationType too. Missing ops: "
                + nullOpsNames;

        assertEquals(msg, expectedNullCnt, nullOps.size());
    }

    /**
     * Test that failover doesn't lead to silent query inconsistency.
     */
    @Test
    public void testQueryConsistencyOnFailover() throws Exception {
        int CLUSTER_SIZE = 2;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(CLUSTER_SIZE);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setAddresses(cluster.clientAddresses().toArray(new String[CLUSTER_SIZE])))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            cachePut(cache, 0, 0);
            cachePut(cache, 1, 1);

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

                fail("ClientReconnectedException or ClientConnectionException must be thrown");
            }
            catch (ClientReconnectedException | ClientConnectionException expected) {
                // No-op.
            }
        }
    }

    /**
     * Test that client works properly with servers txId intersection.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testTxWithIdIntersection() throws Exception {
        // Partition-aware client connects to all known servers at the start, and dropAllThinClientConnections
        // causes failure on all channels, so the logic in this test is not applicable.
        Assume.assumeFalse(partitionAware);

        int CLUSTER_SIZE = 2;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(CLUSTER_SIZE);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setAddresses(cluster.clientAddresses().toArray(new String[CLUSTER_SIZE])))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache(new ClientCacheConfiguration().setName("cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            CyclicBarrier barrier = new CyclicBarrier(2);

            GridTestUtils.runAsync(() -> {
                try {
                    // Another thread starts transaction here.
                    barrier.await(1, TimeUnit.SECONDS);

                    for (int i = 0; i < CLUSTER_SIZE; i++)
                        dropAllThinClientConnections(Ignition.allGrids().get(i));

                    ClientTransaction tx = client.transactions().txStart();

                    barrier.await(1, TimeUnit.SECONDS);

                    // Another thread puts to cache here.
                    barrier.await(1, TimeUnit.SECONDS);

                    tx.commit();

                    barrier.await(1, TimeUnit.SECONDS);
                }
                catch (Exception e) {
                    log.error("Unexpected error", e);
                }
            });

            ClientTransaction tx = client.transactions().txStart();

            barrier.await(1, TimeUnit.SECONDS);

            // Another thread drops connections and create new transaction here, which started on another node with the
            // same transaction id as we started in this thread.
            barrier.await(1, TimeUnit.SECONDS);

            GridTestUtils.assertThrows(null, () -> {
                cachePut(cache, 0, 0);

                return null;
            }, ClientException.class, "Transaction context has been lost due to connection errors");

            tx.close();

            barrier.await(1, TimeUnit.SECONDS);

            // Another thread commit transaction here.
            barrier.await(1, TimeUnit.SECONDS);

            assertFalse(cache.containsKey(0));
        }
    }

    /**
     * Test reconnection throttling.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testReconnectionThrottling() throws Exception {
        // If partition awareness is enabled, channels are restored asynchronously without applying throttling.
        Assume.assumeFalse(partitionAware);

        int throttlingRetries = 5;
        long throttlingPeriod = 3_000L;

        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration()
                 .setReconnectThrottlingPeriod(throttlingPeriod)
                 .setReconnectThrottlingRetries(throttlingRetries)
                 .setAddresses(cluster.clientAddresses().toArray(new String[1])))
        ) {
            ClientCache<Integer, Integer> cache = client.createCache("cache");

            for (int i = 0; i < throttlingRetries; i++) {
                // Attempts to reconnect within throttlingRetries should pass.
                cachePut(cache, 0, 0);

                dropAllThinClientConnections(Ignition.allGrids().get(0));

                GridTestUtils.assertThrowsWithCause(() -> cachePut(cache, 0, 0), ClientConnectionException.class);
            }

            for (int i = 0; i < 10; i++) // Attempts to reconnect after throttlingRetries should fail.
                GridTestUtils.assertThrowsWithCause(() -> cachePut(cache, 0, 0), ClientConnectionException.class);

            doSleep(throttlingPeriod);

            // Attempt to reconnect after throttlingPeriod should pass.
            assertTrue(GridTestUtils.waitForCondition(() -> {
                try {
                    cachePut(cache, 0, 0);

                    return true;
                }
                catch (ClientConnectionException e) {
                    return false;
                }
            }, throttlingPeriod));
        }
    }

    /**
     * Test server-side critical error.
     */
    @Test
    public void testServerCriticalError() throws Exception {
        AtomicBoolean failure = new AtomicBoolean();

        FailureHandler hnd = (ignite, ctx) -> failure.compareAndSet(false, true);

        try (Ignite ignite = startGrid(getConfiguration().setFailureHandler(hnd)
            .setIncludeEventTypes(EVTS_CACHE)); IgniteClient client = startClient(ignite)
        ) {
            ClientCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            cachePut(cache, 0, 0);

            String msg = "critical error message";

            ignite.events().localListen(e -> {
                throw new Error(msg);
            }, EVT_CACHE_OBJECT_READ);

            GridTestUtils.assertThrowsAnyCause(log, () -> cache.get(0), ClientServerError.class, msg);

            assertFalse(failure.get());

            // OutOfMemoryError should also invoke failure handler.
            ignite.events().localListen(e -> {
                throw new OutOfMemoryError(msg);
            }, EVT_CACHE_OBJECT_REMOVED);

            GridTestUtils.assertThrowsAnyCause(log, () -> cache.remove(0), ClientServerError.class, msg);

            assertTrue(GridTestUtils.waitForCondition(failure::get, 1_000L));
        }
    }

    /**
     * Test that client can invoke service method with externalizable parameter after
     * cluster failover.
     */
    @Test
    public void testServiceMethodInvocationAfterFailover() throws Exception {
        PersonExternalizable person = new PersonExternalizable("Person 1");

        ServiceConfiguration testServiceConfig = new ServiceConfiguration();
        testServiceConfig.setName(SERVICE_NAME);
        testServiceConfig.setService(new TestService());
        testServiceConfig.setTotalCount(1);

        Ignite ignite = null;
        IgniteClient client = null;
        try {
            // Initialize cluster and client
            ignite = startGrid(getConfiguration().setServiceConfiguration(testServiceConfig));
            client = startClient(ignite);
            TestServiceInterface svc = client.services().serviceProxy(SERVICE_NAME, TestServiceInterface.class);

            // Invoke the service method with Externalizable parameter for the first time.
            // This triggers registration of the PersonExternalizable type in the cluter.
            String result = svc.testMethod(person);
            assertEquals("testMethod(PersonExternalizable person): " + person, result);

            // Kill the cluster node, clean up the working directory (with cached types)
            // and drop the client connection.
            ignite.close();
            U.delete(U.resolveWorkDirectory(
                    U.defaultWorkDirectory(),
                    DataStorageConfiguration.DFLT_MARSHALLER_PATH,
                    false));
            dropAllThinClientConnections();

            // Invoke the service.
            GridTestUtils.assertThrowsWithCause(() -> svc.testMethod(person), ClientConnectionException.class);

            // Restore the cluster and redeploy the service.
            ignite = startGrid(getConfiguration().setServiceConfiguration(testServiceConfig));

            // Invoke the service method with Externalizable parameter once again.
            // This should restore the client connection and trigger registration of the
            // PersonExternalizable once again.
            result = svc.testMethod(person);
            assertEquals("testMethod(PersonExternalizable person): " + person, result);
        }
        finally {
            if (ignite != null) {
                try {
                    ignite.close();
                }
                catch (Throwable ignore) {
                    // Ignore.
                }
            }

            if (client != null) {
                try {
                    client.close();
                }
                catch (Throwable ignore) {
                    // Ignore.
                }
            }
        }
    }

    /**
     * Tests that server does not disconnect idle clients when heartbeats are enabled.
     */
    @Test
    public void testServerDoesNotDisconnectIdleClientWithHeartbeats() throws Exception {
        IgniteConfiguration serverCfg = getConfiguration().setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setIdleTimeout(2000));

        ClientConfiguration clientCfg = new ClientConfiguration()
                .setAddresses("127.0.0.1")
                .setHeartbeatEnabled(true)
                .setHeartbeatInterval(500);

        try (Ignite ignored = startGrid(serverCfg); IgniteClient client = Ignition.startClient(clientCfg)) {
            Thread.sleep(6000);
            assertEquals(0, client.cacheNames().size());
        }
    }

    /**
     * Performs cache put.
     *
     * @param cache Cache.
     * @param key Key.
     * @param val Val.
     * @param <K> Key type.
     * @param <V> Val type.
     */
    protected <K, V> void cachePut(ClientCache<K, V> cache, K key, V val) {
        if (async) {
            try {
                cache.putAsync(key, val).get();
            }
            catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof RuntimeException)
                    throw (RuntimeException)e.getCause();

                throw new RuntimeException(e);
            }
        }
        else
            cache.put(key, val);
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

    /** {@inheritDoc} */
    @Override protected boolean isClientPartitionAwarenessEnabled() {
        return partitionAware;
    }

    /** */
    public static interface TestServiceInterface {
        /** */
        public String testMethod(PersonExternalizable person);
    }

    /**
     * Implementation of TestServiceInterface.
     */
    public static class TestService implements Service, TestServiceInterface {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String testMethod(PersonExternalizable person) {
            return "testMethod(PersonExternalizable person): " + person;
        }
    }
}
