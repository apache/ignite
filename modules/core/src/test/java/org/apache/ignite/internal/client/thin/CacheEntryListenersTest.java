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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientDisconnectListener;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Thin client cache entry listeners test.
 */
public class CacheEntryListenersTest extends AbstractThinClientTest {
    /** Timeout. */
    private static final long TIMEOUT = 1_000L;

    /** */
    private boolean enpointsDiscoveryEnabled = true;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        enpointsDiscoveryEnabled = true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(100)));
    }

    /** {@inheritDoc} */
    @Override protected boolean isClientEndpointsDiscoveryEnabled() {
        return enpointsDiscoveryEnabled;
    }

    /** Test continuous queries. */
    @Test
    public void testContinuousQueries() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQ");

            ContinuousQueryListener<Integer, Integer> lsnr = new ContinuousQueryListener<>();

            cache.query(new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr));

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            assertEquals(F.asMap(EventType.CREATED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> i))), aggregateListenerEvents(lsnr, 10));

            for (int i = 0; i < 10; i++)
                cache.put(i, -i);

            assertEquals(F.asMap(EventType.UPDATED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> -i))), aggregateListenerEvents(lsnr, 10));

            for (int i = 0; i < 10; i++)
                cache.remove(i);

            assertEquals(F.asMap(EventType.REMOVED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> -i))), aggregateListenerEvents(lsnr, 10));

            assertTrue(lsnr.isQueueEmpty());
        }
    }

    /** Test continuous queries with initial query. */
    @Test
    public void testContinuousQueriesWithInitialQuery() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQWithInitQ");

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            ContinuousQueryListener<Integer, Integer> lsnr = new ContinuousQueryListener<>();

            QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(new ContinuousQuery<Integer, Integer>()
                .setInitialQuery(new ScanQuery<>()).setLocalListener(lsnr));

            assertTrue(lsnr.isQueueEmpty());

            assertEquals(100, cur.getAll().size());

            cache.put(100, 100);

            lsnr.assertNextCacheEvent(EventType.CREATED, 100, 100);

            cache.put(100, 101);

            lsnr.assertNextCacheEvent(EventType.UPDATED, 100, 101);

            cache.remove(100);

            lsnr.assertNextCacheEvent(EventType.REMOVED, 100);
        }
    }

    /** Test continuous queries with include expired parameter. */
    @Test
    public void testContinuousQueriesWithIncludeExpired() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQWithInclExp");

            ContinuousQueryListener<Integer, Integer> lsnr1 = new ContinuousQueryListener<>();
            ContinuousQueryListener<Integer, Integer> lsnr2 = new ContinuousQueryListener<>();

            ContinuousQuery<Integer, Integer> qry1 = new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr1);
            ContinuousQuery<Integer, Integer> qry2 = new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr2);

            qry1.setIncludeExpired(false);
            qry2.setIncludeExpired(true);

            cache.query(qry1);
            cache.query(qry2);

            cache = cache.withExpirePolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)));

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            int cntCreated = 0;
            int cntExpired = 0;

            for (int i = 0; i < 100; i++)
                assertEquals(EventType.CREATED, lsnr1.poll().getEventType());

            for (int i = 0; i < 200; i++) { // There should be two events for each cache entry.
                CacheEntryEvent<?, ?> evt = lsnr2.poll();

                if (evt.getEventType() == EventType.CREATED)
                    cntCreated++;
                else if (evt.getEventType() == EventType.EXPIRED)
                    cntExpired++;
                else
                    fail("Unexpected event type: " + evt.getEventType());
            }

            assertEquals(100, cntCreated);
            assertEquals(100, cntExpired);

            assertTrue(lsnr1.isQueueEmpty());
            assertTrue(lsnr2.isQueueEmpty());
        }
    }

    /** Test continuous queries with page size parameter. */
    @Test
    public void testContinuousQueriesWithPageSize() throws Exception {
        // It's required to connect exactly to nodes 1 and 2, without node 0.
        enpointsDiscoveryEnabled = false;

        try (IgniteClient client = startClient(1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQWithPageSize");
            IgniteCache<Integer, Integer> nodeCache = grid(0).getOrCreateCache(cache.getName());

            ContinuousQueryListener<Integer, Integer> lsnr = new ContinuousQueryListener<>();

            ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr)
                .setPageSize(10);

            cache.query(qry);

            // Each node has its own buffer, put data to the exactly one remote node.
            primaryKeys(nodeCache, 15).forEach(key -> cache.put(key, key));

            // Check that only first page is received.
            aggregateListenerEvents(lsnr, 10);

            assertTrue(lsnr.isQueueEmpty());

            primaryKeys(nodeCache, 6).forEach(key -> cache.put(key, key));

            // Check that only second page is received.
            aggregateListenerEvents(lsnr, 10);

            assertTrue(lsnr.isQueueEmpty());
        }
    }

    /** Test continuous queries with time interval parameter. */
    @Test
    public void testContinuousQueriesWithTimeInterval() throws Exception {
        // It's required to connect exactly to nodes 1 and 2, without node 0.
        enpointsDiscoveryEnabled = false;

        try (IgniteClient client = startClient(1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQWithTimeInterval");
            IgniteCache<Integer, Integer> nodeCache = grid(0).getOrCreateCache(cache.getName());

            ContinuousQueryListener<Integer, Integer> lsnr = new ContinuousQueryListener<>();

            ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr)
                .setPageSize(10).setTimeInterval(TIMEOUT);

            long ts1 = U.currentTimeMillis();

            cache.query(qry);

            // Put data to the remote node.
            int key = primaryKey(nodeCache);
            cache.put(key, key);

            assertNotNull(lsnr.poll(TIMEOUT * 2));

            assertTrue(lsnr.isQueueEmpty());

            long ts2 = U.currentTimeMillis();

            // Ensure that item was received after timeout.
            assertTrue("ts2 - ts1 = " + (ts2 - ts1), ts2 - ts1 >= TIMEOUT);
        }
    }

    /** Test JCache entry listeners. */
    @Test
    public void testJCacheListeners() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testJCacheListeners");

            JCacheEntryListener<Integer, Integer> lsnr = new JCacheEntryListener<>();

            cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr, null, true, false));

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            assertEquals(F.asMap(EventType.CREATED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> i))), aggregateListenerEvents(lsnr, 10));

            for (int i = 0; i < 10; i++)
                cache.put(i, -i);

            assertEquals(F.asMap(EventType.UPDATED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> -i))), aggregateListenerEvents(lsnr, 10));

            for (int i = 0; i < 10; i++)
                cache.remove(i);

            assertEquals(F.asMap(EventType.REMOVED, IntStream.range(0, 10).boxed()
                .collect(Collectors.toMap(i -> i, i -> -i))), aggregateListenerEvents(lsnr, 10));

            assertTrue(lsnr.isQueueEmpty());
        }
    }

    /** Test JCache entry listeners with expired entries. */
    @Test
    public void testJCacheListenersExpiredEntries() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testJCacheListenersWithExp");

            JCacheEntryListener<Integer, Integer> lsnr = new JCacheEntryListener<>();

            cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr, null, true, false));

            cache = cache.withExpirePolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)));

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            int cntCreated = 0;
            int cntExpired = 0;

            for (int i = 0; i < 200; i++) { // There should be two events for each cache entry.
                CacheEntryEvent<?, ?> evt = lsnr.poll();

                if (evt.getEventType() == EventType.CREATED)
                    cntCreated++;
                else if (evt.getEventType() == EventType.EXPIRED)
                    cntExpired++;
                else
                    fail("Unexpected event type: " + evt.getEventType());
            }

            assertEquals(100, cntCreated);
            assertEquals(100, cntExpired);

            assertTrue(lsnr.isQueueEmpty());
        }
    }

    /** Test continuous queries and JCache entry listeners with keep binary flag. */
    @Test
    public void testListenersWithKeepBinary() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Object, Object> cache1 = client.getOrCreateCache("testListenersWithKB");
            ClientCache<Object, Object> cache2 = cache1.withKeepBinary();

            ContinuousQueryListener<Object, Object> lsnr1 = new ContinuousQueryListener<>();
            ContinuousQueryListener<Object, Object> lsnr2 = new ContinuousQueryListener<>();

            cache1.query(new ContinuousQuery<>().setLocalListener(lsnr1));
            cache2.query(new ContinuousQuery<>().setLocalListener(lsnr2));

            JCacheEntryListener<Object, Object> lsnr3 = new JCacheEntryListener<>();
            JCacheEntryListener<Object, Object> lsnr4 = new JCacheEntryListener<>();

            cache1.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr3, null, true, false));
            cache2.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr4, null, true, false));

            Person person1 = new Person(0, "name");
            Person person2 = new Person(1, "another name");

            cache1.put(0, person1);

            lsnr1.assertNextCacheEvent(EventType.CREATED, 0, person1);
            lsnr2.assertNextCacheEvent(EventType.CREATED, 0, client.binary().toBinary(person1));
            lsnr3.assertNextCacheEvent(EventType.CREATED, 0, person1);
            lsnr4.assertNextCacheEvent(EventType.CREATED, 0, client.binary().toBinary(person1));

            cache1.put(0, person2);

            lsnr1.assertNextCacheEvent(EventType.UPDATED, 0, person2);
            lsnr2.assertNextCacheEvent(EventType.UPDATED, 0, client.binary().toBinary(person2));
            lsnr3.assertNextCacheEvent(EventType.UPDATED, 0, person2);
            lsnr4.assertNextCacheEvent(EventType.UPDATED, 0, client.binary().toBinary(person2));
        }
    }

    /** Test continuous queries and JCache entry listeners with remote filters. */
    @Test
    @SuppressWarnings("deprecation")
    public void testListenersWithRemoteFilter() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testListenersWithRmtFilter");

            CacheEntryEventSerializableFilter<Integer, Integer> rmtFilter = evt -> (evt.getKey() & 1) == 0;

            ContinuousQueryListener<Integer, Integer> lsnr1 = new ContinuousQueryListener<>();
            ContinuousQueryListener<Integer, Integer> lsnr2 = new ContinuousQueryListener<>();

            cache.query(new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr1)
                .setRemoteFilterFactory(() -> rmtFilter));

            cache.query(new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr2)
                .setRemoteFilter(rmtFilter));

            JCacheEntryListener<Integer, Integer> lsnr3 = new JCacheEntryListener<>();

            cache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr3, () -> rmtFilter, true, false));

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            Map<EventType, Map<Integer, Integer>> expRes = F.asMap(EventType.CREATED,
                IntStream.range(0, 5).boxed().collect(Collectors.toMap(i -> i * 2, i -> i * 2)));

            assertEquals(expRes, aggregateListenerEvents(lsnr1, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr2, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr3, 5));

            for (int i = 0; i < 10; i++)
                cache.put(i, -i);

            expRes = F.asMap(EventType.UPDATED,
                IntStream.range(0, 5).boxed().collect(Collectors.toMap(i -> i * 2, i -> -i * 2)));

            assertEquals(expRes, aggregateListenerEvents(lsnr1, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr2, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr3, 5));

            for (int i = 0; i < 10; i++)
                cache.remove(i);

            expRes = F.asMap(EventType.REMOVED,
                IntStream.range(0, 5).boxed().collect(Collectors.toMap(i -> i * 2, i -> -i * 2)));

            assertEquals(expRes, aggregateListenerEvents(lsnr1, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr2, 5));
            assertEquals(expRes, aggregateListenerEvents(lsnr3, 5));

            assertTrue(lsnr1.isQueueEmpty());
            assertTrue(lsnr2.isQueueEmpty());
            assertTrue(lsnr3.isQueueEmpty());
        }
    }

    /** Test disconnect event for cache entry listeners. */
    @Test
    public void testDisconnectListeners() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Object, Object> cache = client.getOrCreateCache("testDisconnect");

            ContinuousQueryListener<Object, Object> lsnr1 = new ContinuousQueryListener<>();

            cache.query(new ContinuousQuery<>().setLocalListener(lsnr1), lsnr1);

            JCacheEntryListener<Object, Object> lsnr2 = new JCacheEntryListener<>();

            CacheEntryListenerConfiguration<Object, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr2, null, true, false);

            cache.registerCacheEntryListener(lsnrCfg, lsnr2);

            cache.put(0, 0);

            lsnr1.assertNextCacheEvent(EventType.CREATED, 0, 0);
            lsnr2.assertNextCacheEvent(EventType.CREATED, 0, 0);

            dropAllThinClientConnections();

            // Can't detect channel failure until we send something to server.
            cache.put(1, 1);

            assertTrue(lsnr1.isQueueEmpty());
            assertTrue(lsnr2.isQueueEmpty());

            assertTrue(waitForCondition(lsnr1::isDisconnected, TIMEOUT));
            assertTrue(waitForCondition(lsnr2::isDisconnected, TIMEOUT));

            // Should be able to register the same listener on the same cache again.
            cache.registerCacheEntryListener(lsnrCfg);
        }
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testRegisterDeregisterListener() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            String cacheName = "registerListener";

            ClientCache<Integer, Integer> cache0 = client.getOrCreateCache(cacheName);

            CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                JCacheEntryListener::new,
                null,
                true,
                false
            );

            cache0.registerCacheEntryListener(lsnrCfg);

            ClientCache<Integer, Integer> cache1 = client.getOrCreateCache(cacheName + '2');

            // Can register the same listener on another cache.
            cache1.registerCacheEntryListener(lsnrCfg);

            ClientCache<Integer, Integer> cache2 = client.cache(cacheName);

            // Can't register the same listener on the same cache.
            assertThrowsWithCause(() -> cache2.registerCacheEntryListener(lsnrCfg), IllegalStateException.class);

            ClientCache<Integer, Integer> cache3 = client.cache(cacheName);

            cache3.deregisterCacheEntryListener(lsnrCfg);

            // Can register the listener after deregisteration.
            cache2.registerCacheEntryListener(lsnrCfg);
        }
    }

    /** */
    @Test
    @SuppressWarnings({"ThrowableNotThrown", "deprecation"})
    public void testListenersUnsupportedParameters() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testUnsupportedParams");

            // Check null listener factory.
            CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg1 = new MutableCacheEntryListenerConfiguration<>(
                null,
                null,
                true,
                false
            );

            assertThrowsWithCause(() -> cache.registerCacheEntryListener(lsnrCfg1), NullPointerException.class);

            // Check synchronous flag.
            CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg2 = new MutableCacheEntryListenerConfiguration<>(
                JCacheEntryListener::new,
                null,
                true,
                true
            );

            assertThrowsWithCause(() -> cache.registerCacheEntryListener(lsnrCfg2), IllegalArgumentException.class);

            // Check local flag.
            ContinuousQueryListener<Integer, Integer> cqLsnr = new ContinuousQueryListener<>();

            ContinuousQuery<Integer, Integer> qry1 = new ContinuousQuery<Integer, Integer>().setLocalListener(cqLsnr)
                .setLocal(true);

            assertThrowsWithCause(() -> cache.query(qry1), IllegalArgumentException.class);

            // Check null listener.
            ContinuousQuery<Integer, Integer> qry2 = new ContinuousQuery<>();

            assertThrowsWithCause(() -> cache.query(qry2), NullPointerException.class);

            // Check auto unsubscribe flag.
            ContinuousQuery<Integer, Integer> qry3 = new ContinuousQuery<Integer, Integer>().setLocalListener(cqLsnr)
                .setAutoUnsubscribe(false);

            assertThrowsWithCause(() -> cache.query(qry3), IllegalArgumentException.class);

            // Check continuous query as initial query.
            ContinuousQuery<Integer, Integer> qry4 = new ContinuousQuery<Integer, Integer>().setLocalListener(cqLsnr)
                .setInitialQuery(new ContinuousQuery<>());

            assertThrowsWithCause(() -> cache.query(qry4), IllegalArgumentException.class);

            // Check filter factory and filter defined at the same time.
            CacheEntryEventSerializableFilter<Integer, Integer> rmtFilter = r -> true;

            ContinuousQuery<Integer, Integer> qry5 = new ContinuousQuery<Integer, Integer>().setLocalListener(cqLsnr)
                .setRemoteFilter(rmtFilter);

            qry5.setRemoteFilterFactory(FactoryBuilder.factoryOf(rmtFilter));

            assertThrowsWithCause(() -> cache.query(qry5), IllegalArgumentException.class);
        }
    }

    /** */
    @Test
    public void testListenersClose() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache("testListenersClose");

            ContinuousQueryListener<Integer, Integer> lsnr1 = new ContinuousQueryListener<>();

            QueryCursor<?> qry = cache.query(new ContinuousQuery<Integer, Integer>().setLocalListener(lsnr1));

            JCacheEntryListener<Integer, Integer> lsnr2 = new JCacheEntryListener<>();

            CacheEntryListenerConfiguration<Integer, Integer> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                () -> lsnr2, null, true, false);

            cache.registerCacheEntryListener(lsnrCfg);

            cache.put(0, 0);

            lsnr1.assertNextCacheEvent(EventType.CREATED, 0, 0);
            lsnr2.assertNextCacheEvent(EventType.CREATED, 0, 0);

            qry.close();
            cache.deregisterCacheEntryListener(lsnrCfg);

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            assertTrue(lsnr1.isQueueEmpty());
            assertTrue(lsnr2.isQueueEmpty());
        }
    }

    /** */
    @Test
    public void testContinuousQueriesWithConcurrentCompute() throws Exception {
        try (IgniteClient client = startClient(0, 1, 2)) {
            int threadsCnt = 20;
            int iterations = 50;

            Set<UUID> allNodesIds = new HashSet<>(F.nodeIds(grid(0).cluster().nodes()));

            AtomicInteger threadIdxs = new AtomicInteger();

            GridTestUtils.runMultiThreaded(
                () -> {
                    int threadIdx = threadIdxs.incrementAndGet();

                    ClientCache<Integer, Integer> cache = client.getOrCreateCache("testCQwithCompute" + threadIdx);

                    try {
                        for (int i = 0; i < iterations; i++) {
                            ContinuousQueryListener<Integer, Integer> lsnr = new ContinuousQueryListener<>();

                            QueryCursor<?> cur = cache.query(new ContinuousQuery<Integer, Integer>()
                                .setLocalListener(lsnr));

                            cache.put(i, i);

                            Future<T2<UUID, Set<UUID>>> fut = client.compute().executeAsync2(TestTask.class.getName(),
                                null);

                            assertEquals(allNodesIds, fut.get().get2());

                            lsnr.assertNextCacheEvent(EventType.CREATED, i, i);

                            assertTrue(lsnr.isQueueEmpty());

                            cur.close();
                        }
                    }
                    catch (Exception e) {
                        log.error("Failure: ", e);

                        fail();
                    }
                }, threadsCnt, "run-task-async"
            );
        }
    }

    /** */
    private static <K, V> Map<EventType, Map<K, V>> aggregateListenerEvents(ContinuousQueryListener<K, V> lsnr,
        int evtsCnt) throws Exception {
        Map<EventType, Map<K, V>> res = new EnumMap<>(EventType.class);

        for (int i = 0; i < evtsCnt; i++) {
            CacheEntryEvent<? extends K, ? extends V> evt = lsnr.poll();

            Map<K, V> locMap = res.computeIfAbsent(evt.getEventType(), k -> new HashMap<>());

            locMap.put(evt.getKey(), evt.getValue());
        }

        return res;
    }

    /** */
    private static class ContinuousQueryListener<K, V> implements CacheEntryUpdatedListener<K, V>,
        ClientDisconnectListener {
        /** Local entries map. */
        private final BlockingQueue<CacheEntryEvent<? extends K, ? extends V>> evtsQ =
            new LinkedBlockingQueue<>();

        /** Disconnected flag. */
        private volatile boolean disconnected;

        /** Failure. */
        private volatile Exception failure;

        /**
         * @param expectedEvtType Expected event type ({@code null} for any event type).
         * @param evt Event.
         */
        protected void addEvent(EventType expectedEvtType, CacheEntryEvent<? extends K, ? extends V> evt) {
            if (expectedEvtType != null && evt.getEventType() != expectedEvtType)
                failure = new Exception("Unexpected event type [expEvtType=" + expectedEvtType + ", evt=" + evt + ']');
            else
                evtsQ.add(evt);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
            events.forEach(evt -> addEvent(null, evt));
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(Exception reason) {
            disconnected = true;
        }

        /**
         * Poll next cache event.
         */
        public CacheEntryEvent<? extends K, ? extends V> poll(long timeout) throws Exception {
            if (failure != null)
                throw failure;

            CacheEntryEvent<? extends K, ? extends V> evt = evtsQ.poll(timeout, TimeUnit.MILLISECONDS);

            assertNotNull(evt);

            return evt;
        }

        /**
         * Poll next cache event.
         */
        public CacheEntryEvent<? extends K, ? extends V> poll() throws Exception {
            return poll(TIMEOUT);
        }

        /**
         * Assert parameters of the next cache event.
         */
        public void assertNextCacheEvent(EventType expType, K expKey) throws Exception {
            CacheEntryEvent<? extends K, ? extends V> evt = poll();
            assertEquals(expType, evt.getEventType());
            assertEquals(expKey, evt.getKey());
        }

        /**
         * Assert parameters of the next cache event.
         */
        public void assertNextCacheEvent(EventType expType, K expKey, V expVal) throws Exception {
            CacheEntryEvent<? extends K, ? extends V> evt = poll();
            assertEquals(expType, evt.getEventType());
            assertEquals(expKey, evt.getKey());
            assertEquals(expVal, evt.getValue());
        }

        /** */
        public boolean isDisconnected() {
            return disconnected;
        }

        /** */
        public boolean isQueueEmpty() {
            return evtsQ.isEmpty();
        }
    }

    /** */
    private static class JCacheEntryListener<K, V> extends ContinuousQueryListener<K, V> implements
        CacheEntryCreatedListener<K, V>, CacheEntryRemovedListener<K, V>, CacheEntryExpiredListener<K, V> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
            events.forEach(evt -> addEvent(EventType.CREATED, evt));
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
            events.forEach(evt -> addEvent(EventType.UPDATED, evt));
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
            events.forEach(evt -> addEvent(EventType.REMOVED, evt));
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
            events.forEach(evt -> addEvent(EventType.EXPIRED, evt));
        }
    }
}
