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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 */
public class CacheContinuousWithTransformerReplicatedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int DFLT_ENTRY_CNT = 10;

    /** */
    private static final int DFLT_LATCH_TIMEOUT = 30_000;

    /** */
    private static final int DFLT_SERVER_NODE_CNT = 1;

    /** */
    private static final String SARAH_CONNOR = "Sarah Connor";

    /** */
    private static final String JOHN_CONNOR = "John Connor";

    /** */
    private static final boolean ADD_EVT_FILTER = true;

    /** */
    private static final boolean SKIP_EVT_FILTER = false;

    /** */
    private static final boolean KEEP_BINARY = true;

    /** */
    private static final boolean SKIP_KEEP_BINARY = false;

    /** */
    private static final long LATCH_TIMEOUT = 10_000L;

    /** */
    protected boolean client = false;

    /** */
    protected CacheMode cacheMode() {
        return CacheMode.REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (client)
            cfg.setClientMode(true);
        else {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setCacheMode(cacheMode());
            ccfg.setAtomicityMode(atomicityMode());

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        gridToRunQuery().cache(DEFAULT_CACHE_NAME).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(DFLT_SERVER_NODE_CNT);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @return Grid to run query on.
     * @throws Exception If failed.
     */
    protected Ignite gridToRunQuery() throws Exception {
        return grid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformer() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, SKIP_KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAsync() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, SKIP_KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListener() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, SKIP_KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerAsync() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, SKIP_KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerWithFilter() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, SKIP_KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerWithFilterAsync() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, SKIP_KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerWithFilter() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerWithFilterAsync() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerKeepBinaryAsync() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerKeepBinaryAsync() throws Exception {
        runContinuousQueryWithTransformer(SKIP_EVT_FILTER, DFLT_ENTRY_CNT, KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerWithFilterKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerWithFilterKeepBinaryAsync() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerWithFilterKeepBinary() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousWithTransformerAndRegularListenerWithFilterKeepBinaryAsync() throws Exception {
        runContinuousQueryWithTransformer(ADD_EVT_FILTER, DFLT_ENTRY_CNT / 2, KEEP_BINARY, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformerReturnNull() throws Exception {
        Ignite ignite = gridToRunQuery();

        IgniteCache<Integer, Employee> cache = ignite.cache(DEFAULT_CACHE_NAME);

        ContinuousQueryWithTransformer<Integer, Employee, String> qry = new ContinuousQueryWithTransformer<>();

        final AtomicInteger cnt = new AtomicInteger(0);

        qry.setLocalListener(new EventListener() {
            @Override public void onUpdated(Iterable events) throws CacheEntryListenerException {
                for (Object e : events) {
                    assertNull(e);

                    cnt.incrementAndGet();
                }
            }
        });

        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(
            new IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Employee>, String>() {
                @Override public String apply(CacheEntryEvent<? extends Integer, ? extends Employee> evt) {
                    return null;
                }
        }));

        qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheEntryEventSerializableFilter<Integer, Employee>() {
            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Employee> evt) {
                return true;
            }
        }));

        try (QueryCursor<Cache.Entry<Integer, Employee>> ignored = cache.query(qry)) {
            for (int i = 0; i < 10; i++)
                cache.put(i, new Employee(JOHN_CONNOR, i));

            boolean evtsReceived = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cnt.get() == 10;
                }
            }, 20_000);

            assertTrue(evtsReceived);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpired() throws Exception {
        Ignite ignite = gridToRunQuery();

        IgniteCache<Integer, Employee> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache = cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100)));

        final Set<Integer> keys = new GridConcurrentHashSet<>();
        final CountDownLatch latch = new CountDownLatch(2);

        ContinuousQueryWithTransformer<Integer, Employee, Integer> qry = new ContinuousQueryWithTransformer<>();

        qry.setIncludeExpired(true);

        qry.setRemoteFilterFactory(FactoryBuilder.factoryOf(new CacheEntryEventSerializableFilter<Integer, Employee>() {
            @Override public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
                return event.getEventType() == EventType.EXPIRED;
            }
        }));

        qry.setRemoteTransformerFactory(FactoryBuilder.factoryOf(
            new IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Employee>, Integer>() {
                @Override public Integer apply(CacheEntryEvent<? extends Integer, ? extends Employee> evt) {
                    assertNotNull(evt.getValue());

                    assertNotNull(evt.getOldValue());

                    return evt.getKey();
                }
            }));

        qry.setLocalListener(new EventListener<Integer>() {
            @Override public void onUpdated(Iterable<? extends Integer> evts) {
                for (Integer key : evts) {
                    keys.add(key);

                    latch.countDown();
                }
            }
        });

        try (QueryCursor<Cache.Entry<Integer, Employee>> ignored = cache.query(qry)) {
            cache.put(1, new Employee(SARAH_CONNOR, 42));
            cache.put(2, new Employee(JOHN_CONNOR, 42));

            // Wait for expiration.
            latch.await(LATCH_TIMEOUT, MILLISECONDS);

            assertEquals(2, keys.size());

            assertTrue(keys.contains(1));
            assertTrue(keys.contains(2));
        }
    }

    /**
     * @param addEvtFilter Add event filter to ContinuousQueryWithTransformer flag.
     * @param expTransCnt Expected transformed event count.
     * @param keepBinary Keep binary flag.
     * @param async Flag to use transformed event listener with {@link IgniteAsyncCallback}.
     * @throws Exception If failed.
     */
    private void runContinuousQueryWithTransformer(boolean addEvtFilter, int expTransCnt, boolean keepBinary,
        boolean async)
        throws Exception {
        Ignite ignite = gridToRunQuery();

        IgniteCache<Integer, Employee> cache = ignite.cache(DEFAULT_CACHE_NAME);

        if (keepBinary)
            cache = cache.withKeepBinary();

        populateData(cache, JOHN_CONNOR);

        CountDownLatch transUpdCntLatch = new CountDownLatch(expTransCnt);

        AtomicInteger transCnt = new AtomicInteger(0);

        EventListener<String> transLsnr = async ?
            new LocalEventListenerAsync(transCnt, transUpdCntLatch) :
            new LocalEventListener(transCnt, transUpdCntLatch);

        Factory<? extends CacheEntryEventFilter> rmtFilterFactory = null;

        if (addEvtFilter)
            rmtFilterFactory = FactoryBuilder.factoryOf(new RemoteCacheEntryEventFilter());

        Factory<? extends IgniteClosure> factory = FactoryBuilder.factoryOf(new RemoteTransformer(keepBinary));

        ContinuousQueryWithTransformer<Integer, Employee, String> qry = new ContinuousQueryWithTransformer<>();

        qry.setInitialQuery(new ScanQuery<Integer, Employee>());
        qry.setRemoteFilterFactory((Factory<? extends CacheEntryEventFilter<Integer, Employee>>)rmtFilterFactory);
        qry.setRemoteTransformerFactory(
            (Factory<? extends IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Employee>, String>>)factory);
        qry.setLocalListener(transLsnr);

        try (QueryCursor<Cache.Entry<Integer, Employee>> cur = cache.query(qry)) {
            for (Cache.Entry<Integer, Employee> e : cur) {
                assertNotNull(e);

                if (keepBinary) {
                    assertTrue(((BinaryObject)e.getValue())
                        .field("name").toString().startsWith(JOHN_CONNOR));
                }
                else {
                    assertTrue(e.getValue().name.startsWith(JOHN_CONNOR));
                }
            }

            populateData(cache, SARAH_CONNOR);

            assertTrue("Receive all expected events",
                transUpdCntLatch.await(DFLT_LATCH_TIMEOUT, MILLISECONDS));
            assertEquals("Count of updated records equal to expected", expTransCnt, transCnt.get());

        }
    }

    /**
     * Put some data to cache.
     *
     * @param cache Cache to put data to.
     * @param name Base name of Employee.
     */
    private void populateData(IgniteCache<Integer, Employee> cache, String name) {
        for (int i = 0; i < DFLT_ENTRY_CNT; i++)
            cache.put(i, new Employee(name + i, 42 * i));
    }

    /**
     */
    @IgniteAsyncCallback
    private static class LocalEventListenerAsync extends LocalEventListener {
        LocalEventListenerAsync(AtomicInteger transCnt, CountDownLatch transUpdCnt) {
            super(transCnt, transUpdCnt);
        }
    }

    /**
     */
    private static class RemoteTransformer implements IgniteClosure<CacheEntryEvent<?, ?>, String> {
        /** */
        private boolean keepBinary;

        /** */
        RemoteTransformer(boolean keepBinary) {
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override public String apply(CacheEntryEvent<?, ?> evt) {
            if (keepBinary)
                return ((BinaryObject)evt.getValue()).field("name");

            return ((Employee)evt.getValue()).name;
        }
    }

    /**
     */
    private static class RemoteCacheEntryEventFilter implements CacheEntryEventSerializableFilter<Integer, Object> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(
            CacheEntryEvent<? extends Integer, ?> event) throws CacheEntryListenerException {
            return event.getKey() % 2 == 0;
        }
    }

    /**
     */
    private static class LocalEventListener implements EventListener<String> {
        /** */
        private final AtomicInteger cnt;

        /** */
        private final CountDownLatch cntLatch;

        /** */
        LocalEventListener(AtomicInteger transCnt, CountDownLatch transUpdCnt) {
            this.cnt = transCnt;
            this.cntLatch = transUpdCnt;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<? extends String> events) throws CacheEntryListenerException {
            for (String evt : events) {
                cnt.incrementAndGet();

                if (evt.startsWith(SARAH_CONNOR))
                    cntLatch.countDown();
            }
        }
    }

    /**
     */
    public class Employee {
        /** */
        public String name;

        /** */
        public Integer salary;

        /** */
        Employee(String name, Integer salary) {
            this.name = name;
            this.salary = salary;
        }
    }
}
