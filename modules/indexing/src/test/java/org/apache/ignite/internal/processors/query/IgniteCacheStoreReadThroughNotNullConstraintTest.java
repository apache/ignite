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
package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Tests validation of NOT NULL SQL constraint when the value is loaded from cache store.
 */
public class IgniteCacheStoreReadThroughNotNullConstraintTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of client node. */
    private static String NODE_CLIENT = "client";

    /** Number of server nodes. */
    private static int NODE_COUNT = 2;

    /** Cache prefix. */
    private static String CACHE_PREFIX = "person";

    /** Expected error message. */
    private static String ERR_MSG = "Null value is not allowed for field 'NAME'";

    /** OK value. */
    private final Person okValue = new Person("Name", 18);

    /** Bad value, violating constraint. */
    private final Person badValue = new Person(null, 25);

    /** Cache store stub. */
    private final static TestStore store = new TestStore();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);
        disco.setForceServerMode(true);

        c.setDiscoverySpi(disco);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.addAll(cacheConfigurations());

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        if (gridName.equals(NODE_CLIENT)) {
            c.setClientMode(true);

            // Not allowed to have local cache on client without memory config
            c.setMemoryConfiguration(new MemoryConfiguration());
        }

        return c;
    }

    /** */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> res = new ArrayList<>();

        for (boolean wrt : new boolean[] { false, true}) {
            res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.ATOMIC, false, wrt));
            res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.TRANSACTIONAL, false, wrt));

            res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, false, wrt));
            res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL, false, wrt));

            res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, false, wrt));
            res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, true, wrt));
            res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, false, wrt));
            res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, true, wrt));
        }

        return res;
    }
    /** */
    private CacheConfiguration buildCacheConfiguration(CacheMode mode,
        CacheAtomicityMode atomicityMode, boolean hasNear, boolean writeThrough) {

        CacheConfiguration cfg = new CacheConfiguration(CACHE_PREFIX + "-" +
            mode.name() + "-" + atomicityMode.name() + (hasNear ? "-near" : "") +
            (writeThrough ? "-writethrough" : ""));

        cfg.setCacheMode(mode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qe = new QueryEntity(Integer.class, Person.class);

        qe.setNotNullFields(Collections.singleton("name"));

        cfg.setQueryEntities(F.asList(qe));

        if (hasNear)
            cfg.setNearConfiguration(new NearCacheConfiguration().setNearStartSize(100));

        cfg.setCacheStoreFactory(singletonFactory(store));
        cfg.setReadThrough(true);

        if (writeThrough)
            cfg.setWriteThrough(true);

        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        startGrid(NODE_CLIENT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanup();
    }

    /** */
    public void testInvoke() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, badValue));

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.invoke(key1, new TestErrorEntryProcessor());
                    }
                }, EntryProcessorException.class, "");

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testTxInvoke() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, badValue));

                Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.invoke(key1, new TestErrorEntryProcessor());

                            tx.commit();
                        }

                        return null;
                    }
                }, EntryProcessorException.class);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testInvokeAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                    key1, new TestErrorEntryProcessor(),
                    key2, new TestErrorEntryProcessor()));

                assertNotNull(r);

                assertEquals(1, cache.size());

                assertTrue(cache.containsKey(key1));

                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** */
    public void testTxInvokeAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                        key1, new TestErrorEntryProcessor(),
                        key2, new TestErrorEntryProcessor()));

                    assertNotNull(r);

                    tx.commit();
                }

                assertEquals(1, cache.size());

                assertTrue(cache.containsKey(key1));

                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** */
    public void testGet() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(key1, badValue);

                assertNull(cache.get(key1));

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testTxGet() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(key1, badValue);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    assertNull(cache.get(key1));

                    tx.commit();
                }

                assertNull(cache.get(key1));

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testGetAndPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(key1, badValue);

                assertNull(cache.getAndPut(key1, okValue));

                assertEquals(1, cache.size());
            }
        });
    }

    /** */
    public void testTxGetAndPut() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(key1, badValue);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    assertNull(cache.getAndPut(key1, okValue));

                    tx.commit();
                }

                assertEquals(1, cache.size());
            }
        });
    }

    /** */
    public void testGetAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                cache.getAll(new LinkedHashSet<>(Arrays.asList(key1, key2)));

                assertEquals(1, cache.size());

                assertTrue(cache.containsKey(key1));
            }
        });
    }

    /** */
    public void testTxGetAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    Map<Integer, Person> r = cache.getAll(new LinkedHashSet<>(Arrays.asList(key1, key2)));

                    assertNotNull(r);

                    assertEquals(okValue, r.get(key1));
                    assertNull(r.get(key2));

                    tx.commit();
                }

                assertEquals((configuration.getCacheMode() == CacheMode.LOCAL &&
                    concurrency == TransactionConcurrency.PESSIMISTIC &&
                    isolation != TransactionIsolation.READ_COMMITTED)? 0 : 1, cache.size());
            }
        });
    }

    /** */
    public void testGetAndPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, badValue, key2, okValue));

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndPutIfAbsent(key1, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndPutIfAbsent(key2, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testGetAndReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, badValue, key2, okValue));

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndReplace(key1, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndReplace(key2, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    public void testLoadCache() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                cache.loadCache(null);

                assertEquals(1, cache.size());

                assertTrue(cache.containsKey(key1));
            }
        });
    }

    /** */
    public void testLoadAll() throws Exception {
        executeWithAllNonLocalCaches(new TestClosure() {
            @Override public void run() throws Exception {
                store.setResults(F.asMap(key1, okValue, key2, badValue));

                TestCompletionListener listener = new TestCompletionListener();

                cache.loadAll(new LinkedHashSet<>(Arrays.asList(key1, key2)), false, listener);

                listener.await();

                assertEquals(1, cache.size());

                assertTrue(cache.containsKey(key1));
            }
        });
    }

    /** */
    private void executeWithAllCaches(TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            executeForCache(ccfg, clo, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /** */
    private void executeWithAllNonLocalCaches(TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
            if (ccfg.getCacheMode() != CacheMode.LOCAL)
                executeForCache(ccfg, clo, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        }
    }

    /** */
    private void executeWithAllTxCaches(final TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
            if (ccfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL) {

                for (TransactionConcurrency con : TransactionConcurrency.values()) {
                    for (TransactionIsolation iso : TransactionIsolation.values())
                        executeForCache(ccfg, clo, con, iso);
                }
            }
        }
    }

    /** */
    private void executeForCache(CacheConfiguration ccfg, TestClosure clo, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {

        Ignite ignite = grid(NODE_CLIENT);
        executeForNodeAndCache(ccfg, ignite, clo, concurrency, isolation);

        for (int node = 0; node < NODE_COUNT; node++) {
            ignite = grid(node);

            executeForNodeAndCache(ccfg, ignite, clo, concurrency, isolation);
        }
    }

    /** */
    private void executeForNodeAndCache(CacheConfiguration ccfg, Ignite ignite, TestClosure clo,
        TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        String cacheName = ccfg.getName();

        IgniteCache cache;

        if (ignite.configuration().isClientMode() &&
            ccfg.getCacheMode() == CacheMode.PARTITIONED &&
            ccfg.getNearConfiguration() != null)
            cache = ignite.getOrCreateNearCache(ccfg.getName(), ccfg.getNearConfiguration());
        else
            cache = ignite.cache(ccfg.getName());

        cache.removeAll();

        assertEquals(0, cache.size());

        clo.configure(ignite, cache, concurrency, isolation);

        StringBuilder sb = new StringBuilder("Running test with node " + ignite.name() + ", cache " + cacheName);

        if (ccfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
            sb.append(", concurrency=" + concurrency + ", isolation=" + isolation);

        log.info(sb.toString());

        clo.key1 = 1;
        clo.key2 = 4;

        clo.run();
    }

    /** */
    private void cleanup() throws Exception {
        for (CacheConfiguration ccfg: cacheConfigurations()) {
            String cacheName = ccfg.getName();

            if (ccfg.getCacheMode() == CacheMode.LOCAL) {
                grid(NODE_CLIENT).cache(cacheName).clear();

                for (int node = 0; node < NODE_COUNT; node++)
                    grid(node).cache(cacheName).clear();
            }
            else {
                if (ccfg.getCacheMode() == CacheMode.PARTITIONED && ccfg.getNearConfiguration() != null) {
                    IgniteCache cache = grid(NODE_CLIENT).getOrCreateNearCache(cacheName, ccfg.getNearConfiguration());

                    cache.clear();
                }

                grid(NODE_CLIENT).cache(cacheName).clear();
            }
        }
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField
        private String name;

        /** */
        @QuerySqlField
        private int age;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /** */
        @Override public int hashCode() {
            return (name == null ? 0 : name.hashCode()) ^ age;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Person))
                return false;

            Person other = (Person)o;

            return F.eq(other.name, name) && other.age == age;
        }
    }

    /** */
    public class TestEntryProcessor implements EntryProcessor<Integer, Person, Object> {
        /** Value to set. */
        private Person value;

        /** */
        public TestEntryProcessor(Person value) {
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Person> entry,
            Object... objects) throws EntryProcessorException {

            entry.setValue(value);

            return null;
        }
    }

    /** */
    public class TestErrorEntryProcessor implements EntryProcessor<Integer, Person, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Person> entry,
            Object... objects) throws EntryProcessorException {

            throw new EntryProcessorException("Error originating from entry processor");
        }
    }

    /** */
    public abstract class TestClosure {
        /** */
        protected Ignite ignite;

        /** */
        protected IgniteCache<Integer, Person> cache;

        /** */
        protected TransactionConcurrency concurrency;

        /** */
        protected TransactionIsolation isolation;

        /** */
        protected CacheConfiguration configuration;

        /** */
        public int key1;

        /** */
        public int key2;

        /** */
        public void configure(Ignite ignite, IgniteCache<Integer, Person> cache, TransactionConcurrency concurrency,
            TransactionIsolation isolation) {
            this.ignite = ignite;
            this.cache = cache;
            this.concurrency = concurrency;
            this.isolation = isolation;
            this.configuration = cache.getConfiguration(CacheConfiguration.class);
        }

        /** */
        public abstract void run() throws Exception;
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Person> {
        volatile Map<Integer, Person> results;

        /** */
        public void setResults(Map<Integer, Person> results) {
            this.results = results;
        }

        /** */
        public void setResults(Integer k, Person v) {
            this.results = F.asMap(k, v);
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Person> clo, @Nullable Object... args) {
            for (Map.Entry<Integer, Person> e: results.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Person load(Integer key) {
            return results.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Person> e) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op
        }
    }

    /** */
    private static class TestCompletionListener implements CompletionListener {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        /** */
        @Override public void onCompletion() {
            fut.onDone();
        }

        /** */
        @Override public void onException(Exception e) {
            fut.onDone(e);
        }

        /** */
        public void await() throws Exception {
            fut.get(5000);
        }
    }
}
