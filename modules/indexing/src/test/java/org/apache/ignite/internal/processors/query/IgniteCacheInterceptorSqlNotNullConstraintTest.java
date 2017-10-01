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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Tests SQL NOT NULL constraint validation for values provided by {@link CacheInterceptor#onBeforePut}
 */
public class IgniteCacheInterceptorSqlNotNullConstraintTest extends GridCommonAbstractTest {
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

    /** OK value. */
    private final Person okValue2 = new Person("Name", 19);

    /** Bad value, violating constraint. */
    private final Person badValue = new Person(null, 25);

    /** Cache store stub. */
    private final static TestStore store = new TestStore();

    /** Cache interceptor stub. */
    private final static TestInterceptor intercept = new TestInterceptor();

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
        cfg.setReadThrough(false);

        if (writeThrough)
            cfg.setWriteThrough(true);

        cfg.setLoadPreviousValue(true);

        cfg.setInterceptor(intercept);

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

    /** Test put outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(null);

                cache.put(key1, okValue);

                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.put(key1, okValue2);

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test putIfAbsent outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.putIfAbsent(key1, okValue);

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** Test getAndPut outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxGetAndPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.getAndPut(key1, okValue);

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** Test getAndPutIfAbsent outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxGetAndPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndPutIfAbsent(key1, okValue);
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** Test replace outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(null);

                cache.put(key1, okValue);

                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.replace(key1, okValue2);
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** Test getAndReplace outside of transaction or within implicit one. */
    public void testAtomicOrImplicitTxGetAndReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(null);

                cache.put(key1, okValue);

                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndReplace(key1, okValue2);
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** Test putAll outside transaction or within explicit one. */
    public void testAtomicOrImplicitTxPutAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(null);

                cache.putAll(F.asMap(key1, okValue, key2, okValue));

                intercept.setResults(key2, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.putAll(F.asMap(key1, okValue2, key2, okValue2));

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(2, cache.size());
            }
        });
    }

    /** Test invoke outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxInvoke() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.invoke(key1, new TestEntryProcessor(okValue));
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** Test invokeAll outside transaction or within implicit one. */
    public void testAtomicOrImplicitTxInvokeAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(F.asMap(key1, okValue2, key2, badValue));

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.invokeAll(F.asMap(
                            key1, new TestEntryProcessor(okValue),
                            key2, new TestEntryProcessor(okValue)));
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(configuration.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL? 0 : 1, cache.size());
            }
        });
    }

    /** Test put non-existing value, inside explicit transaction. */
    public void testTxPutCreate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(key2, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.put(key1, okValue);
                            cache.put(key2, okValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test put over existing value, within explicit transaction. */
    public void testTxPutUpdate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(null);

                        cache.put(key1, okValue);
                        cache.put(key2, okValue);

                        intercept.setResults(key2, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.put(key1, okValue2);
                            cache.put(key2, okValue2);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test putIfAbsent within explicit transaction. */
    public void testTxPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(key1, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.putIfAbsent(key1, okValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test getAndPut within explicit transaction. */
    public void testTxGetAndPut() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(key1, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndPut(key1, okValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test getAndPutIfAbsent within explicit transaction. */
    public void testTxGetAndPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(key1, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndPutIfAbsent(key1, okValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test replace within explicit transaction. */
    public void testTxReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(null);

                        cache.put(key1, okValue);

                        intercept.setResults(key1, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.replace(key1, okValue2);

                            tx.commit();
                        }

                        assertEquals(1, cache.size());
                        assertEquals(okValue, cache.get(key1));

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test getAndReplace within explicit transaction. */
    public void testTxGetAndReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(null);

                        cache.put(key1, okValue);

                        intercept.setResults(key1, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndReplace(key1, okValue2);

                            tx.commit();
                        }

                        assertEquals(1, cache.size());
                        assertEquals(okValue, cache.get(key1));

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test putAll within explicit transaction. */
    public void testTxPutAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        intercept.setResults(key2, badValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.putAll(F.asMap(key1, okValue, key2, okValue));

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);
            }
        });
    }

    /** Test invoke within explicit transaction. */
    public void testTxInvoke() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(null);

                cache.put(key1, okValue);

                intercept.setResults(key1, badValue);

                assertThrows(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.invoke(key1, new TestEntryProcessor(okValue2));

                            tx.commit();
                        }

                        return null;
                    }
                }, IgniteSQLException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** Test invokeAll within explicit transaction. */
    public void testTxInvokeAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                intercept.setResults(key2, badValue);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    assertThrows(new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            cache.invokeAll(F.asMap(
                                key1, new TestEntryProcessor(okValue),
                                key2, new TestEntryProcessor(okValue)));

                            tx.commit();

                            return null;
                        }
                    }, IgniteSQLException.class, ERR_MSG);
                }

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    private void executeWithAllCaches(TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            executeForCache(ccfg, clo, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
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

        intercept.setResults(null);

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
    private void assertThrows(Callable<?> c, Class<? extends Throwable> cls, String message) {
        Throwable t = GridTestUtils.assertThrowsWithCause(c, cls);

        t = X.cause(t, cls);

        assertNotNull(t);

        assertTrue("Unexpected error message: " + t.getMessage(), t.getMessage().contains(message));

        log.info("Caught expected exception.");
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

    /**
     * Test Cache Interceptor.
     */
    private static class TestInterceptor implements CacheInterceptor<Integer, Person> {
        /** */
        volatile Map<Integer, Person> results;

        /** */
        public void setResults(Integer k, Person v) {
            this.results = F.asMap(k, v);
        }

        /** */
        public void setResults(Map<Integer, Person> results) {
            this.results = results;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Person onGet(Integer key, @Nullable Person val) {
            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Person onBeforePut(Cache.Entry<Integer, Person> entry, Person newVal) {
            if (results == null)
                return newVal;

            Person r = results.get(entry.getKey());

            if (r == null)
                return newVal;

            return r;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Person> entry) {
            // No-op
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Person> onBeforeRemove(Cache.Entry<Integer, Person> entry) {
            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Person> entry) {
            // No-op
        }
    }
}
