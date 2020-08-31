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
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class IgniteSqlNotNullConstraintTest extends AbstractIndexingCommonTest {
    /** Name of client node. */
    private static final String NODE_CLIENT = "client";

    /** Number of server nodes. */
    private static final int NODE_COUNT = 2;

    /** Cache prefix. */
    private static final String CACHE_PREFIX = "person";

    /** Transactional person cache. */
    private static final String CACHE_PERSON = "person-PARTITIONED-TRANSACTIONAL";

    /** Name of SQL table. */
    private static final String TABLE_PERSON = "\"" + CACHE_PERSON + "\".\"PERSON\"";

    /** Template of cache with read-through setting. */
    private static final String CACHE_READ_THROUGH = "cacheReadThrough";

    /** Template of cache with interceptor setting. */
    private static final String CACHE_INTERCEPTOR = "cacheInterceptor";

    /** Expected error message. */
    private static final String ERR_MSG = "Null value is not allowed for column 'NAME'";

    /** Expected error message for read-through restriction. */
    private static final String READ_THROUGH_ERR_MSG = "NOT NULL constraint is not supported when " +
        "CacheConfiguration.readThrough is enabled.";

    /** Expected error message for cache interceptor restriction. */
    private static final String INTERCEPTOR_ERR_MSG = "NOT NULL constraint is not supported when " +
        "CacheConfiguration.interceptor is set.";

    /** Name of the node which configuration includes restricted cache config. */
    private static final String READ_THROUGH_CFG_NODE_NAME = "nodeCacheReadThrough";

    /** Name of the node which configuration includes restricted cache config. */
    private static final String INTERCEPTOR_CFG_NODE_NAME = "nodeCacheInterceptor";

    /** OK value. */
    private final Person okValue = new Person("Name", 18);

    /** Bad value, violating constraint. */
    private final Person badValue = new Person(null, 25);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.addAll(cacheConfigurations());

        if (gridName.equals(READ_THROUGH_CFG_NODE_NAME))
            ccfgs.add(buildCacheConfigurationRestricted("BadCfgTestCacheRT", true, false, true));

        if (gridName.equals(INTERCEPTOR_CFG_NODE_NAME))
            ccfgs.add(buildCacheConfigurationRestricted("BadCfgTestCacheINT", false, true, true));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        if (gridName.equals(NODE_CLIENT)) {
            // Not allowed to have local cache on client without memory config
            c.setDataStorageConfiguration(new DataStorageConfiguration());
        }

        return c;
    }

    /** */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> res = new ArrayList<>();

        for (boolean wrt : new boolean[] {false, true}) {
            for (boolean annot : new boolean[] {false, true}) {
                res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));

                res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));

                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, true, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, true, wrt, annot));
            }
        }

        return res;
    }

    /** */
    private CacheConfiguration buildCacheConfiguration(CacheMode mode,
        CacheAtomicityMode atomicityMode, boolean hasNear, boolean writeThrough, boolean notNullAnnotated) {

        CacheConfiguration cfg = new CacheConfiguration(CACHE_PREFIX + "-" +
            mode.name() + "-" + atomicityMode.name() + (hasNear ? "-near" : "") +
            (writeThrough ? "-writethrough" : "") + (notNullAnnotated ? "-annot" : ""));

        cfg.setCacheMode(mode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qe = new QueryEntity(new QueryEntity(Integer.class, Person.class));

        if (!notNullAnnotated)
            qe.setNotNullFields(Collections.singleton("name"));

        cfg.setQueryEntities(F.asList(qe));

        if (hasNear)
            cfg.setNearConfiguration(new NearCacheConfiguration().setNearStartSize(100));

        if (writeThrough) {
            cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
            cfg.setWriteThrough(true);
        }

        return cfg;
    }

    /** */
    private CacheConfiguration buildCacheConfigurationRestricted(String cacheName, boolean readThrough,
        boolean interceptor, boolean hasQueryEntity) {
        CacheConfiguration cfg = new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (readThrough) {
            cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
            cfg.setReadThrough(true);
        }

        if (interceptor)
            cfg.setInterceptor(new TestInterceptor());

        if (hasQueryEntity) {
            cfg.setQueryEntities(F.asList(new QueryEntity(Integer.class, Person.class)
                .setNotNullFields(Collections.singleton("name"))));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        startClientGrid(NODE_CLIENT);

        // Add cache template with read-through cache store.
        grid(NODE_CLIENT).addCacheConfiguration(
            buildCacheConfigurationRestricted(CACHE_READ_THROUGH, true, false, false));

        // Add cache template with cache interceptor.
        grid(NODE_CLIENT).addCacheConfiguration(
            buildCacheConfigurationRestricted(CACHE_INTERCEPTOR, false, true, false));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanup();
    }

    /**
     * We need to keep serialized cache store factories to share them between tests.
     */
    @Override protected boolean keepSerializedObjects() {
        return true;
    }

    /** */
    @Test
    public void testQueryEntityGetSetNotNullFields() throws Exception {
        QueryEntity qe = new QueryEntity();

        assertNull(qe.getNotNullFields());

        Set<String> val = Collections.singleton("test");

        qe.setNotNullFields(val);

        assertEquals(val, Collections.singleton("test"));

        qe.setNotNullFields(null);

        assertNull(qe.getNotNullFields());
    }

    /** */
    @Test
    public void testQueryEntityEquals() throws Exception {
        QueryEntity a = new QueryEntity();

        QueryEntity b = new QueryEntity();

        assertEquals(a, b);

        a.setNotNullFields(Collections.singleton("test"));

        assertFalse(a.equals(b));

        b.setNotNullFields(Collections.singleton("test"));

        assertTrue(a.equals(b));
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.put(key2, badValue);

                        return null;
                    }
                }, IgniteCheckedException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.putIfAbsent(key1, badValue);

                        return null;
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxGetAndPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.getAndPut(key1, badValue);

                        return null;
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxGetAndPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                assertEquals(0, cache.size());

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndPutIfAbsent(key1, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.replace(key1, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxGetAndReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.getAndReplace(key1, badValue);
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxPutAll() throws Exception {
        doAtomicOrImplicitTxPutAll(F.asMap(1, okValue, 5, badValue), 1);
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxPutAllForSingleValue() throws Exception {
        doAtomicOrImplicitTxPutAll(F.asMap(5, badValue), 0);
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxInvoke() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return cache.invoke(key1, new TestEntryProcessor(badValue));
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxInvokeAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                    key1, new TestEntryProcessor(okValue),
                    key2, new TestEntryProcessor(badValue)));

                assertNotNull(r);

                GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return r.get(key2).get();
                    }
                }, IgniteCheckedException.class, ERR_MSG);

                assertEquals(1, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testTxPutCreate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.put(key1, okValue);
                            cache.put(key2, badValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxPutUpdate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.put(key1, okValue);
                            cache.put(key2, okValue);
                            cache.put(key2, badValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.putIfAbsent(key1, badValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxGetAndPut() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndPut(key1, badValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxGetAndPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndPutIfAbsent(key1, badValue);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.put(1, okValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.replace(key1, badValue);

                            tx.commit();
                        }

                        assertEquals(1, cache.size());
                        assertEquals(okValue, cache.get(key1));

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxGetAndReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.put(key1, okValue);

                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.getAndReplace(key1, badValue);

                            tx.commit();
                        }

                        assertEquals(1, cache.size());
                        assertEquals(okValue, cache.get(key1));

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
            }
        });
    }

    /** */
    @Test
    public void testTxPutAll() throws Exception {
        doTxPutAll(F.asMap(1, okValue, 5, badValue));
    }

    /** */
    @Test
    public void testTxPutAllForSingleValue() throws Exception {
        doTxPutAll(F.asMap(5, badValue));
    }

    /** */
    @Test
    public void testTxInvoke() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.invoke(key1, new TestEntryProcessor(badValue));

                            tx.commit();
                        }

                        return null;
                    }
                }, EntryProcessorException.class, ERR_MSG);

                assertEquals(1, cache.size());
                assertEquals(okValue, cache.get(key1));
            }
        });
    }

    /** */
    @Test
    public void testTxInvokeAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                        key1, new TestEntryProcessor(okValue),
                        key2, new TestEntryProcessor(badValue)));

                    assertNotNull(r);

                    GridTestUtils.assertThrows(log, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return r.get(key2).get();
                        }
                    }, EntryProcessorException.class, ERR_MSG);

                    tx.rollback();
                }

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxInvokeDelete() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                cache.invoke(key1, new TestEntryProcessor(null));

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testAtomicOrImplicitTxInvokeAllDelete() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);
                cache.put(key2, okValue);

                cache.invokeAll(F.asMap(
                    key1, new TestEntryProcessor(null),
                    key2, new TestEntryProcessor(null)));

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testTxInvokeDelete() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.invoke(key1, new TestEntryProcessor(null));

                    tx.commit();
                }

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testTxInvokeAllDelete() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, okValue);
                cache.put(key2, okValue);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.invokeAll(F.asMap(
                        key1, new TestEntryProcessor(null),
                        key2, new TestEntryProcessor(null)));

                    tx.commit();
                }

                assertEquals(0, cache.size());
            }
        });
    }

    /** */
    @Test
    public void testDynamicTableCreateNotNullFieldsAllowed() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, field INT NOT NULL)");

        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "TEST");

        IgniteEx client = grid(NODE_CLIENT);

        CacheConfiguration ccfg = client.context().cache().cache(cacheName).configuration();

        QueryEntity qe = (QueryEntity)F.first(ccfg.getQueryEntities());

        assertEquals(Collections.singleton("FIELD"), qe.getNotNullFields());

        checkState("PUBLIC", "TEST", "FIELD");
    }

    /** */
    @Test
    public void testAlterTableAddColumnNotNullFieldAllowed() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT)");

        executeSql("ALTER TABLE test ADD COLUMN name CHAR NOT NULL");

        checkState("PUBLIC", "TEST", "NAME");
    }

    /** */
    @Test
    public void testAtomicNotNullCheckDmlInsertValues() throws Exception {
        checkNotNullCheckDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testTransactionalNotNullCheckDmlInsertValues() throws Exception {
        checkNotNullCheckDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkNotNullCheckDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR NOT NULL, age INT) WITH \"atomicity="
            + atomicityMode.name() + "\"");

        checkNotNullInsertValues();
    }

    /** */
    @Test
    public void testAtomicAddColumnNotNullCheckDmlInsertValues() throws Exception {
        checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    @Test
    public void testTransactionalAddColumnNotNullCheckDmlInsertValues() throws Exception {
        checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT) WITH \"atomicity="
            + atomicityMode.name() + "\"");

        executeSql("ALTER TABLE test ADD COLUMN name VARCHAR NOT NULL");

        checkNotNullInsertValues();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNotNullInsertValues() throws Exception {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeSql("INSERT INTO test(id, name, age) VALUES (2, NULLIF('a', 'a'), 2)");

                return null;
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT id, name, age FROM test ORDER BY id");

        assertEquals(0, result.size());

        executeSql("INSERT INTO test(id, name) VALUES (1, 'ok'), (2, 'ok2'), (3, 'ok3')");

        result = executeSql("SELECT id, name FROM test ORDER BY id");

        assertEquals(3, result.size());
    }

    /** */
    @Test
    public void testNotNullCheckDmlInsertFromSelect() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR, age INT)");

        executeSql("INSERT INTO test(id, name, age) VALUES (2, null, 25)");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("INSERT INTO " + TABLE_PERSON +
                    "(_key, name, age) " +
                    "SELECT id, name, age FROM test");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT _key, name FROM " + TABLE_PERSON + " ORDER BY _key");

        assertEquals(0, result.size());

        executeSql("INSERT INTO test(id, name, age) VALUES (1, 'Macy', 25), (3, 'John', 30)");
        executeSql("DELETE FROM test WHERE id = 2");

        result = executeSql("INSERT INTO " + TABLE_PERSON + "(_key, name, age) " + "SELECT id, name, age FROM test");

        assertEquals(2L, result.get(0).get(0));
    }

    /** */
    @Test
    public void testNotNullCheckDmlUpdateValues() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR NOT NULL)");

        executeSql("INSERT INTO test(id, name) VALUES (1, 'John')");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("UPDATE test SET name = NULLIF(id, 1) WHERE id = 1");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT id, name FROM test");

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get(0));
        assertEquals("John", result.get(0).get(1));

        executeSql("UPDATE test SET name = 'James' WHERE id = 1");

        result = executeSql("SELECT id, name FROM test");

        assertEquals(1, result.get(0).get(0));
        assertEquals("James", result.get(0).get(1));
    }

    /** */
    @Test
    public void testNotNullCheckDmlUpdateFromSelect() throws Exception {
        executeSql("CREATE TABLE src(id INT PRIMARY KEY, name VARCHAR)");
        executeSql("CREATE TABLE dest(id INT PRIMARY KEY, name VARCHAR NOT NULL)");

        executeSql("INSERT INTO dest(id, name) VALUES (1, 'William'), (2, 'Warren'), (3, 'Robert')");
        executeSql("INSERT INTO src(id, name) VALUES (1, 'Bill'), (2, null), (3, 'Bob')");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("UPDATE dest p " +
                    "SET (name) = (SELECT name FROM src t WHERE p.id = t.id) " +
                    "WHERE p.id = 2");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT id, name FROM dest ORDER BY id");

        assertEquals(3, result.size());

        assertEquals(1, result.get(0).get(0));
        assertEquals("William", result.get(0).get(1));

        assertEquals(2, result.get(1).get(0));
        assertEquals("Warren", result.get(1).get(1));

        assertEquals(3, result.get(2).get(0));
        assertEquals("Robert", result.get(2).get(1));

        executeSql("UPDATE src SET name = 'Ren' WHERE id = 2");

        executeSql("UPDATE dest p SET (name) = (SELECT name FROM src t WHERE p.id = t.id)");

        result = executeSql("SELECT id, name FROM dest ORDER BY id");

        assertEquals(3, result.size());

        assertEquals(1, result.get(0).get(0));
        assertEquals("Bill", result.get(0).get(1));

        assertEquals(2, result.get(1).get(0));
        assertEquals("Ren", result.get(1).get(1));

        assertEquals(3, result.get(2).get(0));
        assertEquals("Bob", result.get(2).get(1));
    }

    /** Check QueryEntity configuration fails with NOT NULL field and read-through. */
    @Test
    public void testReadThroughRestrictionQueryEntity() throws Exception {
        // Node start-up failure (read-through cache store).
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startClientGrid(READ_THROUGH_CFG_NODE_NAME);
            }
        }, IgniteCheckedException.class, READ_THROUGH_ERR_MSG);

        // Dynamic cache start-up failure (read-through cache store)
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return grid(NODE_CLIENT).createCache(
                    buildCacheConfigurationRestricted("dynBadCfgCacheRT", true, false, true));
            }
        }, IgniteCheckedException.class, READ_THROUGH_ERR_MSG);
    }

    /** Check QueryEntity configuration fails with NOT NULL field and cache interceptor. */
    @Test
    public void testInterceptorRestrictionQueryEntity() throws Exception {
        // Node start-up failure (interceptor).
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startClientGrid(INTERCEPTOR_CFG_NODE_NAME);
            }
        }, IgniteCheckedException.class, INTERCEPTOR_ERR_MSG);

        // Dynamic cache start-up failure (interceptor)
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return grid(NODE_CLIENT).createCache(
                    buildCacheConfigurationRestricted("dynBadCfgCacheINT", false, true, true));
            }
        }, IgniteCheckedException.class, INTERCEPTOR_ERR_MSG);
    }

    /** Check create table fails with NOT NULL field and read-through. */
    @Test
    public void testReadThroughRestrictionCreateTable() throws Exception {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("CREATE TABLE test(id INT PRIMARY KEY, name char NOT NULL) " +
                    "WITH \"template=" + CACHE_READ_THROUGH + "\"");
            }
        }, IgniteSQLException.class, READ_THROUGH_ERR_MSG);
    }

    /** Check create table fails with NOT NULL field and cache interceptor. */
    @Test
    public void testInterceptorRestrictionCreateTable() throws Exception {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("CREATE TABLE test(id INT PRIMARY KEY, name char NOT NULL) " +
                    "WITH \"template=" + CACHE_INTERCEPTOR + "\"");
            }
        }, IgniteSQLException.class, INTERCEPTOR_ERR_MSG);
    }

    /** Check alter table fails with NOT NULL field and read-through. */
    @Test
    public void testReadThroughRestrictionAlterTable() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT) " +
            "WITH \"template=" + CACHE_READ_THROUGH + "\"");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("ALTER TABLE test ADD COLUMN name char NOT NULL");
            }
        }, IgniteSQLException.class, READ_THROUGH_ERR_MSG);
    }

    /** Check alter table fails with NOT NULL field and cache interceptor. */
    @Test
    public void testInterceptorRestrictionAlterTable() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT) " +
            "WITH \"template=" + CACHE_INTERCEPTOR + "\"");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("ALTER TABLE test ADD COLUMN name char NOT NULL");
            }
        }, IgniteSQLException.class, INTERCEPTOR_ERR_MSG);
    }

    /** */
    private void doAtomicOrImplicitTxPutAll(final Map<Integer, Person> values, int expAtomicCacheSize) throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.putAll(values);

                        return null;
                    }
                }, IgniteSQLException.class);

                IgniteSQLException ex = X.cause(t, IgniteSQLException.class);

                assertNotNull(ex);

                assertTrue(ex.getMessage().contains(ERR_MSG));

                assertEquals(isLocalAtomic() ? expAtomicCacheSize : 0, cache.size());
            }
        });
    }

    /** */
    private void doTxPutAll(Map<Integer, Person> values) throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.putAll(values);

                            tx.commit();
                        }

                        assertEquals(0, cache.size());

                        return null;
                    }
                }, CacheException.class, ERR_MSG);
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

        cache.removeAll();

        assertEquals(0, cache.size());

        clo.configure(ignite, cache, concurrency, isolation);

        log.info("Running test with node " + ignite.name() + ", cache " + cacheName);

        clo.key1 = 1;
        clo.key2 = 4;

        clo.run();
    }

    /** */
    private List<List<?>> executeSql(String sqlText) throws Exception {
        GridQueryProcessor qryProc = grid(NODE_CLIENT).context().query();

        return qryProc.querySqlFields(new SqlFieldsQuery(sqlText), true).getAll();
    }

    /** */
    private void cleanup() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
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

        executeSql("DROP TABLE test IF EXISTS");
    }

    /** */
    private void checkState(String schemaName, String tableName, String fieldName) {
        IgniteEx client = grid(NODE_CLIENT);

        checkNodeState(client, schemaName, tableName, fieldName);

        for (int i = 0; i < NODE_COUNT; i++)
            checkNodeState(grid(i), schemaName, tableName, fieldName);
    }

    /** */
    private void checkNodeState(IgniteEx node, String schemaName, String tableName, String fieldName) {
        String cacheName = F.eq(schemaName, QueryUtils.DFLT_SCHEMA) ?
            QueryUtils.createTableCacheName(schemaName, tableName) : schemaName;

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

        assertNotNull("Cache descriptor not found", desc);

        QuerySchema schema = desc.schema();

        assertNotNull(schema);

        QueryEntity entity = null;

        for (QueryEntity e : schema.entities()) {
            if (F.eq(tableName, e.getTableName())) {
                entity = e;

                break;
            }
        }

        assertNotNull(entity);

        assertNotNull(entity.getNotNullFields());

        assertTrue(entity.getNotNullFields().contains(fieldName));
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(notNull = true)
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

            if (value == null)
                entry.remove();
            else
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
        }

        /** */
        protected boolean isLocalAtomic() {
            CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

            return cfg.getCacheMode() == CacheMode.LOCAL && cfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC;
        }

        /** */
        public abstract void run() throws Exception;
    }

    /**
     * Test cache store stub.
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Person> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Person> clo, @Nullable Object... args) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public Person load(Integer key) {
            return null;
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
     * Test interceptor stub.
     */
    private static class TestInterceptor implements CacheInterceptor<Integer, Person> {
        /** {@inheritDoc} */
        @Nullable @Override public Person onGet(Integer key, @Nullable Person val) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Person onBeforePut(Cache.Entry<Integer, Person> entry, Person newVal) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Person> entry) {
            // No-op
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Person> onBeforeRemove(Cache.Entry<Integer, Person> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Person> entry) {
            // No-op
        }
    }
}
