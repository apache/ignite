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
import javax.cache.CacheException;
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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteSqlNotNullConstraintTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of client node. */
    private static String NODE_CLIENT = "client";

    /** Number of server nodes. */
    private static int NODE_COUNT = 2;

    /** Cache prefix. */
    private static String CACHE_PREFIX = "person";

    /** Transactional person cache. */
    private static String CACHE_PERSON = "person-PARTITIONED-TRANSACTIONAL";

    /** Name of SQL table. */
    private static String TABLE_PERSON = "\"" + CACHE_PERSON +  "\".\"PERSON\"";

    /** Expected error message. */
    private static String ERR_MSG = "Null value is not allowed for field 'NAME'";

    /** OK value. */
    private final Person okValue = new Person("Name", 18);

    /** Bad value, violating constraint. */
    private final Person badValue = new Person(null, 25);

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

        QueryEntity qe = new QueryEntity(new QueryEntity(Integer.class, Person.class));

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
    public void testAtomicOrImplicitTxPutAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        cache.putAll(F.asMap(key1, okValue, key2, badValue));

                        return null;
                    }
                }, IgniteSQLException.class);

                IgniteSQLException ex = X.cause(t, IgniteSQLException.class);

                assertNotNull(ex);

                assertTrue(ex.getMessage().contains(ERR_MSG));

                assertEquals(isLocalAtomic() ? 1 : 0, cache.size());
            }
        });
    }

    /** */
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
    public void testTxPutAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                            cache.putAll(F.asMap(key1, okValue, key2, badValue));

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
    public void testAlterTableAddColumnNotNullFieldAllowed() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT)");

        executeSql("ALTER TABLE test ADD COLUMN name CHAR NOT NULL");

        checkState("PUBLIC", "TEST", "NAME");
    }

    /** */
    public void testAtomicNotNullCheckDmlInsertValues() throws Exception {
        checkNotNullCheckDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    public void testTransactionalNotNullCheckDmlInsertValues() throws Exception {
        checkNotNullCheckDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkNotNullCheckDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR NOT NULL) WITH \"atomicity="
                + atomicityMode.name() + "\"");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeSql("INSERT INTO test(id, name) " +
                    "VALUES (1, 'ok'), (2, NULLIF('a', 'a')), (3, 'ok')");

                return null;
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT id, name FROM test ORDER BY id");

        assertEquals(0, result.size());

        executeSql("INSERT INTO test(id, name) VALUES (1, 'ok'), (2, 'ok2'), (3, 'ok3')");

        result = executeSql("SELECT id, name FROM test ORDER BY id");

        assertEquals(3, result.size());
    }

    /** */
    public void testAtomicAddColumnNotNullCheckDmlInsertValues() throws Exception {
        checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    public void testTransactionalAddColumnNotNullCheckDmlInsertValues() throws Exception {
        checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkAddColumnNotNullCheckDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT) WITH \"atomicity="
            + atomicityMode.name() + "\"");

        executeSql("ALTER TABLE test ADD COLUMN name VARCHAR NOT NULL");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeSql("INSERT INTO test(id, name, age) " +
                    "VALUES (1, 'ok', 1), (2, NULLIF('a', 'a'), 2), (3, 'ok', 3)");

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
    public void testNotNullCheckDmlInsertFromSelect() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR, age INT)");

        executeSql("INSERT INTO test(id, name, age) VALUES (1, 'Macy', 25), (2, null, 25), (3, 'John', 30)");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("INSERT INTO " + TABLE_PERSON +
                    "(_key, name, age) " +
                    "SELECT id, name, age FROM test");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("SELECT _key, name FROM " + TABLE_PERSON + " ORDER BY _key");

        assertEquals(0, result.size());

        executeSql("DELETE FROM test WHERE id = 2");

        result = executeSql("INSERT INTO " + TABLE_PERSON + "(_key, name, age) " + "SELECT id, name, age FROM test");

        assertEquals(2L, result.get(0).get(0));
    }

    /** */
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
    public void testNotNullCheckDmlUpdateFromSelect() throws Exception {
        executeSql("CREATE TABLE src(id INT PRIMARY KEY, name VARCHAR)");
        executeSql("CREATE TABLE dest(id INT PRIMARY KEY, name VARCHAR NOT NULL)");

        executeSql("INSERT INTO dest(id, name) VALUES (1, 'William'), (2, 'Warren'), (3, 'Robert')");
        executeSql("INSERT INTO src(id, name) VALUES (1, 'Bill'), (2, null), (3, 'Bob')");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("UPDATE dest" +
                    " p SET (name) = " +
                    "(SELECT name FROM src t WHERE p.id = t.id)");
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

        return qryProc.querySqlFieldsNoCache(new SqlFieldsQuery(sqlText), true).getAll();
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
     * Test store.
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
}
