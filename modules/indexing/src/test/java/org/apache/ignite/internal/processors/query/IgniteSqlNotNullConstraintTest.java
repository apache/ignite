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
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

/** */
public class IgniteSqlNotNullConstraintTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static String NODE_CLIENT = "client";

    /** */
    private static int NODE_COUNT = 2;

    /** Atomic person cache. */
    private static String CACHE_PERSON = "person";

    /** Transactional person cache. */
    private static String CACHE_PERSON_TX = "person-tx";

    /** */
    private static String TABLE_PERSON = "\"" + CACHE_PERSON +  "\".\"PERSON\"";

    /** */
    private static String ERR_MSG = "Null value is not allowed for field 'NAME'";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        CacheConfiguration ccfg = buildCacheConfiguration(gridName);

        if (ccfg != null)
            ccfgs.add(ccfg);

        ccfgs.add(buildCacheConfiguration(CACHE_PERSON));
        ccfgs.add(buildCacheConfiguration(CACHE_PERSON_TX));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        if (gridName.equals(NODE_CLIENT))
            c.setClientMode(true);

        return c;
    }

    /** */
    private CacheConfiguration buildCacheConfiguration(String cacheName) {
        if (!F.eq(cacheName, CACHE_PERSON) && !F.eq(cacheName, CACHE_PERSON_TX))
            return null;

        CacheConfiguration cfg = new CacheConfiguration(cacheName);

        cfg.setCacheMode(CacheMode.PARTITIONED);

        QueryEntity qe = new QueryEntity(Integer.class.getName(), Person.class.getName());
        qe.addQueryField("name", String.class.getName(), null);
        qe.addQueryField("age", Integer.class.getName(), null);

        qe.setNotNullFields(Collections.singleton("name"));

        cfg.setQueryEntities(F.asList(qe));

        if (F.eq(cacheName, CACHE_PERSON))
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        else if (F.eq(cacheName, CACHE_PERSON_TX))
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

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
    public void testAtomicPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        cache.put(1, macy);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.put(2, john);

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(1, cache.size());
        assertEquals(macy, cache.get(1));
        assertNull(cache.get(2));
    }

    /** */
    public void testAtomicPutIfAbsent() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.putIfAbsent(1, john);

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testAtomicGetAndPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.putIfAbsent(1, john);

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testAtomicGetAndPutIfAbsent() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.getAndPutIfAbsent(1, john);
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testAtomicReplace() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        cache.put(1, macy);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.replace(1, john);
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(1, cache.size());
        assertEquals(macy, cache.get(1));
    }

    /** */
    public void testAtomicGetAndReplace() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        cache.put(1, macy);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.getAndReplace(1, john);
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(1, cache.size());
        assertEquals(macy, cache.get(1));
    }

    /** */
    public void testAtomicPutAll() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.putAll(F.asMap(1, macy, 2, john));

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.put(1, macy);
                    cache.put(2, john);

                    tx.commit();
                }
                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalPutAfterPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.put(1, macy);
                    cache.put(2, macy);
                    cache.put(2, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalPutIfAbsent() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.putIfAbsent(1, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalGetAndPut() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.getAndPut(1, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalGetAndPutIfAbsent() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.getAndPutIfAbsent(1, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testTransactionalReplace() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        cache.put(1, macy);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.replace(1, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(1, cache.size());
        assertEquals(macy, cache.get(1));
    }

    /** */
    public void testTransactionalGetAndReplace() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        cache.put(1, macy);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.getAndReplace(1, john);

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(1, cache.size());
        assertEquals(macy, cache.get(1));
    }

    /** */
    public void testTransactionalPutAll() throws Exception {
        final IgniteCache<Integer, Person> cache = grid(NODE_CLIENT).cache(CACHE_PERSON_TX);

        final Person macy = new Person("Macy", 18);
        final Person john = new Person(null, 25);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction tx = grid(NODE_CLIENT).transactions().txStart()) {
                    cache.putAll(F.asMap(1, macy, 2, john));

                    tx.commit();
                }

                return null;
            }

        }, CacheException.class, ERR_MSG);

        assertEquals(0, cache.size());
    }

    /** */
    public void testDynamicTableCreateNotNullFieldsAllowed() throws Exception {
        executeSql("create table test(id int primary key, field int not null)");

        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "TEST");

        CacheConfiguration ccfg = grid(NODE_CLIENT).context().cache().cache(cacheName).configuration();

        QueryEntity qe = (QueryEntity)F.first(ccfg.getQueryEntities());

        assertEquals(Collections.singleton("FIELD"), qe.getNotNullFields());

        executeSql("drop table test");
    }

    /** */
    public void testAtomicNotNullCheckDmlInsertValues() throws Exception {
        executeSql("create table test(id int primary key, name varchar not null) with \"atomicity=atomic\"");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeSql("insert into test(id, name) " +
                    "values (1, 'ok'), (2, nullif('a', 'a')), (3, 'ok')");

                return null;
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("select id, name from test order by id");

        assertEquals(0, result.size());

        executeSql("drop table test");
    }

    /** */
    public void testTransactionalNotNullCheckDmlInsertValues() throws Exception {
        executeSql("create table test(id int primary key, name varchar not null) with \"atomicity=transactional\"");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("insert into test(id, name) " +
                    "values (1, 'ok'), (2, nullif('a', 'a')), (3, 'ok')");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("select id, name from test order by id");

        assertEquals(0, result.size());

        executeSql("drop table test");
    }

    /** */
    public void testNotNullCheckDmlInsertFromSelect() throws Exception {
        executeSql("create table test(id int primary key, name varchar, age int)");

        executeSql("insert into test(id, name, age) values (1, 'Macy', 25), (2, null, 25), (3, 'John', 30)");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("insert into " + TABLE_PERSON +
                    "(_key, name, age) " +
                    "select id, name, age from test");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("select _key, name from " + TABLE_PERSON + " order by _key");

        assertEquals(0, result.size());

        executeSql("drop table test");
    }

    /** */
    public void testNotNullCheckDmlUpdateValues() throws Exception {
        executeSql("create table test(id int primary key, name varchar not null)");

        executeSql("insert into test(id, name) values (1, 'John')");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("update test set name = nullif('a', 'a') where id = 1");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("select id, name from test");

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get(0));
        assertEquals("John", result.get(0).get(1));

        executeSql("drop table test");
    }

    /** */
    public void testNotNullCheckDmlUpdateFromSelect() throws Exception {
        executeSql("create table src(id int primary key, name varchar)");
        executeSql("create table dest(id int primary key, name varchar not null)");

        executeSql("insert into dest(id, name) values (1, 'William'), (2, 'Warren'), (3, 'Robert')");
        executeSql("insert into src(id, name) values (1, 'Bill'), (2, null), (3, 'Bob')");

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                return executeSql("update dest" +
                    " p set (name) = " +
                    "(select name from src t where p.id = t.id)");
            }
        }, IgniteSQLException.class, ERR_MSG);

        List<List<?>> result = executeSql("select id, name from dest order by id");

        assertEquals(3, result.size());

        assertEquals(1, result.get(0).get(0));
        assertEquals("William", result.get(0).get(1));

        assertEquals(2, result.get(1).get(0));
        assertEquals("Warren", result.get(1).get(1));

        assertEquals(3, result.get(2).get(0));
        assertEquals("Robert", result.get(2).get(1));
    }

    /** */
    private List<List<?>> executeSql(String sqlText) throws Exception {
        GridQueryProcessor qryProc = grid(NODE_CLIENT).context().query();

        return qryProc.querySqlFieldsNoCache(new SqlFieldsQuery(sqlText), true).getAll();
    }

    /** */
    private void cleanup() throws Exception {
        IgniteEx client = grid(NODE_CLIENT);

        client.cache(CACHE_PERSON).clear();
        client.cache(CACHE_PERSON_TX).clear();

        executeSql("drop table test if exists");
    }

    /** */
    public static class Person {
        /** */
        private String name;

        /** */
        private int age;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /** */
        @Override public int hashCode() {
            return name.hashCode() ^ age;
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
}
