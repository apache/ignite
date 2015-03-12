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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.*;
import static org.apache.ignite.events.EventType.*;
import static org.junit.Assert.*;

/**
 * Various tests for cache queries.
 */
public abstract class GridCacheAbstractQuerySelfTest extends GridCommonAbstractTest {
    /** Cache store. */
    private static TestStore store = new TestStore();

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final UUID subjId = UUID.fromString("8EB3B06D-0885-4B4A-9A54-02C93EF09B65");

    /** */
    protected Ignite ignite;

    /**
     * @return Grid count.
     */
    protected abstract int gridCount();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Distribution.
     */
    protected CacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        QueryConfiguration idxCfg = new QueryConfiguration();

        idxCfg.setIndexCustomFunctionClasses(SqlFunctions.class);

        c.setQueryConfiguration(idxCfg);

        // Otherwise noop swap space will be chosen on Windows.
        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        c.setMarshaller(new OptimizedMarshaller(false));

        CacheConfiguration[] ccs = new CacheConfiguration[2];

        for (int i = 0; i < ccs.length; i++) {
            CacheConfiguration cc = defaultCacheConfiguration();

            if (i > 0)
                cc.setName("c" + i);

            cc.setCacheMode(cacheMode());
            cc.setAtomicityMode(atomicityMode());
            cc.setDistributionMode(gridName.startsWith("client") ? CLIENT_ONLY : distributionMode());
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);
            cc.setRebalanceMode(SYNC);
            cc.setSwapEnabled(true);
            cc.setEvictNearSynchronized(false);

            CacheQueryConfiguration qcfg = new CacheQueryConfiguration();

            qcfg.setIndexPrimitiveKey(true);
            qcfg.setIndexFixedTyping(true);

            cc.setQueryConfiguration(qcfg);

            // Explicitly set number of backups equal to number of grids.
            if (cacheMode() == CacheMode.PARTITIONED)
                cc.setBackups(gridCount());

            ccs[i] = cc;
        }

        c.setCacheConfiguration(ccs);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        store.reset();

        ignite = null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testDifferentKeyTypes() throws Exception {
        GridCache<Object, Object> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx("key", "value");

        // Put the same value but for other key type.
        // Operation should succeed but with warning log message.
        cache.putx(1, "value");
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testDifferentValueTypes() throws Exception {
        GridCache<Object, Object> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx("key", "value");

        // Put value of different type but for the same key type.
        // Operation should succeed but with warning log message.
        cache.putx("key", 1);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testStringType() throws Exception {
        GridCache<String, String> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx("tst", "test");

        CacheQuery<Map.Entry<String, String>> qry = cache.queries().createSqlQuery(String.class, "_val='test'");

        Map.Entry<String, String> entry = F.first(qry.execute().get());

        assert entry != null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testIntegerType() throws Exception {
        GridCache<String, Integer> cache = ((IgniteKernal)ignite).cache(null);

        String key = "k";

        int val = 2;

        cache.putx(key, val);

        CacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class,
            "select * from Integer where _key = 'k' and _val > 1");

        Map.Entry<String, Integer> entry = F.first(qry.execute().get());

        assert entry != null;

        assertEquals(Integer.valueOf(val), entry.getValue());
    }

    /**
     * Tests UDFs.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testUserDefinedFunction() throws IgniteCheckedException {
        // Without alias.
        CacheQuery<List<?>> qry = ((IgniteKernal)ignite).cache(null).queries().createSqlFieldsQuery("select square(1), square(2)").
            projection(ignite.cluster());

        CacheQueryFuture<List<?>> fut = qry.execute();

        Collection<List<?>> res = fut.get();

        assertEquals(gridCount(), res.size());

        List<?> row = res.iterator().next();

        assertEquals(1, row.get(0));
        assertEquals(4, row.get(1));

        // With alias.
        qry = ((IgniteKernal)ignite).cache(null).queries().createSqlFieldsQuery("select _cube_(1), _cube_(2)").projection(ignite.cluster());

        fut = qry.execute();

        res = fut.get();

        assertEquals(gridCount(), res.size());

        row = res.iterator().next();

        assertEquals(1, row.get(0));
        assertEquals(8, row.get(1));

        // Not registered.
        final CacheQuery<List<?>> qry3 = ((IgniteKernal)ignite).cache(null).queries().createSqlFieldsQuery("select no()");

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    qry3.execute().get();

                    return null;
                }
            },
            IgniteCheckedException.class,
            null
        );
    }

    /**
     * Expired entries are not included to result.
     *
     * @throws Exception If failed.
     */
    public void testExpiration() throws Exception {
        ignite.jcache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, 1000))).put("key1", 1);

        GridCache<String, Integer> cache = ((IgniteKernal)ignite).cache(null);

        CacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class, "1=1");

        Map.Entry<String, Integer> res = F.first(qry.execute().get());

        assertEquals(1, res.getValue().intValue());

        U.sleep(1020);

        res = F.first(qry.execute().get());

        assertNull(res);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIllegalBounds() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        cache.put(1, 1);
        cache.put(2, 2);

        CacheQuery<Map.Entry<Integer, Integer>> qry = cache.queries().createSqlQuery(Integer.class,
            "_key between 2 and 1");

        assertTrue(qry.execute().get().isEmpty());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testComplexType() throws Exception {
        GridCache<Key, GridCacheQueryTestValue> cache = ((IgniteKernal)ignite).cache(null);

        GridCacheQueryTestValue val1 = new GridCacheQueryTestValue();

        val1.setField1("field1");
        val1.setField2(1);
        val1.setField3(1L);

        GridCacheQueryTestValue val2 = new GridCacheQueryTestValue();

        val2.setField1("field2");
        val2.setField2(2);
        val2.setField3(2L);
        val2.setField6(null);

        cache.putx(new Key(100500), val1);
        cache.putx(new Key(100501), val2);

        CacheQuery<Map.Entry<Key, GridCacheQueryTestValue>> qry =
            cache.queries().createSqlQuery(GridCacheQueryTestValue.class,
                "fieldName='field1' and field2=1 and field3=1 and id=100500 and embeddedField2=11 and x=3");

        Map.Entry<Key, GridCacheQueryTestValue> entry = F.first(qry.execute().get());

        assertNotNull(entry);
        assertEquals(100500, entry.getKey().id);
        assertEquals(val1, entry.getValue());
    }

    /**
     * Complex key type.
     */
    private static class Key {
        /** */
        @QuerySqlField
        private final long id;

        /**
         * @param id Id.
         */
        private Key(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return id == key.id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(id ^ (id >>> 32));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testSelectQuery() throws Exception {
        GridCache<String, String> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx("key", "value");

        CacheQuery<Map.Entry<String, String>> qry = cache.queries().createSqlQuery(String.class,
            "select * from String");

        CacheQueryFuture<Map.Entry<String, String>> iter = qry.execute();

        assert iter != null;
        assert iter.next() != null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectQuery() throws Exception {
        GridCache<Integer, ObjectValue> cache = ((IgniteKernal)ignite).cache(null);

        ObjectValue val = new ObjectValue("test", 0);

        cache.putx(1, val);

        CacheQuery<Map.Entry<Integer, ObjectValue>> qry =
            cache.queries().createSqlQuery(ObjectValue.class, "_val=?");

        CacheQueryFuture<Map.Entry<Integer, ObjectValue>> iter = qry.execute(val);

        assert iter != null;

        int expCnt = 1;

        for (int i = 0; i < expCnt; i++)
            assert iter.next() != null;

        assert iter.next() == null;

        qry = cache.queries().createFullTextQuery(ObjectValue.class, "test");

        iter = qry.execute();

        assert iter != null;

        for (int i = 0; i < expCnt; i++)
            assert iter.next() != null;

        assert iter.next() == null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTextQueryOnProjection() throws Exception {
        GridCache<Integer, ObjectValue> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx(1, new ObjectValue("test", 1));
        cache.putx(2, new ObjectValue("test", 2));

        CacheQuery<Map.Entry<Integer, ObjectValue>> qry =
            cache.queries().createFullTextQuery(ObjectValue.class, "test");

        CacheQueryFuture<Map.Entry<Integer, ObjectValue>> iter = qry.execute();

        assert iter != null;

        int expCnt = 2;

        for (int i = 0; i < expCnt; i++)
            assert iter.next() != null;

        assert iter.next() == null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectQueryWithSwap() throws Exception {
        GridCache<Integer, ObjectValue> cache = ((IgniteKernal)ignite).cache(null);

        boolean partitioned = cache.configuration().getCacheMode() == PARTITIONED;

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            cache.putx(i, new ObjectValue("test" + i, i));

        for (Ignite g : G.allGrids()) {
            GridCache<Integer, ObjectValue> c = ((IgniteKernal)g).cache(null);

            for (int i = 0; i < cnt; i++) {
                if (i % 2 == 0) {
                    assertNotNull(c.peek(i));

                    c.evict(i); // Swap.

                    if (!partitioned || c.affinity().mapKeyToNode(i).isLocal()) {
                        ObjectValue peekVal = c.peek(i);

                        assertNull("Non-null value for peek [key=" + i + ", val=" + peekVal + ']', peekVal);
                    }
                }
            }
        }

        CacheQuery<Map.Entry<Integer, ObjectValue>> qry =
            cache.queries().createSqlQuery(ObjectValue.class, "intVal >= ? order by intVal");

        qry.enableDedup(true);

        CacheQueryFuture<Map.Entry<Integer, ObjectValue>> iter = qry.execute(0);

        assert iter != null;

        Collection<Integer> set = new HashSet<>(cnt);

        Map.Entry<Integer, ObjectValue> next;

        while ((next = iter.next()) != null) {
            ObjectValue v = next.getValue();

            assert !set.contains(v.intValue());

            set.add(v.intValue());
        }

        assert iter.next() == null;

        assertEquals(cnt, set.size());

        for (int i = 0; i < cnt; i++)
            assert set.contains(i);

        qry = cache.queries().createSqlQuery(ObjectValue.class, "MOD(intVal, 2) = ? order by intVal");

        qry.enableDedup(true);

        iter = qry.execute(0);

        assert iter != null;

        set.clear();

        while ((next = iter.next()) != null) {
            ObjectValue v = next.getValue();

            assert !set.contains(v.intValue());

            set.add(v.intValue());
        }

        assert iter.next() == null;

        assertEquals(cnt / 2, set.size());

        for (int i = 0; i < cnt; i++)
            if (i % 2 == 0)
                assert set.contains(i);
            else
                assert !set.contains(i);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testFullTextSearch() throws Exception {
        GridCache<Integer, ObjectValue> cache = ((IgniteKernal)ignite).cache(null);

        // Try to execute on empty cache first.
        CacheQuery<Map.Entry<Integer, ObjectValue>> qry = cache.queries().createFullTextQuery(ObjectValue.class,
            "full");

        assert qry.execute().get().isEmpty();

        qry = cache.queries().createFullTextQuery(ObjectValue.class, "full");

        assert qry.execute().get().isEmpty();

        // Now put indexed values into cache.
        int key1 = 1;

        ObjectValue val1 = new ObjectValue("test full text", 0);

        cache.putx(key1, val1);

        int key2 = 2;

        ObjectValue val2 = new ObjectValue("test full text more", 0);

        cache.putx(key2, val2);

        qry = cache.queries().createFullTextQuery(ObjectValue.class, "full");

        Collection<Map.Entry<Integer, ObjectValue>> res = qry.execute().get();

        assert res != null;

        assert res.size() == 2;

        qry = cache.queries().createFullTextQuery(ObjectValue.class, "full");

        res = qry.execute().get();

        assert res != null;
        assert res.size() == 2;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testRemoveIndex() throws Exception {
        GridCache<Integer, ObjectValue> cache = ((IgniteKernal)ignite).cache(null);
        GridCache<Integer, ObjectValue> cache1 = ((IgniteKernal)ignite).cache("c1");

        ObjectValue val = new ObjectValue("test full text", 0);

        int key = 1;

        cache.putx(key, val);
        cache1.putx(key, val);

        GridCacheQueryManager<Object, Object> qryMgr = ((IgniteKernal) ignite).internalCache().context().queries();
        GridCacheQueryManager<Object, Object> qryMgr1 = ((IgniteKernal) ignite).internalCache("c1").context().queries();

        assert hasIndexTable(ObjectValue.class, qryMgr);
        assert hasIndexTable(ObjectValue.class, qryMgr1);

        assert qryMgr != null;

        qryMgr.onUndeploy(ObjectValue.class.getClassLoader());

        assert !hasIndexTable(ObjectValue.class, qryMgr);
        assert hasIndexTable(ObjectValue.class, qryMgr1);

        // Put again.
        cache.putx(key, val);

        assert hasIndexTable(ObjectValue.class, qryMgr);
        assert hasIndexTable(ObjectValue.class, qryMgr1);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testScanQuery() throws Exception {
        GridCache<String, String> c1 = ((IgniteKernal)ignite).cache(null);

        c1.putx("key", "value");

        CacheQuery<Map.Entry<String, String>> qry1 = c1.queries().createScanQuery(null);

        CacheQueryFuture<Map.Entry<String, String>> iter = qry1.execute();

        assert iter != null;

        int expCnt = 1;

        for (int i = 0; i < expCnt; i++) {
            Map.Entry<String, String> e1 = iter.next();

            assertEquals("key", e1.getKey());
            assertEquals("value", e1.getValue());
        }

        assert iter.next() == null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTwoObjectsTextSearch() throws Exception {
        GridCache<Object, Object> c = ((IgniteKernal)ignite).cache(null);

        c.put(1, new ObjectValue("ObjectValue str", 1));
        c.put("key", new ObjectValueOther("ObjectValueOther str"));

        Collection<Map.Entry<Object, Object>> res =
            c.queries().createFullTextQuery(ObjectValue.class, "str").execute().get();

        assert res != null;
        int expCnt = 1;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValue.class;

        res = c.queries().createFullTextQuery(ObjectValueOther.class, "str").execute().get();

        assert res != null;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValueOther.class;

        res = c.queries().createFullTextQuery(ObjectValue.class, "str").execute().get();

        assert res != null;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValue.class;

        res = c.queries().createFullTextQuery(ObjectValueOther.class, "str").execute().get();

        assert res != null;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValueOther.class;
    }

    /**
     * JUnit.
     * @throws Exception In case of error.
     */
    public void testTransformQuery() throws Exception {
        GridCache<UUID, Person> c = ((IgniteKernal)ignite).cache(null);

        final Person p1 = new Person("Bob", 100);
        final Person p2 = new Person("Tom", 200);

        c.put(p1.id, p1);
        c.put(p2.id, p2);

        CacheQuery<Map.Entry<UUID, Person>> q = c.queries().createScanQuery(null);

        Collection<Map.Entry<UUID, String>> res = q.execute(new C1<Map.Entry<UUID, Person>, Map.Entry<UUID, String>>() {
            @Override public Map.Entry<UUID, String> apply(Map.Entry<UUID, Person> p) {
                return F.t(p.getKey(), p.getValue().name);
            }
        }).get();

        assert res != null;
        assert res.size() == 2;

        F.forEach(res, new CI1<Map.Entry<UUID, String>>() {
            @Override public void apply(Map.Entry<UUID, String> e) {
                if (p1.id.equals(e.getKey()))
                    assert "Bob".equals(e.getValue());
                else if (p2.id.equals(e.getKey()))
                    assert "Tom".equals(e.getValue());
                else
                    assert false : "Unexpected entry.";
            }
        });

        q = c.queries().createSqlQuery(Person.class, "id = ?");

        res = q.execute(new C1<Map.Entry<UUID, Person>, Map.Entry<UUID, String>>() {
            @Override public Map.Entry<UUID, String> apply(Map.Entry<UUID, Person> p) {
                return F.t(p.getKey(), p.getValue().name);
            }
        }, p1.id).get();

        assert res != null;
        assert res.size() == 1;

        F.forEach(res, new CI1<Map.Entry<UUID, String>>() {
            @Override public void apply(Map.Entry<UUID, String> e) {
                assert p1.id.equals(e.getKey());
                assert "Bob".equals(e.getValue());
            }
        });

        q = c.queries().createFullTextQuery(Person.class, "Bob");

        res = q.execute(new C1<Map.Entry<UUID, Person>, Map.Entry<UUID, String>>() {
            @Override public Map.Entry<UUID, String> apply(Map.Entry<UUID, Person> p) {
                return F.t(p.getKey(), p.getValue().name);
            }
        }).get();

        assertNotNull(res);

        assertEquals(1, res.size());

        F.forEach(res, new CI1<Map.Entry<UUID, String>>() {
            @Override public void apply(Map.Entry<UUID, String> e) {
                assert p1.id.equals(e.getKey());
                assert "Bob".equals(e.getValue());
            }
        });

        q = c.queries().createFullTextQuery(Person.class, "Bob");

        res = q.execute(new C1<Map.Entry<UUID, Person>, Map.Entry<UUID, String>>() {
            @Override public Map.Entry<UUID, String> apply(Map.Entry<UUID, Person> p) {
                return F.t(p.getKey(), p.getValue().name);
            }
        }).get();

        assert res != null;
        assert res.size() == 1;

        F.forEach(res, new CI1<Map.Entry<UUID, String>>() {
            @Override public void apply(Map.Entry<UUID, String> e) {
                assert p1.id.equals(e.getKey());
                assert "Bob".equals(e.getValue());
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceQuery() throws Exception {
        GridCache<String, Integer> c = ((IgniteKernal)ignite).cache(null);

        assert c.putx("key1", 1);
        assert c.putx("key2", 2);
        assert c.putx("key3", 3);
        assert c.putx("key4", 4);
        assert c.putx("key5", 5);

        CacheQuery<Map.Entry<String, Integer>> qry = c.queries().createSqlQuery(Integer.class, "_val > 2");

        Collection<Integer> res1 = qry.execute(new SumRemoteReducer()).get();

        assert res1 != null;
        assert res1.size() == gridCount();
        assert F.sumInt(res1) == (cacheMode() == REPLICATED ? 12 * gridCount() : 12);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptyObject() throws Exception {
        GridCache<EmptyObject, EmptyObject> cache = ((IgniteKernal)ignite).cache(null);

        cache.putx(new EmptyObject(1), new EmptyObject(2));

        for (int i = 0; i < gridCount(); i++) {
            GridCacheQueryManager<Object, Object> qryMgr =
                ((IgniteKernal)grid(i)).internalCache().context().queries();

            assert !hasIndexTable(EmptyObject.class, qryMgr);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveType() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        assert cache.putx(1, 1);
        assert cache.putx(2, 2);

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(int.class, "_val > 1");

        Collection<Map.Entry<Integer, Integer>> res = q.execute().get();

        assertEquals(1, res.size());

        for (Map.Entry<Integer, Integer> e : res) {
            assertEquals(2, (int)e.getKey());
            assertEquals(2, (int)e.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationIteratorDefaultCache() throws Exception {
        testPaginationIterator(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationIteratorNamedCache() throws Exception {
        testPaginationIterator("c1");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void testPaginationIterator(@Nullable String cacheName) throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(cacheName);

        for (int i = 0; i < 50; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class,
            "_key >= 0").projection(ignite.cluster());

        q.pageSize(10);
        q.enableDedup(true);
        q.keepAll(false);

        CacheQueryFuture<Map.Entry<Integer, Integer>> f = q.execute();

        int cnt = 0;

        Map.Entry<Integer, Integer> e;

        while ((e = f.next()) != null) {
            assertTrue(e.getKey() >= 0 && e.getKey() < 50);
            assertTrue(e.getValue() >= 0 && e.getValue() < 50);

            cnt++;
        }

        assertEquals(50, cnt);

        assertTrue(f.isDone());

        if (cacheMode() != LOCAL)
            assertTrue(f.get().size() < 50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationIteratorKeepAll() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 50; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class, "_key >= 0");

        q.pageSize(10);
        q.enableDedup(true);
        q.keepAll(true);

        CacheQueryFuture<Map.Entry<Integer, Integer>> f = q.execute();

        int cnt = 0;

        Map.Entry<Integer, Integer> e;

        while ((e = f.next()) != null) {
            assertTrue(e.getKey() >= 0 && e.getKey() < 50);
            assertTrue(e.getValue() >= 0 && e.getValue() < 50);

            cnt++;
        }

        assertEquals(50, cnt);

        assertTrue(f.isDone());

        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(f.get());

        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override public int compare(Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) {
                return e1.getKey().compareTo(e2.getKey());
            }
        });

        for (int i = 0; i < 50; i++) {
            Map.Entry<Integer, Integer> e0 = list.get(i);

            assertEquals(i, (int)e0.getKey());
            assertEquals(i, (int)e0.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationGetDefaultCache() throws Exception {
        testPaginationGet(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationGetNamedCache() throws Exception {
        testPaginationGet("c1");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void testPaginationGet(@Nullable String cacheName) throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(cacheName);

        for (int i = 0; i < 50; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class, "_key >= 0");

        q.pageSize(10);
        q.enableDedup(true);

        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(q.execute().get());

        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override public int compare(Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) {
                return e1.getKey().compareTo(e2.getKey());
            }
        });

        for (int i = 0; i < 50; i++) {
            Map.Entry<Integer, Integer> e = list.get(i);

            assertEquals(i, (int)e.getKey());
            assertEquals(i, (int)e.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanFilters() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 50; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createScanQuery(
            new P2<Integer, Integer>() {
                @Override public boolean apply(Integer k, Integer v) {
                    assertNotNull(k);
                    assertNotNull(v);

                    return k >= 20 && v < 40;
                }
            });

        q.enableDedup(true);

        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(q.execute().get());

        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override public int compare(Map.Entry<Integer, Integer> e1, Map.Entry<Integer, Integer> e2) {
                return e1.getKey().compareTo(e2.getKey());
            }
        });

        assertEquals(20, list.size());

        for (int i = 20; i < 40; i++) {
            Map.Entry<Integer, Integer> e = list.get(i - 20);

            assertEquals(i, (int)e.getKey());
            assertEquals(i, (int)e.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptyGrid() throws Exception {
        GridCache<String, Integer> cache = ((IgniteKernal)ignite).cache(null);

        String key = "k";

        int val = 2;

        cache.putx(key, val);

        CacheQuery<Map.Entry<String, Integer>> qry = cache.queries().createSqlQuery(Integer.class,
            "select * from Integer where _key = 'k' and _val > 1");

        Map.Entry<String, Integer> entry = F.first(qry.execute().get());

        assert entry != null;

        assertEquals(Integer.valueOf(val), entry.getValue());
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testBadHashObjectKey() throws IgniteCheckedException {
        GridCache<BadHashKeyObject, Integer> cache = ((IgniteKernal)ignite).cache(null);

        cache.put(new BadHashKeyObject("test_key1"), 9);
        cache.put(new BadHashKeyObject("test_key0"), 1005001);
        cache.put(new BadHashKeyObject("test_key1"), 7);

        assertEquals(1005001, cache.queries().createSqlQuery(Integer.class, "_key = ?").execute(new BadHashKeyObject(
            "test_key0")).get().iterator().next().getValue().intValue());
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testTextIndexedKey() throws IgniteCheckedException {
        GridCache<ObjectValue, Integer> cache = ((IgniteKernal)ignite).cache(null);

        cache.put(new ObjectValue("test_key1", 10), 19);
        cache.put(new ObjectValue("test_key0", 11), 11005);
        cache.put(new ObjectValue("test_key1", 12), 17);

        assertEquals(11005,
            cache.queries().createFullTextQuery(Integer.class, "test_key0").execute().get().iterator().next()
                .getValue().intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAnonymousClasses() throws Exception {
        GridCache<Integer, Object> cache = ((IgniteKernal)ignite).cache(null);

        Object val = new Object() {
            @QuerySqlField
            private int field1 = 10;

            @Override public String toString() {
                return "Test anonymous object.";
            }
        };

        assertTrue("Unexpected name: " + val.getClass().getName(),
            val.getClass().getName().endsWith("GridCacheAbstractQuerySelfTest$14"));

        assertTrue(cache.putx(1, val));

        CacheQuery<Map.Entry<Integer, Object>> q = cache.queries().createSqlQuery(val.getClass(), "_key >= 0");

        q.enableDedup(true);

        Collection<Map.Entry<Integer, Object>> res = q.execute().get();

        assertEquals(1, res.size());

        CacheQuery<List<?>> fieldsQry = cache.queries().createSqlFieldsQuery(
            "select field1 from GridCacheAbstractQuerySelfTest_14");

        fieldsQry.enableDedup(true);

        Collection<List<?>> fieldsRes = fieldsQry.execute().get();

        assertEquals(1, fieldsRes.size());

        List<?> fields =  F.first(fieldsRes);

        assertEquals(1, fields.size());
        assertEquals(10, fields.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoAnonymousClasses() throws Exception {
        GridCache<Integer, Object> cache = ((IgniteKernal)ignite).cache(null);

        Object val1 = new Object() {
            @Override public String toString() {
                return "Test anonymous object1.";
            }
        };

        Object val2 = new Object() {
            @Override public String toString() {
                return "Test anonymous object2.";
            }
        };

        assertTrue(cache.putx(1, val1));
        assertTrue(cache.putx(2, val2));

        CacheQuery<Map.Entry<Integer, Object>> q = cache.queries().createSqlQuery(val1.getClass(), "_key >= 0");

        q.enableDedup(true);

        Collection<Map.Entry<Integer, Object>> res = q.execute().get();

        assertEquals(1, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOrderByOnly() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 10; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class, "order by _val");

        q.enableDedup(true);

        Collection<Map.Entry<Integer, Integer>> res = q.execute().get();

        assertEquals(10, res.size());

        if (cacheMode() != PARTITIONED) {
            Iterator<Map.Entry<Integer, Integer>> it = res.iterator();

            for (Integer i = 0; i < 10; i++) {
                assertTrue(it.hasNext());

                Map.Entry<Integer, Integer> e = it.next();

                assertEquals(i, e.getKey());
                assertEquals(i, e.getValue());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLimitOnly() throws Exception {
        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 10; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class, "limit 5");

        q.enableDedup(true);

        Collection<Map.Entry<Integer, Integer>> res = q.execute().get();

        if (cacheMode() != PARTITIONED) {
            assertEquals(5, res.size());

            Iterator<Map.Entry<Integer, Integer>> it = res.iterator();

            for (Integer i = 0; i < 5; i++) {
                assertTrue(it.hasNext());

                Map.Entry<Integer, Integer> e = it.next();

                assertEquals(i, e.getKey());
                assertEquals(i, e.getValue());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testArray() throws Exception {
        GridCache<Integer, ArrayObject> cache = ((IgniteKernal)ignite).cache(null);

        assertTrue(cache.putx(1, new ArrayObject(new Long[] {1L, null, 3L})));
        assertTrue(cache.putx(2, new ArrayObject(new Long[] {4L, 5L, 6L})));

        CacheQuery<Map.Entry<Integer, ArrayObject>> q =
            cache.queries().createSqlQuery(ArrayObject.class, "array_contains(arr, cast(4 as long))");

        q.enableDedup(true);

        Collection<Map.Entry<Integer, ArrayObject>> res = q.execute().get();

        assertEquals(1, res.size());

        Map.Entry<Integer, ArrayObject> e = F.first(res);

        assertEquals(2, (int)e.getKey());
        assertArrayEquals(new Long[] {4L, 5L, 6L}, e.getValue().arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryEvents() throws Exception {
        testSqlQueryEvents(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryEventsSubjectId() throws Exception {
        testSqlQueryEvents(true);
    }

    /**
     * @param customSubjId Use custom subject ID.
     * @throws Exception If failed.
     */
    private void testSqlQueryEvents(final boolean customSubjId) throws Exception {
        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent<Integer, Integer> qe = (CacheQueryReadEvent<Integer, Integer>)evt;

                    assertEquals(SQL, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertEquals("Integer", qe.className());
                    assertEquals("_key >= ?", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] { 10 }, qe.arguments());

                    map.put(qe.key(), qe.value());

                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_OBJECT_READ);

            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(SQL, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertEquals("Integer", qe.className());
                    assertEquals("_key >= ?", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] { 10 }, qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_EXECUTED);
        }

        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 20; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createSqlQuery(Integer.class, "_key >= ?");

        if (customSubjId)
            ((GridCacheQueryAdapter)q).subjectId(subjId);

        q.execute(10).get();

        assert latch.await(1000, MILLISECONDS);
        assert execLatch.await(1000, MILLISECONDS);

        assertEquals(10, map.size());

        for (int i = 10; i < 20; i++)
            assertEquals(i, map.get(i).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryEvents() throws Exception {
        testScanQueryEvents(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryEventsSubjectId() throws Exception {
        testScanQueryEvents(true);
    }

    /**
     * @param customSubjId Use custom subject ID.
     * @throws Exception If failed.
     */
    private void testScanQueryEvents(final boolean customSubjId) throws Exception {
        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent<Integer, Integer> qe = (CacheQueryReadEvent<Integer, Integer>)evt;

                    assertEquals(SCAN, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertNull(qe.className());
                    assertNull(null, qe.clause());
                    assertNotNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    map.put(qe.key(), qe.value());

                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_OBJECT_READ);

            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(SCAN, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertNull(qe.className());
                    assertNull(null, qe.clause());
                    assertNotNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_EXECUTED);
        }

        GridCache<Integer, Integer> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 0; i < 20; i++)
            assertTrue(cache.putx(i, i));

        CacheQuery<Map.Entry<Integer, Integer>> q = cache.queries().createScanQuery(
            new IgniteBiPredicate<Integer, Integer>() {
                @Override public boolean apply(Integer key, Integer val) {
                    return key >= 10;
                }
            });

        if (customSubjId)
            ((GridCacheQueryAdapter)q).subjectId(subjId);

        q.execute().get();

        assert latch.await(1000, MILLISECONDS);
        assert execLatch.await(1000, MILLISECONDS);

        assertEquals(10, map.size());

        for (int i = 10; i < 20; i++)
            assertEquals(i, map.get(i).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTextQueryEvents() throws Exception {
        testTextQueryEvents(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTextQueryEventsSubjectId() throws Exception {
        testTextQueryEvents(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testTextQueryEvents(final boolean customSubjId) throws Exception {
        final Map<Integer, Person> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent<Integer, Person> qe = (CacheQueryReadEvent<Integer, Person>)evt;

                    assertEquals(FULL_TEXT, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertEquals("Person", qe.className());
                    assertEquals("White", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    map.put(qe.key(), qe.value());

                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_OBJECT_READ);

            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(FULL_TEXT, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertEquals("Person", qe.className());
                    assertEquals("White", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_EXECUTED);
        }

        GridCache<Integer, Person> cache = ((IgniteKernal)ignite).cache(null);

        assertTrue(cache.putx(1, new Person("Bob White", 1000)));
        assertTrue(cache.putx(2, new Person("Tom White", 1000)));
        assertTrue(cache.putx(3, new Person("Mike Green", 1000)));

        CacheQuery<Map.Entry<Integer, Person>> q = cache.queries().createFullTextQuery(Person.class, "White");

        if (customSubjId)
            ((GridCacheQueryAdapter)q).subjectId(subjId);

        q.execute().get();

        assert latch.await(1000, MILLISECONDS);
        assert execLatch.await(1000, MILLISECONDS);

        assertEquals(2, map.size());

        assertEquals("Bob White", map.get(1).name());
        assertEquals("Tom White", map.get(2).name());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryEvents() throws Exception {
        testFieldsQueryEvents(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryEventsSubjectId() throws Exception {
        testFieldsQueryEvents(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testFieldsQueryEvents(final boolean customSubjId) throws Exception {
        final Map<Integer, String> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent qe = (CacheQueryReadEvent)evt;

                    assertEquals(SQL_FIELDS, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertNull(qe.className());
                    assertEquals("select _key, name from Person where salary > ?", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] { 10 }, qe.arguments());

                    List<?> row = (List<?>)qe.row();

                    map.put((Integer)row.get(0), (String)row.get(1));

                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_OBJECT_READ);

            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(SQL_FIELDS, qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals(customSubjId ? subjId : grid(0).localNode().id(), qe.subjectId());

                    assertNull(qe.className());
                    assertEquals("select _key, name from Person where salary > ?", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] { 10 }, qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            }, EVT_CACHE_QUERY_EXECUTED);
        }

        GridCache<Integer, Person> cache = ((IgniteKernal)ignite).cache(null);

        for (int i = 1; i <= 20; i++)
            assertTrue(cache.putx(i, new Person("Person " + i, i)));

        CacheQuery<List<?>> q = cache.queries().createSqlFieldsQuery(
            "select _key, name from Person where salary > ?");

        if (customSubjId)
            ((GridCacheQueryAdapter)q).subjectId(subjId);

        q.execute(10).get();

        assert latch.await(1000, MILLISECONDS);
        assert execLatch.await(1000, MILLISECONDS);

        assertEquals(10, map.size());

        for (int i = 11; i <= 20; i++)
            assertEquals("Person " + i, map.get(i));
    }

    /**
     * @param cls Class to check index table for.
     * @param qryMgr Query manager.
     * @return {@code true} if index has a table for given class.
     * @throws IgniteCheckedException If failed.
     */
    private boolean hasIndexTable(Class<?> cls, GridCacheQueryManager<Object, Object> qryMgr) throws IgniteCheckedException {
        return qryMgr.size(cls) != -1;
    }

    /**
     *
     */
    private static class ArrayObject implements Serializable {
        /** */
        @QuerySqlField
        private Long[] arr;

        /**
         * @param arr Array.
         */
        private ArrayObject(Long[] arr) {
            this.arr = arr;
        }
    }

    /**
     *
     */
    public static class Person implements Externalizable {
        /** */
        @GridToStringExclude
        @QuerySqlField
        private UUID id = UUID.randomUUID();

        /** */
        @QuerySqlField
        @QueryTextField
        private String name;

        /** */
        @QuerySqlField
        private int salary;

        /** */
        @QuerySqlField(index = true)
        private int fake$Field;

        /**
         * Required by {@link Externalizable}.
         */
        public Person() {
            // No-op.
        }

        /**
         * @param name Name.
         * @param salary Salary.
         */
        public Person(String name, int salary) {
            assert name != null;
            assert salary > 0;

            this.name = name;
            this.salary = salary;
        }

        /**
         * @return Id.
         */
        public UUID id() {
            return id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return Salary.
         */
        public double salary() {
            return salary;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, id);
            U.writeString(out, name);
            out.writeInt(salary);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = U.readUuid(in);
            name = U.readString(in);
            salary = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode() + 31 * name.hashCode() + 31 * 31 * salary;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (!(obj instanceof Person))
                return false;

            Person that = (Person)obj;

            return that.id.equals(id) && that.name.equals(name) && that.salary == salary;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }

    /**
     * Test value object.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class ObjectValue implements Serializable {
        /** String value. */
        @QueryTextField
        private String strVal;

        /** Integer value. */
        @QuerySqlField
        private int intVal;

        /**
         * Constructor.
         *
         * @param strVal String value.
         * @param intVal Integer value.
         */
        ObjectValue(String strVal, int intVal) {
            this.strVal = strVal;
            this.intVal = intVal;
        }

        /**
         * Gets value.
         *
         * @return Value.
         */
        public String getStringValue() {
            return strVal;
        }

        /**
         * @return Integer value.
         */
        public int intValue() {
            return intVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValue other = (ObjectValue)o;

            return strVal == null ? other.strVal == null : strVal.equals(other.strVal);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return strVal != null ? strVal.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectValue.class, this);
        }
    }

    /**
     * Another test value object.
     */
    private static class ObjectValueOther {
        /** Value. */
        @QueryTextField
        private String val;

        /**
         * @param val String value.
         */
        ObjectValueOther(String val) {
            this.val = val;
        }

        /**
         * Gets value.
         *
         * @return Value.
         */
        public String value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValueOther other = (ObjectValueOther)o;

            return val == null ? other.val == null : val.equals(other.val);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectValueOther.class, this);
        }
    }

    /**
     * Empty test object.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class EmptyObject {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private EmptyObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof EmptyObject))
                return false;

            EmptyObject that = (EmptyObject)o;

            return val == that.val;
        }
    }

    /**
     *
     */
    private static class BadHashKeyObject implements Serializable {
        /** */
        @QuerySqlField(index = false)
        private final String str;

        /**
         * @param str String.
         */
        private BadHashKeyObject(String str) {
            this.str = str;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BadHashKeyObject keyObj = (BadHashKeyObject) o;

            return str.equals(keyObj.str);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 10;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BadHashKeyObject.class, this);
        }
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private Map<Object, Object> map = new ConcurrentHashMap<>();

        /** */
        void reset() {
            map.clear();
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }

    /**
     * Functions for test.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class SqlFunctions {
        /**
         * @param x Argument.
         * @return Square of given value.
         */
        @QuerySqlFunction
        public static int square(int x) {
            return x * x;
        }

        /**
         * @param x Argument.
         * @return Cube of given value.
         */
        @QuerySqlFunction(alias = "_cube_")
        public static int cube(int x) {
            return x * x * x;
        }

        /**
         * Method which should not be registered.
         * @return Nothing.
         */
        public static int no() {
            throw new IllegalStateException();
        }
    }

    /**
     * Sum remote reducer factory.
     */
    private static class SumRemoteReducer implements IgniteReducer<Map.Entry<String, Integer>, Integer> {
        /** */
        private int sum;

        @Override public boolean collect(Map.Entry<String, Integer> e) {
            sum += e.getValue();

            return true;
        }

        @Override public Integer reduce() {
            return sum;
        }
    }

    /**
     * Sum local reducer factory.
     */
    private static class SumLocalReducer implements IgniteReducer<Integer, Integer> {
        /** */
        private int sum;

        @Override public boolean collect(Integer e) {
            sum += e;

            return true;
        }

        @Override public Integer reduce() {
            return sum;
        }
    }
}
