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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.FULL_TEXT;
import static org.apache.ignite.internal.processors.cache.query.CacheQueryType.SCAN;
import static org.junit.Assert.assertArrayEquals;

/**
 * Various tests for cache queries.
 */
public abstract class IgniteCacheAbstractQuerySelfTest extends GridCommonAbstractTest {
    /** Key count. */
    private static final int KEY_CNT = 5000;

    /** Cache store. */
    private static TestStore store = new TestStore();

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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
    protected NearCacheConfiguration nearCacheConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setDiscoverySpi(new TcpDiscoverySpi().setForceServerMode(true).setIpFinder(ipFinder));

        // Otherwise noop swap space will be chosen on Windows.
        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        if (!gridName.startsWith("client")) {
            CacheConfiguration[] ccs = new CacheConfiguration[2];

            for (int i = 0; i < ccs.length; i++) {
                CacheConfiguration cc = defaultCacheConfiguration();

                if (i > 0)
                    cc.setName("c" + i);

                cc.setCacheMode(cacheMode());
                cc.setAtomicityMode(atomicityMode());
                cc.setNearConfiguration(nearCacheConfiguration());
                cc.setWriteSynchronizationMode(FULL_SYNC);
                cc.setCacheStoreFactory(new StoreFactory());
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
                cc.setLoadPreviousValue(true);
                cc.setRebalanceMode(SYNC);
                cc.setSwapEnabled(true);
                cc.setSqlFunctionClasses(SqlFunctions.class);
                cc.setIndexedTypes(
                    BadHashKeyObject.class, Byte.class,
                    ObjectValue.class, Long.class,
                    Integer.class, Integer.class,
                    Integer.class, String.class,
                    Integer.class, ObjectValue.class,
                    String.class, ObjectValueOther.class,
                    Integer.class, ArrayObject.class,
                    Key.class, GridCacheQueryTestValue.class,
                    UUID.class, Person.class,
                    IgniteCacheReplicatedQuerySelfTest.CacheKey.class, IgniteCacheReplicatedQuerySelfTest.CacheValue.class,
                    Long.class, EnumObject.class
                );

                List<QueryEntity> entityList = new ArrayList<>();

                QueryEntity qryEntity = new QueryEntity();

                qryEntity.setKeyType(Integer.class.getName());
                qryEntity.setValueType(Type1.class.getName());
                qryEntity.addQueryField("id", Integer.class.getName(), null);
                qryEntity.addQueryField("name", String.class.getName(), null);
                qryEntity.setTableName("Type2");
                qryEntity.setIndexes(Arrays.asList(new QueryIndex("id")));

                entityList.add(qryEntity);

                qryEntity = new QueryEntity();

                qryEntity.setKeyType(Integer.class.getName());
                qryEntity.setValueType(Type2.class.getName());
                qryEntity.addQueryField("id", Integer.class.getName(), null);
                qryEntity.addQueryField("name", String.class.getName(), null);
                qryEntity.setTableName("Type1");
                qryEntity.setIndexes(Arrays.asList(new QueryIndex("id")));

                entityList.add(qryEntity);

                cc.setQueryEntities(entityList);

                if (cacheMode() != CacheMode.LOCAL)
                    cc.setAffinity(new RendezvousAffinityFunction());

                // Explicitly set number of backups equal to number of grids.
                if (cacheMode() == CacheMode.PARTITIONED)
                    cc.setBackups(gridCount());

                cc.setSnapshotableIndex(snapshotableIndex());

                ccs[i] = cc;
            }

            c.setCacheConfiguration(ccs);
        }
        else
            c.setClientMode(true);

        return c;
    }

    /**
     * @return {@code True} if index snapshot is enabled.
     */
    protected boolean snapshotableIndex() {
        return false;
    }

    /**
     * @return Ignite instance.
     */
    protected Ignite ignite() {
        return grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite().cache(null).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        store.reset();
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testDifferentKeyTypes() throws Exception {
        final IgniteCache<Object, Object> cache = ignite().cache(null);

        cache.put(1, "value");

        try {
            cache.put("key", "value");

            fail();
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testDifferentValueTypes() throws Exception {
        IgniteCache<Integer, Object> cache = ignite().cache(null);

        cache.put(7, "value");

        // Put value of different type but for the same key type.
        // Operation should succeed but with warning log message.
        cache.put(7, 1);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testStringType() throws Exception {
        IgniteCache<Integer, String> cache = ignite().cache(null);

        cache.put(666, "test");

        QueryCursor<Cache.Entry<Integer, String>> qry =
            cache.query(new SqlQuery<Integer, String>(String.class, "_val='test'"));

        Cache.Entry<Integer, String> entry = F.first(qry.getAll());

        assert entry != null;
        assertEquals(666, entry.getKey().intValue());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testIntegerType() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        int key = 898;

        int val = 2;

        cache.put(key, val);

        QueryCursor<Cache.Entry<Integer, Integer>> qry =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key = ? and _val > 1").setArgs(key));

        Cache.Entry<Integer, Integer> entry = F.first(qry.getAll());

        assert entry != null;

        assertEquals(key, entry.getKey().intValue());
        assertEquals(val, entry.getValue().intValue());
    }

    /**
     * Test table alias in SqlQuery.
     *
     * @throws Exception In case of error.
     */
    public void testTableAliasInSqlQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        int key = 898;

        int val = 2;

        cache.put(key, val);

        SqlQuery<Integer, Integer> sqlQry = new SqlQuery<>(Integer.class, "t1._key = ? and t1._val > 1");

        QueryCursor<Cache.Entry<Integer, Integer>> qry = cache.query(sqlQry.setAlias("t1").setArgs(key));

        Cache.Entry<Integer, Integer> entry = F.first(qry.getAll());

        assert entry != null;

        assertEquals(key, entry.getKey().intValue());
        assertEquals(val, entry.getValue().intValue());

        sqlQry = new SqlQuery<>(Integer.class, "FROM Integer as t1 WHERE t1._key = ? and t1._val > 1");

        qry = cache.query(sqlQry.setAlias("t1").setArgs(key));

        entry = F.first(qry.getAll());

        assert entry != null;

        assertEquals(key, entry.getKey().intValue());
        assertEquals(val, entry.getValue().intValue());
    }

    /**
     * Tests UDFs.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testUserDefinedFunction() throws IgniteCheckedException {
        // Without alias.
        final IgniteCache<Object, Object> cache = ignite().cache(null);

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select square(1), square(2)"));

        Collection<List<?>> res = qry.getAll();

        if (cacheMode() == REPLICATED)
            assertEquals(1, res.size());
        else
            assertEquals(gridCount(), res.size());

        List<?> row = res.iterator().next();

        assertEquals(1, row.get(0));
        assertEquals(4, row.get(1));

        // With alias.
        qry = cache.query(new SqlFieldsQuery("select _cube_(1), _cube_(2)"));

        res = qry.getAll();

        if (cacheMode() == REPLICATED)
            assertEquals(1, res.size());
        else
            assertEquals(gridCount(), res.size());

        row = res.iterator().next();

        assertEquals(1, row.get(0));
        assertEquals(8, row.get(1));

        // Not registered.
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    cache.query(new SqlFieldsQuery("select no()"));

                    return null;
                }
            },
            CacheException.class,
            null
        );
    }

    /**
     * Expired entries are not included to result.
     *
     * @throws Exception If failed.
     */
    public void testExpiration() throws Exception {
        ignite().cache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, 1000))).put(7, 1);

        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        List<Cache.Entry<Integer, Integer>> qry =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll();

        Cache.Entry<Integer, Integer> res = F.first(qry);

        assertEquals(1, res.getValue().intValue());

        U.sleep(800); // Less than minimal amount of time that must pass before a cache entry is considered expired.

        qry =  cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll();

        res = F.first(qry);

        assertEquals(1, res.getValue().intValue());

        U.sleep(1200); // No expiry guarantee here. Test should be refactored in case of fails.

        qry = cache.query(new SqlQuery<Integer, Integer>(Integer.class, "1=1")).getAll();

        res = F.first(qry);

        assertNull(res);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIllegalBounds() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        cache.put(1, 1);
        cache.put(2, 2);

        QueryCursor<Cache.Entry<Integer,Integer>> qry =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key between 2 and 1"));

        assertTrue(qry.getAll().isEmpty());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testComplexType() throws Exception {
        IgniteCache<Key, GridCacheQueryTestValue> cache = ignite().cache(null);

        GridCacheQueryTestValue val1 = new GridCacheQueryTestValue();

        val1.setField1("field1");
        val1.setField2(1);
        val1.setField3(1L);

        GridCacheQueryTestValue val2 = new GridCacheQueryTestValue();

        val2.setField1("field2");
        val2.setField2(2);
        val2.setField3(2L);
        val2.setField6(null);

        cache.put(new Key(100500), val1);
        cache.put(new Key(100501), val2);

        QueryCursor<Cache.Entry<Key, GridCacheQueryTestValue>> qry = cache
            .query(new SqlQuery<Key, GridCacheQueryTestValue>(GridCacheQueryTestValue.class,
                    "fieldName='field1' and field2=1 and field3=1 and id=100500 and embeddedField2=11 and x=3"));

        Cache.Entry<Key, GridCacheQueryTestValue> entry = F.first(qry.getAll());

        assertNotNull(entry);
        assertEquals(100500, entry.getKey().id);
        assertEquals(val1, entry.getValue());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testComplexTypeKeepBinary() throws Exception {
        if (ignite().configuration().getMarshaller() == null || ignite().configuration().getMarshaller() instanceof BinaryMarshaller) {
            IgniteCache<Key, GridCacheQueryTestValue> cache = ignite().cache(null);

            GridCacheQueryTestValue val1 = new GridCacheQueryTestValue();

            val1.setField1("field1");
            val1.setField2(1);
            val1.setField3(1L);

            GridCacheQueryTestValue val2 = new GridCacheQueryTestValue();

            val2.setField1("field2");
            val2.setField2(2);
            val2.setField3(2L);
            val2.setField6(null);

            cache.put(new Key(100500), val1);
            cache.put(new Key(100501), val2);

            QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> qry = cache.withKeepBinary()
                .query(new SqlQuery<BinaryObject, BinaryObject>(GridCacheQueryTestValue.class,
                    "fieldName='field1' and field2=1 and field3=1 and id=100500 and embeddedField2=11 and x=3"));

            Cache.Entry<BinaryObject, BinaryObject> entry = F.first(qry.getAll());

            assertNotNull(entry);
            assertEquals(Long.valueOf(100500L), entry.getKey().field("id"));
            assertEquals(val1, entry.getValue().deserialize());
        }
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
        IgniteCache<Integer, String> cache = ignite().cache(null);

        cache.put(10, "value");

        QueryCursor<Cache.Entry<Integer, String>> qry =
            cache.query(new SqlQuery<Integer, String>(String.class, "true"));

        Iterator<Cache.Entry<Integer, String>> iter = qry.iterator();

        assert iter != null;
        assert iter.next() != null;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testSimpleCustomTableName() throws Exception {
        final IgniteCache<Integer, Object> cache = ignite().cache(null);

        cache.put(10, new Type1(1, "Type1 record #1"));
        cache.put(20, new Type1(2, "Type1 record #2"));

        QueryCursor<Cache.Entry<Integer, Type1>> qry1 =
            cache.query(new SqlQuery<Integer, Type1>(Type1.class, "FROM Type2"));

        List<Cache.Entry<Integer, Type1>> all = qry1.getAll();

        assertEquals(2, all.size());

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("SELECT name FROM Type2"));

        assertEquals(2, qry.getAll().size());

        GridTestUtils.assertThrows(log, new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                QueryCursor<Cache.Entry<Integer, Type1>> qry =
                    cache.query(new SqlQuery<Integer, Type1>(Type1.class, "FROM Type1"));

                qry.getAll();

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testMixedCustomTableName() throws Exception {
        final IgniteCache<Integer, Object> cache = ignite().cache(null);

        cache.put(10, new Type1(1, "Type1 record #1"));
        cache.put(20, new Type1(2, "Type1 record #2"));
        cache.put(30, new Type2(1, "Type2 record #1"));
        cache.put(40, new Type2(2, "Type2 record #2"));
        cache.put(50, new Type2(3, "Type2 record #3"));

        QueryCursor<Cache.Entry<Integer, Type1>> qry1 =
            cache.query(new SqlQuery<Integer, Type1>(Type1.class, "FROM Type2"));

        List<Cache.Entry<Integer, Type1>> all = qry1.getAll();

        assertEquals(2, all.size());

        QueryCursor<Cache.Entry<Integer, Type2>> qry2 =
            cache.query(new SqlQuery<Integer, Type2>(Type2.class, "FROM Type1"));

        assertEquals(3, qry2.getAll().size());

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("SELECT name FROM Type1"));

        assertEquals(3, qry.getAll().size());

        qry = cache.query(new SqlFieldsQuery("SELECT name FROM Type2"));

        assertEquals(2, qry.getAll().size());

        GridTestUtils.assertThrows(log, new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                QueryCursor<Cache.Entry<Integer, Type1>> qry1 =
                    cache.query(new SqlQuery<Integer, Type1>(Type1.class, "FROM Type1"));

                qry1.getAll().size();

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testDistributedJoinCustomTableName() throws Exception {
        IgniteCache<Integer, Object> cache = ignite().cache(null);

        cache.put(10, new Type1(1, "Type1 record #1"));
        cache.put(20, new Type1(2, "Type1 record #2"));
        cache.put(30, new Type2(1, "Type2 record #1"));
        cache.put(40, new Type2(2, "Type2 record #2"));
        cache.put(50, new Type2(3, "Type2 record #3"));

        QueryCursor<List<?>> query = cache.query(
            new SqlFieldsQuery("SELECT t2.name, t1.name FROM Type2 as t2 LEFT JOIN Type1 as t1 ON t1.id = t2.id")
                .setDistributedJoins(cacheMode() == PARTITIONED));

        assertEquals(2, query.getAll().size());

        query = cache.query(
            new SqlFieldsQuery("SELECT t2.name, t1.name FROM Type2 as t2 RIGHT JOIN Type1 as t1 ON t1.id = t2.id")
                .setDistributedJoins(cacheMode() == PARTITIONED));

        assertEquals(3, query.getAll().size());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectQuery() throws Exception {
        IgniteCache<Integer, ObjectValue> cache = ignite().cache(null);

        ObjectValue val = new ObjectValue("test", 0);

        cache.put(1, val);

        QueryCursor<Cache.Entry<Integer, ObjectValue>> qry =
            cache.query(new SqlQuery<Integer, ObjectValue>(ObjectValue.class, "_val=?").setArgs(val));

        Iterator<Cache.Entry<Integer, ObjectValue>> iter = qry.iterator();

        assert iter != null;

        int expCnt = 1;

        for (int i = 0; i < expCnt; i++)
            assert iter.next() != null;

        assert !iter.hasNext();

        qry = cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "test"));

        iter = qry.iterator();

        assert iter != null;

        for (int i = 0; i < expCnt; i++)
            assert iter.next() != null;

        assert !iter.hasNext();
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testEnumObjectQuery() throws Exception {
        final IgniteCache<Long, EnumObject> cache = ignite().cache(null);

        for (long i = 0; i < 50; i++)
            cache.put(i, new EnumObject(i, i % 2 == 0 ? EnumType.TYPE_A : EnumType.TYPE_B));


        assertEnumQry("type = ?", EnumType.TYPE_A, EnumType.TYPE_A, cache, 25);
        assertEnumQry("type > ?", EnumType.TYPE_A, EnumType.TYPE_B, cache, 25);
        assertEnumQry("type < ?", EnumType.TYPE_B, EnumType.TYPE_A, cache, 25);
        assertEnumQry("type != ?", EnumType.TYPE_B, EnumType.TYPE_A, cache, 25);

        assertEmptyEnumQry("type = ?", null, cache);
        assertEmptyEnumQry("type > ?", EnumType.TYPE_B, cache);
        assertEmptyEnumQry("type < ?", EnumType.TYPE_A, cache);

        cache.put(50L, new EnumObject(50, null));

        assertNoArgEnumQry("type is null", null, cache, 1);
        assertAnyResTypeEnumQry("type is not null", cache, 50);

        // Additional tests for binary enums.
        IgniteBinary binary = ignite().binary();

        if (binary != null) {
            assertEnumQry("type = ?", binaryEnum(binary, EnumType.TYPE_A), EnumType.TYPE_A, cache, 25);
            assertEnumQry("type > ?", binaryEnum(binary, EnumType.TYPE_A), EnumType.TYPE_B, cache, 25);
            assertEnumQry("type < ?", binaryEnum(binary, EnumType.TYPE_B), EnumType.TYPE_A, cache, 25);
            assertEnumQry("type != ?", binaryEnum(binary, EnumType.TYPE_B), EnumType.TYPE_A, cache, 25);

            assertEmptyEnumQry("type > ?", binaryEnum(binary, EnumType.TYPE_B), cache);
            assertEmptyEnumQry("type < ?", binaryEnum(binary, EnumType.TYPE_A), cache);
        }
    }

    /**
     * Create binary enum.
     *
     * @param binary Binary facade.
     * @param val Enum value.
     * @return Binary enum.
     */
    private static BinaryObject binaryEnum(IgniteBinary binary, EnumType val) {
        return binary.buildEnum(EnumType.class.getName(), val.ordinal());
    }

    /**
     * Fails if result size not equals to resSize.
     *
     * @param qryStr to execute.
     * @param cache cache.
     * @param resSize size of the result.
     */
    private void assertAnyResTypeEnumQry(String qryStr, IgniteCache<Long, EnumObject> cache, int resSize) {
        final SqlQuery<Long, EnumObject> qry = new SqlQuery<>(EnumObject.class, qryStr);

        final List<Cache.Entry<Long, EnumObject>> res = cache.query(qry).getAll();

        assert resSize == res.size();
    }

    /**
     * Fails if result size not equals to resSize or
     * at least one entry has different of resType type.
     *
     * @param qryStr to execute.
     * @param resType to compare with.
     * @param cache cache.
     * @param resSize size of the result.
     */
    private void assertNoArgEnumQry(String qryStr, EnumType resType, IgniteCache<Long, EnumObject> cache, int resSize) {
        final SqlQuery<Long, EnumObject> qry = new SqlQuery<>(EnumObject.class, qryStr);

        final List<Cache.Entry<Long, EnumObject>> res = cache.query(qry).getAll();

        assert resSize == res.size();

        assertEnumType(res, resType);
    }

    /**
     * Fails if result size not equals to resSize or
     * at least one entry has different of resType type.
     *
     * @param qryStr to execute.
     * @param arg to be passed to query.
     * @param resType to compare with.
     * @param cache cache.
     * @param resSize size of the result.
     */
    private void assertEnumQry(String qryStr, Object arg, EnumType resType, IgniteCache<Long, EnumObject> cache,
        int resSize) {
        final SqlQuery<Long, EnumObject> qry = new SqlQuery<>(EnumObject.class, qryStr);

        qry.setArgs(arg);

        final List<Cache.Entry<Long, EnumObject>> res = cache.query(qry).getAll();

        assert resSize == res.size();

        assertEnumType(res, resType);
    }

    /**
     * Fails if result has entries.
     *
     * @param qryStr to execute.
     * @param arg argument that will be passed to query.
     * @param cache cache on which query will be executed.
     */
    private void assertEmptyEnumQry(String qryStr, Object arg, IgniteCache<Long, EnumObject> cache) {
        final SqlQuery<Long, EnumObject> qry = new SqlQuery<>(EnumObject.class, qryStr);

        qry.setArgs(arg);

        final List<Cache.Entry<Long, EnumObject>> res = cache.query(qry).getAll();

        assert res.isEmpty();
    }

    /**
     * Fails if at least one object in result has type field that doesn't
     * equal to passed enumType.
     *
     * @param enumObjects query execution result.
     * @param enumType compare to.
     */
    private void assertEnumType(final List<Cache.Entry<Long, EnumObject>> enumObjects, final EnumType enumType) {
        for (final Cache.Entry<Long, EnumObject> entry : enumObjects)
            assert entry.getValue().type == enumType;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectQueryWithSwap() throws Exception {
        IgniteCache<Integer, ObjectValue> cache = ignite().cache(null);

        boolean partitioned = cache.getConfiguration(CacheConfiguration.class).getCacheMode() == PARTITIONED;

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            cache.put(i, new ObjectValue("test" + i, i));

        for (Ignite g : G.allGrids()) {
            IgniteCache<Integer, ObjectValue> c = g.cache(null);

            for (int i = 0; i < cnt; i++) {
                if (i % 2 == 0) {
                    assertNotNull(c.localPeek(i, CachePeekMode.ONHEAP));

                    c.localEvict(Collections.singleton(i)); // Swap.

                    if (!partitioned || g.affinity(null).mapKeyToNode(i).isLocal()) {
                        ObjectValue peekVal = c.localPeek(i, CachePeekMode.ONHEAP);

                        assertNull("Non-null value for peek [key=" + i + ", val=" + peekVal + ']', peekVal);
                    }
                }
            }
        }


        QueryCursor<Cache.Entry<Integer, ObjectValue>> qry =
            cache.query(new SqlQuery<Integer, ObjectValue>(ObjectValue.class, "intVal >= ? order by intVal").
                setArgs(0));

        Iterator<Cache.Entry<Integer, ObjectValue>> iter = qry.iterator();

        assert iter != null;

        Collection<Integer> set = new HashSet<>(cnt);

        Cache.Entry<Integer, ObjectValue> next;

        while (iter.hasNext()) {
            next = iter.next();

            ObjectValue v = next.getValue();

            assert !set.contains(v.intValue());

            set.add(v.intValue());
        }

        assert !iter.hasNext();

        assertEquals(cnt, set.size());

        for (int i = 0; i < cnt; i++)
            assert set.contains(i);

        qry = cache.query(new SqlQuery<Integer, ObjectValue>(ObjectValue.class, "MOD(intVal, 2) = ? order by intVal").
            setArgs(0));

        iter = qry.iterator();

        assert iter != null;

        set.clear();

        while (iter.hasNext()) {
            next = iter.next();

            ObjectValue v = next.getValue();

            assert !set.contains(v.intValue());

            set.add(v.intValue());
        }

        assert !iter.hasNext();

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
        IgniteCache<Integer, ObjectValue> cache = ignite().cache(null);

        // Try to execute on empty cache first.
        QueryCursor<Cache.Entry<Integer, ObjectValue>> qry =
            cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "full"));

        assert qry.getAll().isEmpty();

        qry = cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "full"));

        assert qry.getAll().isEmpty();

        // Now put indexed values into cache.
        int key1 = 1;

        ObjectValue val1 = new ObjectValue("test full text", 0);

        cache.put(key1, val1);

        int key2 = 2;

        ObjectValue val2 = new ObjectValue("test full text more", 0);

        cache.put(key2, val2);

        qry = cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "full"));

        Collection<Cache.Entry<Integer, ObjectValue>> res = qry.getAll();

        assert res != null;

        assert res.size() == 2;

        qry = cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "full"));

        res = qry.getAll();

        assert res != null;
        assert res.size() == 2;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testScanQuery() throws Exception {
        IgniteCache<Integer, String> c1 = ignite().cache(null);

        Map<Integer, String> map = new HashMap(){{
            for (int i = 0; i < 5000; i++)
                put(i, "str" + i);
        }};

        for (Map.Entry<Integer, String> e : map.entrySet())
            c1.put(e.getKey(), e.getValue());

        // Scan query.
        QueryCursor<Cache.Entry<Integer, String>> qry = c1.query(new ScanQuery<Integer, String>());

        Iterator<Cache.Entry<Integer, String>> iter = qry.iterator();

        assert iter != null;

        int cnt = 0;

        while (iter.hasNext()) {
            Cache.Entry<Integer, String> e = iter.next();

            String expVal = map.get(e.getKey());

            assertNotNull(expVal);

            assertEquals(expVal, e.getValue());

            cnt++;
        }

        assertEquals(map.size(), cnt);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testScanPartitionQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        GridCacheContext cctx = ((IgniteCacheProxy)cache).context();

        Map<Integer, Map<Integer, Integer>> entries = new HashMap<>();

        for (int i = 0; i < KEY_CNT; i++) {
            cache.put(i, i);

            int part = cctx.affinity().partition(i);

            Map<Integer, Integer> partEntries = entries.get(part);

            if (partEntries == null)
                entries.put(part, partEntries = new HashMap<>());

            partEntries.put(i, i);
        }

        for (int i = 0; i < cctx.affinity().partitions(); i++) {
            ScanQuery<Integer, Integer> scan = new ScanQuery<>(i);

            Collection<Cache.Entry<Integer, Integer>> actual = cache.query(scan).getAll();

            Map<Integer, Integer> exp = entries.get(i);

            int size = exp == null ? 0 : exp.size();

            assertEquals("Failed for partition: " + i, size, actual.size());

            if (exp == null)
                assertTrue(actual.isEmpty());
            else
                for (Cache.Entry<Integer, Integer> entry : actual)
                    assertTrue(entry.getValue().equals(exp.get(entry.getKey())));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testTwoObjectsTextSearch() throws Exception {
        IgniteCache<Object, Object> c = ignite().cache(null);

        c.put(1, new ObjectValue("ObjectValue str", 1));
        c.put("key", new ObjectValueOther("ObjectValueOther str"));

        Collection<Cache.Entry<Object, Object>> res = c.query(new TextQuery<>(ObjectValue.class, "str")).getAll();

        assert res != null;
        int expCnt = 1;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValue.class;

        res = c.query(new TextQuery<>(ObjectValueOther.class, "str")).getAll();

        assert res != null;
        assert res.size() == expCnt;
        assert F.first(res).getValue().getClass() == ObjectValueOther.class;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveType() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        cache.put(1, 1);
        cache.put(2, 2);

        QueryCursor<Cache.Entry<Integer, Integer>> q =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_val > 1"));

        Collection<Cache.Entry<Integer, Integer>> res = q.getAll();

        assertEquals(1, res.size());

        for (Cache.Entry<Integer, Integer> e : res) {
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
        IgniteCache<Integer, Integer> cache = ignite().cache(cacheName);

        for (int i = 0; i < 50; i++)
            cache.put(i, i);

        SqlQuery<Integer, Integer> qry = new SqlQuery<>(Integer.class, "_key >= 0");

        qry.setPageSize(10);

        QueryCursor<Cache.Entry<Integer, Integer>> q = cache.query(qry);

        int cnt = 0;

        for (Cache.Entry<Integer, Integer> e : q) {
            assertTrue(e.getKey() >= 0 && e.getKey() < 50);
            assertTrue(e.getValue() >= 0 && e.getValue() < 50);

            cnt++;
        }

        assertEquals(50, cnt);
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
        IgniteCache<Integer, Integer> cache = ignite().cache(cacheName);

        for (int i = 0; i < 50; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> q =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 0"));

        List<Cache.Entry<Integer, Integer>> list = new ArrayList<>(q.getAll());

        Collections.sort(list, new Comparator<Cache.Entry<Integer, Integer>>() {
            @Override public int compare(Cache.Entry<Integer, Integer> e1, Cache.Entry<Integer, Integer> e2) {
                return e1.getKey().compareTo(e2.getKey());
            }
        });

        for (int i = 0; i < 50; i++) {
            Cache.Entry<Integer, Integer> e = list.get(i);

            assertEquals(i, (int)e.getKey());
            assertEquals(i, (int)e.getValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanFilters() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        for (int i = 0; i < 50; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> q = cache.query(new ScanQuery<>(new IgniteBiPredicate<Integer,Integer>() {
            @Override public boolean apply(Integer k, Integer v) {
                assertNotNull(k);
                assertNotNull(v);

                return k >= 20 && v < 40;
            }
        }));

        List<Cache.Entry<Integer, Integer>> list = new ArrayList<>(q.getAll());

        Collections.sort(list, new Comparator<Cache.Entry<Integer, Integer>>() {
            @Override public int compare(Cache.Entry<Integer, Integer> e1, Cache.Entry<Integer, Integer> e2) {
                return e1.getKey().compareTo(e2.getKey());
            }
        });

        assertEquals(20, list.size());

        for (int i = 20; i < 40; i++) {
            Cache.Entry<Integer, Integer> e = list.get(i - 20);

            assertEquals(i, (int)e.getKey());
            assertEquals(i, (int)e.getValue());
        }
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testBadHashObjectKey() throws IgniteCheckedException {
        IgniteCache<BadHashKeyObject, Byte> cache = ignite().cache(null);

        cache.put(new BadHashKeyObject("test_key1"), (byte)1);
        cache.put(new BadHashKeyObject("test_key0"), (byte)10);
        cache.put(new BadHashKeyObject("test_key1"), (byte)7);

        assertEquals(10, cache.query(new SqlQuery<BadHashKeyObject, Byte>(Byte.class, "_key = ?").
            setArgs(new BadHashKeyObject("test_key0"))).getAll().get(0).getValue().intValue());
    }

    /**
     * @throws IgniteCheckedException if failed.
     */
    public void testTextIndexedKey() throws IgniteCheckedException {
        IgniteCache<ObjectValue, Long> cache = ignite().cache(null);

        cache.put(new ObjectValue("test_key1", 10), 19L);
        cache.put(new ObjectValue("test_key0", 11), 11005L);
        cache.put(new ObjectValue("test_key1", 12), 17L);

        assertEquals(11005L,
            cache.query(new TextQuery<ObjectValue, Long>(Long.class, "test_key0"))
                .getAll().get(0).getValue().intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOrderByOnly() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> q =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= 0"));

        Collection<Cache.Entry<Integer, Integer>> res = q.getAll();

        assertEquals(10, res.size());

        if (cacheMode() != PARTITIONED) {
            Iterator<Cache.Entry<Integer, Integer>> it = res.iterator();

            for (Integer i = 0; i < 10; i++) {
                assertTrue(it.hasNext());

                Cache.Entry<Integer, Integer> e = it.next();

                assertEquals(i, e.getKey());
                assertEquals(i, e.getValue());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLimitOnly() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite().cache(null);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> q =
            cache.query(new SqlQuery<Integer, Integer>(Integer.class, "limit 5"));

        Collection<Cache.Entry<Integer, Integer>> res = q.getAll();

        assertEquals(5, res.size());

        Set<Integer> checkDuplicate = new HashSet<>();

        for (Cache.Entry<Integer, Integer> e : res) {
            assert e.getKey() < 10 && e.getKey() >= 0;
            assert e.getValue() < 10 && e.getValue() >= 0;

            checkDuplicate.add(e.getValue());
        }

        assertEquals(5, checkDuplicate.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testArray() throws Exception {
        IgniteCache<Integer, ArrayObject> cache = ignite().cache(null);

        cache.put(1, new ArrayObject(new Long[]{1L, null, 3L}));
        cache.put(2, new ArrayObject(new Long[] {4L, 5L, 6L}));

        QueryCursor<Cache.Entry<Integer, ArrayObject>> q =
            cache.query(new SqlQuery<Integer, ArrayObject>(ArrayObject.class, "array_contains(arr, cast(? as long))").
                setArgs(4));

        Collection<Cache.Entry<Integer, ArrayObject>> res = q.getAll();

        assertEquals(1, res.size());

        Cache.Entry<Integer, ArrayObject> e = F.first(res);

        assertEquals(2, (int)e.getKey());
        assertArrayEquals(new Long[]{4L, 5L, 6L}, e.getValue().arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueryEvents() throws Exception {
        checkSqlQueryEvents();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryMetadata() throws Exception {
        IgniteCache<UUID, Person> cache = ignite().cache(null);

        for (int i = 0; i < 100; i++)
            cache.put(UUID.randomUUID(), new Person("name-" + i, (i + 1) * 100));

        QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select name, salary from Person where name like ?")
            .setArgs("name-"));

        assertTrue(cur instanceof QueryCursorEx);

        QueryCursorEx<List<?>> curEx = (QueryCursorEx<List<?>>)cur;

        List<GridQueryFieldMetadata> meta = curEx.fieldsMeta();

        assertNotNull(meta);
        assertEquals(2, meta.size());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSqlQueryEvents() throws Exception {
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate[] lsnrs = new IgnitePredicate[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertNull(qe.cacheName());
                    assertNotNull(qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] {10}, qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(pred, EVT_CACHE_QUERY_EXECUTED);

            lsnrs[i] = pred;
        }

        try {
            IgniteCache<Integer, Integer> cache = ignite().cache(null);

            for (int i = 0; i < 20; i++)
                cache.put(i, i);

            QueryCursor<Cache.Entry<Integer, Integer>> q =
                cache.query(new SqlQuery<Integer, Integer>(Integer.class, "_key >= ?").setArgs(10));

            q.getAll();

            assert execLatch.await(1000, MILLISECONDS);
        }
        finally {
            for (int i = 0; i < gridCount(); i++)
                grid(i).events().stopLocalListen(lsnrs[i], EVT_CACHE_QUERY_EXECUTED);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryEvents() throws Exception {
        final Map<Integer, Integer> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(10);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate[] objReadLsnrs = new IgnitePredicate[gridCount()];
        IgnitePredicate[] qryExecLsnrs = new IgnitePredicate[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent<Integer, Integer> qe = (CacheQueryReadEvent<Integer, Integer>)evt;

                    assertEquals(SCAN.name(), qe.queryType());
                    assertNull(qe.cacheName());

                    assertNull(qe.className());
                    assertNull(null, qe.clause());
                    assertNotNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    map.put(qe.key(), qe.value());

                    latch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(pred, EVT_CACHE_QUERY_OBJECT_READ);
            objReadLsnrs[i] = pred;

            IgnitePredicate<Event> execPred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(SCAN.name(), qe.queryType());
                    assertNull(qe.cacheName());

                    assertNull(qe.className());
                    assertNull(null, qe.clause());
                    assertNotNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(execPred, EVT_CACHE_QUERY_EXECUTED);
            qryExecLsnrs[i] = execPred;
        }

        try {
            IgniteCache<Integer, Integer> cache = ignite().cache(null);

            for (int i = 0; i < 20; i++)
                cache.put(i, i);

            IgniteBiPredicate<Integer, Integer> filter = new IgniteBiPredicate<Integer, Integer>() {
                @Override public boolean apply(Integer k, Integer v) {
                    return k >= 10;
                }
            };

            QueryCursor<Cache.Entry<Integer, Integer>> q = cache.query(new ScanQuery<>(filter));

            q.getAll();

            assert latch.await(1000, MILLISECONDS);
            assert execLatch.await(1000, MILLISECONDS);

            assertEquals(10, map.size());

            for (int i = 10; i < 20; i++)
                assertEquals(i, map.get(i).intValue());
        }
        finally {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().stopLocalListen(objReadLsnrs[i]);
                grid(i).events().stopLocalListen(qryExecLsnrs[i]);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTextQueryEvents() throws Exception {
        final Map<UUID, Person> map = new ConcurrentHashMap8<>();
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate[] objReadLsnrs = new IgnitePredicate[gridCount()];
        IgnitePredicate[] qryExecLsnrs = new IgnitePredicate[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            IgnitePredicate<Event> objReadPred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryReadEvent;

                    CacheQueryReadEvent<UUID, Person> qe = (CacheQueryReadEvent<UUID, Person>)evt;

                    assertEquals(FULL_TEXT.name(), qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals("Person", qe.className());
                    assertEquals("White", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    map.put(qe.key(), qe.value());

                    latch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(objReadPred, EVT_CACHE_QUERY_OBJECT_READ);
            objReadLsnrs[i] = objReadPred;

            IgnitePredicate<Event> qryExecPred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertEquals(FULL_TEXT.name(), qe.queryType());
                    assertNull(qe.cacheName());

                    assertEquals("Person", qe.className());
                    assertEquals("White", qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertNull(qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(qryExecPred, EVT_CACHE_QUERY_EXECUTED);
            qryExecLsnrs[i] = qryExecPred;
        }

        try {
            IgniteCache<UUID, Person> cache = ignite().cache(null);

            UUID k1 = UUID.randomUUID();
            UUID k2 = UUID.randomUUID();
            UUID k3 = UUID.randomUUID();

            cache.put(k1, new Person("Bob White", 1000));
            cache.put(k2, new Person("Tom White", 1000));
            cache.put(k3, new Person("Mike Green", 1000));

            QueryCursor<Cache.Entry<UUID, Person>> q = cache.query(new TextQuery<UUID, Person>(Person.class, "White"));

            q.getAll();

            assert latch.await(1000, MILLISECONDS);
            assert execLatch.await(1000, MILLISECONDS);

            assertEquals(2, map.size());

            assertEquals("Bob White", map.get(k1).name());
            assertEquals("Tom White", map.get(k2).name());
        }
        finally {
            for (int i = 0; i < gridCount(); i++) {
                grid(i).events().stopLocalListen(objReadLsnrs[i]);
                grid(i).events().stopLocalListen(qryExecLsnrs[i]);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQueryEvents() throws Exception {
        final CountDownLatch execLatch = new CountDownLatch(cacheMode() == REPLICATED ? 1 : gridCount());

        IgnitePredicate[] qryExecLsnrs = new IgnitePredicate[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            IgnitePredicate<Event> pred = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt instanceof CacheQueryExecutedEvent;

                    CacheQueryExecutedEvent qe = (CacheQueryExecutedEvent)evt;

                    assertNull(qe.cacheName());
                    assertNotNull(qe.clause());
                    assertNull(qe.scanQueryFilter());
                    assertNull(qe.continuousQueryFilter());
                    assertArrayEquals(new Integer[] {10}, qe.arguments());

                    execLatch.countDown();

                    return true;
                }
            };

            grid(i).events().localListen(pred, EVT_CACHE_QUERY_EXECUTED);
            qryExecLsnrs[i] = pred;
        }

        try {
            IgniteCache<UUID,Person> cache = ignite().cache(null);

            for (int i = 1; i <= 20; i++)
                cache.put(UUID.randomUUID(), new Person("Person " + i, i));

            QueryCursor<List<?>> q = cache.query(new SqlFieldsQuery("select _key, name from Person where salary > ?").
                setArgs(10));

            q.getAll();

            assert execLatch.await(1000, MILLISECONDS);
        }
        finally {
            for (int i = 0; i < gridCount(); i++)
                grid(i).events().stopLocalListen(qryExecLsnrs[i]);
        }
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
        public int salary() {
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
     *
     */
    public static class Type1 implements Serializable {
        /** */
        private int id;

        /** */
        private String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Type1(int id, String name) {
            assert name != null;
            assert id > 0;

            this.name = name;
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return ID.
         */
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return name.hashCode() + 31 * id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (!(obj instanceof Type1))
                return false;

            Type1 that = (Type1)obj;

            return that.name.equals(name) && that.id == id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Type1.class, this);
        }
    }

    /**
     *
     */
    public static class Type2 implements Serializable {
        /** */
        private int id;

        /** */
        private String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        Type2(int id, String name) {
            assert name != null;
            assert id > 0;

            this.name = name;
            this.id = id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }

        /**
         * @return ID.
         */
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return name.hashCode() + 31 * id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (!(obj instanceof Type2))
                return false;

            Type2 that = (Type2)obj;

            return that.name.equals(name) && that.id == id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Type2.class, this);
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
    private static class BadHashKeyObject implements Serializable, Comparable<BadHashKeyObject> {
        /** */
        @QuerySqlField(index = false)
        private final String str;

        /**
         * @param str String.
         */
        private BadHashKeyObject(String str) {
            this.str = str == null ? "" : str;
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
        @Override public int compareTo(BadHashKeyObject o) {
            return str.compareTo(o.str);
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
     *
     */
    private static class StoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            return store;
        }
    }

    /**
     * Test enum class.
     */
    private static class EnumObject {
        /**
         * Test id.
         */
        @QuerySqlField(index = true)
        private long id;

        /**
         * Test enum.
         */
        @QuerySqlField
        private EnumType type;

        /**
         * @param id id.
         * @param type enum.
         */
        public EnumObject(long id, EnumType type) {
            this.id = id;
            this.type = type;
        }

        /**
         * @return string representation of object.
         */
        @Override public String toString() {
            return "EnumObject{" +
                    "id=" + id +
                    ", type=" + type +
                    '}';
        }
    }

    /**
     * Test enum.
     */
    private enum EnumType {
        /** */
        TYPE_A,
        /** */
        TYPE_B
    }
}
