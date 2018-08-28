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

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for fields queries.
 */
public abstract class IgniteCacheAbstractFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static IgniteCache<String, Organization> orgCache;

    /** */
    private static IgniteCache<AffinityKey<String>, Person> personCache;

    /** */
    private static IgniteCache<String, String> strCache;

    /** */
    protected static IgniteCache<Integer, Integer> intCache;

    /** */
    protected static IgniteCache<?, ?> noOpCache;

    /** Flag indicating if starting node should have cache. */
    protected boolean hasCache;

    /** Whether BinaryMarshaller is set. */
    protected boolean binaryMarshaller;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDiscoverySpi(discovery());

        if (hasCache)
            cfg.setCacheConfiguration(cacheConfiguration());
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        if (cacheMode() == PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    /** @return Discovery SPI. */
    private DiscoverySpi discovery() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @param clsK Class k.
     * @param clsV Class v.
     */
    protected <K, V> IgniteCache<K, V> jcache(Class<K> clsK, Class<V> clsV) {
        return jcache(grid(0), cacheConfiguration(), clsK, clsV);
    }

    /**
     * @param name Name.
     * @param clsK Class k.
     * @param clsV Class v.
     */
    protected <K, V> IgniteCache<K, V> jcache(String name, Class<K> clsK, Class<V> clsV) {
        return jcache(grid(0), cacheConfiguration(), name, clsK, clsV);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        hasCache = true;

        startGridsMultiThreaded(gridCount());

        hasCache = false;

        startGrid(gridCount());

        orgCache = jcache(String.class, Organization.class);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<?, ?> c = jcache(AffinityKey.class, Person.class);
        personCache = (IgniteCache<AffinityKey<String>, Person>)c;

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));

        strCache = jcache(String.class, String.class);

        assert strCache != null;

        strCache.put("key", "val");

        intCache = jcache(Integer.class, Integer.class);

        assert intCache != null;

        for (int i = 0; i < 200; i++)
            intCache.put(i, i);

        noOpCache = grid(0).getOrCreateCache("noop");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        binaryMarshaller = grid(0).configuration().getMarshaller() instanceof BinaryMarshaller;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        orgCache = null;
        personCache = null;
        strCache = null;
        intCache = null;
        noOpCache = null;
    }

    /** @return cache mode. */
    protected abstract CacheMode cacheMode();

    /** @return Cache atomicity mode. */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @return Number of grids to start. */
    protected abstract int gridCount();

    /** @throws Exception If failed. */
    public void testCacheMetaData() throws Exception {
        // Put internal key to test filtering of internal objects.

        for (String cacheName : grid(0).cacheNames())
            ((IgniteKernal)grid(0)).getCache(cacheName).getAndPut(new GridCacheInternalKeyImpl("LONG", ""), new GridCacheAtomicLongValue(0));

        try {
            Collection<GridCacheSqlMetadata> metas =
                ((IgniteKernal)grid(0)).getCache(intCache.getName()).context().queries().sqlMetadata();

            assert metas != null;

            for (GridCacheSqlMetadata meta : metas) {
                Collection<String> types = meta.types();

                assertNotNull(types);

                if (personCache.getName().equals(meta.cacheName())) {
                    assertEquals("Invalid types size", 1, types.size());
                    assert types.contains("Person");

                    if (binaryMarshaller) {
                        assert Object.class.getName().equals(meta.keyClass("Person"));
                        assert Object.class.getName().equals(meta.valueClass("Person"));
                    }
                    else {
                        assert AffinityKey.class.getName().equals(meta.keyClass("Person"));
                        assert Person.class.getName().equals(meta.valueClass("Person"));
                    }

                    Map<String, String> fields = meta.fields("Person");

                    assert fields != null;
                    assert fields.size() == 3;

                    if (binaryMarshaller) {
                        assert Integer.class.getName().equals(fields.get("AGE"));
                        assert Integer.class.getName().equals(fields.get("ORGID"));
                    }
                    else {
                        assert int.class.getName().equals(fields.get("AGE"));
                        assert int.class.getName().equals(fields.get("ORGID"));
                    }

                    assert String.class.getName().equals(fields.get("NAME"));

                    Collection<GridCacheSqlIndexMetadata> indexes = meta.indexes("Person");

                    assertNotNull("Indexes should be defined", indexes);
                    assertEquals(2, indexes.size());

                    Set<String> idxFields = new HashSet<>();

                    Iterator<GridCacheSqlIndexMetadata> it = indexes.iterator();

                    Collection<String> indFlds = it.next().fields();

                    assertNotNull("Fields for first index should be defined", indFlds);
                    assertEquals("First index should have one field", indFlds.size(), 1);

                    Iterator<String> indFldIt = indFlds.iterator();

                    idxFields.add(indFldIt.next());

                    indFlds = it.next().fields();

                    assertNotNull("Fields for second index should be defined", indFlds);
                    assertEquals("Second index should have one field", indFlds.size(), 1);

                    indFldIt = indFlds.iterator();

                    idxFields.add(indFldIt.next());

                    assertTrue(idxFields.contains("AGE"));
                    assertTrue(idxFields.contains("ORGID"));
                }
                else if (orgCache.getName().equals(meta.cacheName())) {
                    assertEquals("Invalid types size", 1, types.size());
                    assert types.contains("Organization");

                    if (binaryMarshaller)
                        assert Object.class.getName().equals(meta.valueClass("Organization"));
                    else
                        assert Organization.class.getName().equals(meta.valueClass("Organization"));

                    assert String.class.getName().equals(meta.keyClass("Organization"));

                    Map<String, String> fields = meta.fields("Organization");

                    assert fields != null;
                    assertEquals("Fields: " + fields, 2, fields.size());

                    if (binaryMarshaller) {
                        assert Integer.class.getName().equals(fields.get("ID"));
                    }
                    else {
                        assert int.class.getName().equals(fields.get("ID"));
                    }

                    assert String.class.getName().equals(fields.get("NAME"));
                }
                else if (intCache.getName().equals(meta.cacheName())) {
                    assertEquals("Invalid types size", 1, types.size());
                    assert types.contains("Integer");

                    assert Integer.class.getName().equals(meta.valueClass("Integer"));
                    assert Integer.class.getName().equals(meta.keyClass("Integer"));

                    Map<String, String> fields = meta.fields("Integer");

                    assert fields != null;
                    assert fields.size() == 2;
                    assert Integer.class.getName().equals(fields.get("_KEY"));
                    assert Integer.class.getName().equals(fields.get("_VAL"));
                }
                else if (strCache.getName().equals(meta.cacheName())) {
                    assertEquals("Invalid types size", 1, types.size());
                    assert types.contains("String");

                    assert String.class.getName().equals(meta.valueClass("String"));
                    assert String.class.getName().equals(meta.keyClass("String"));

                    Map<String, String> fields = meta.fields("String");

                    assert fields != null;
                    assert fields.size() == 2;
                    assert String.class.getName().equals(fields.get("_KEY"));
                    assert String.class.getName().equals(fields.get("_VAL"));
                }
                else if (DEFAULT_CACHE_NAME.equals(meta.cacheName()) || noOpCache.getName().equals(meta.cacheName()))
                    assertTrue("Invalid types size", types.isEmpty());
                else if (!"cacheWithCustomKeyPrecision".equalsIgnoreCase(meta.cacheName()))
                    fail("Unknown cache: " + meta.cacheName());
            }
        }
        finally {
            ((IgniteKernal)grid(0)).getCache(intCache.getName()).remove(new GridCacheInternalKeyImpl("LONG", ""));
        }
    }

    /**
     *
     */
    public void testExplain() {
        List<List<?>> res = grid(0).cache(personCache.getName()).query(sqlFieldsQuery(
            String.format("explain select p.age, p.name, o.name " +
                    "from \"%s\".Person p, \"%s\".Organization o where p.orgId = o.id",
                personCache.getName(), orgCache.getName()))).getAll();

        for (List<?> row : res)
            X.println("____ : " + row);

        if (cacheMode() == PARTITIONED || (cacheMode() == REPLICATED && !isReplicatedOnly())) {
            assertEquals(2, res.size());

            assertTrue(((String)res.get(1).get(0)).contains(GridSqlQuerySplitter.mergeTableIdentifier(0)));
        }
        else
            assertEquals(1, res.size());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("unchecked")
    public void testExecuteWithMetaDataAndPrecision() throws Exception {
        QueryEntity qeWithPrecision = new QueryEntity()
            .setKeyType("java.lang.Long")
            .setValueType("TestType")
            .addQueryField("strField", "java.lang.String", "strField")
            .setFieldsPrecision(ImmutableMap.of("strField", 999));

        grid(0).getOrCreateCache(cacheConfiguration()
            .setName("cacheWithPrecision")
            .setQueryEntities(Collections.singleton(qeWithPrecision)));

        GridQueryProcessor qryProc = grid(0).context().query();

        qryProc.querySqlFields(
            new SqlFieldsQuery("INSERT INTO TestType(_KEY, strField) VALUES(?, ?)")
                .setSchema("cacheWithPrecision")
                .setArgs(1, "ABC"), true);

        qryProc.querySqlFields(
            new SqlFieldsQuery("INSERT INTO TestType(_KEY, strField) VALUES(?, ?)")
                .setSchema("cacheWithPrecision")
                .setArgs(2, "DEF"), true);


        QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)qryProc.querySqlFields(
            new SqlFieldsQuery("SELECT _KEY, strField FROM TestType")
                .setSchema("cacheWithPrecision"), true);

        List<GridQueryFieldMetadata> fieldsMeta = cursor.fieldsMeta();

        for (GridQueryFieldMetadata meta : fieldsMeta) {
            if (!meta.fieldName().equalsIgnoreCase("strField"))
                continue;

            assertEquals(999, meta.precision());
        }
    }

    public void testExecuteWithMetaDataAndCustomKeyPrecision() throws Exception {
        QueryEntity qeWithPrecision = new QueryEntity()
            .setKeyType("java.lang.String")
            .setKeyFieldName("my_key")
            .setValueType("CustomKeyType")
            .addQueryField("my_key", "java.lang.String", "my_key")
            .addQueryField("strField", "java.lang.String", "strField")
            .setFieldsPrecision(ImmutableMap.of("strField", 999, "my_key", 777));

        grid(0).getOrCreateCache(cacheConfiguration()
            .setName("cacheWithCustomKeyPrecision")
            .setQueryEntities(Collections.singleton(qeWithPrecision)));

        GridQueryProcessor qryProc = grid(0).context().query();

        qryProc.querySqlFields(
            new SqlFieldsQuery("INSERT INTO CustomKeyType(my_key, strField) VALUES(?, ?)")
                .setSchema("cacheWithCustomKeyPrecision")
                .setArgs("1", "ABC"), true);

        qryProc.querySqlFields(
            new SqlFieldsQuery("INSERT INTO CustomKeyType(my_key, strField) VALUES(?, ?)")
                .setSchema("cacheWithCustomKeyPrecision")
                .setArgs("2", "DEF"), true);

        QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)qryProc.querySqlFields(
            new SqlFieldsQuery("SELECT my_key, strField FROM CustomKeyType")
                .setSchema("cacheWithCustomKeyPrecision"), true);

        List<GridQueryFieldMetadata> fieldsMeta = cursor.fieldsMeta();

        int fldCnt = 0;

        for (GridQueryFieldMetadata meta : fieldsMeta) {
            switch (meta.fieldName()) {
                case "STRFIELD":
                    assertEquals(999, meta.precision());

                    fldCnt++;

                    break;

                case "MY_KEY":
                    assertEquals(777, meta.precision());

                    fldCnt++;

                    break;
                default:
                    fail("Unknown field - " + meta.fieldName());
            }
        }

        assertEquals("Metadata for all fields should be returned.", 2, fldCnt);
    }

    /** @throws Exception If failed. */
    public void testExecuteWithMetaData() throws Exception {
        QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)personCache.query(sqlFieldsQuery(
            String.format("select p._KEY, p.name, p.age, o.name " +
                    "from \"%s\".Person p, \"%s\".Organization o where p.orgId = o.id",
                personCache.getName(), orgCache.getName())));

        Collection<GridQueryFieldMetadata> meta = cursor.fieldsMeta();

        assertNotNull(meta);
        assertEquals(4, meta.size());

        Iterator<GridQueryFieldMetadata> metaIt = meta.iterator();

        assertNotNull(metaIt);
        assert metaIt.hasNext();

        GridQueryFieldMetadata field = metaIt.next();

        assertNotNull(field);
        assertEquals(personCache.getName(), field.schemaName());
        assertEquals("PERSON", field.typeName());
        assertEquals("_KEY", field.fieldName());
        assertEquals(Object.class.getName(), field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assertNotNull(field);
        assertEquals(personCache.getName(), field.schemaName());
        assertEquals("PERSON", field.typeName());
        assertEquals("NAME", field.fieldName());
        assertEquals(String.class.getName(), field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assertNotNull(field);
        assertEquals(personCache.getName(), field.schemaName());
        assertEquals("PERSON", field.typeName());
        assertEquals("AGE", field.fieldName());
        assertEquals(Integer.class.getName(), field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assertNotNull(field);
        assertEquals(orgCache.getName(), field.schemaName());
        assertEquals("ORGANIZATION", field.typeName());
        assertEquals("NAME", field.fieldName());
        assertEquals(String.class.getName(), field.fieldTypeName());

        assert !metaIt.hasNext();

        List<List<?>> res = cursor.getAll();

        dedup(res);

        assertEquals(3, res.size());

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assert row.size() == 4;

            if (cnt == 0) {
                assertEquals(new AffinityKey<>("p1", "o1"), row.get(0));
                assertEquals("John White", row.get(1));
                assertEquals(25, row.get(2));
                assertEquals("A", row.get(3));
            }
            else if (cnt == 1) {
                assertEquals(new AffinityKey<>("p2", "o1"), row.get(0));
                assertEquals("Joe Black", row.get(1));
                assertEquals(35, row.get(2));
                assertEquals("A", row.get(3));
            }
            if (cnt == 2) {
                assertEquals(new AffinityKey<>("p3", "o2"), row.get(0));
                assertEquals("Mike Green", row.get(1));
                assertEquals(40, row.get(2));
                assertEquals("B", row.get(3));
            }

            cnt++;
        }

        assertEquals(3, cnt);
    }

    /** @throws Exception If failed. */
    public void testExecute() throws Exception {
        doTestExecute(personCache, sqlFieldsQuery("select _KEY, name, age from Person"));
    }

    /** @throws Exception If failed. */
    public void testExecuteNoOpCache() throws Exception {
        doTestExecute(noOpCache, sqlFieldsQuery("select _KEY, name, age from \"AffinityKey-Person\".Person"));
    }

    /**
     * Execute given query and check results.
     * @param cache Cache to run query on.
     * @param fldsQry Query.
     * @throws Exception if failed.
     */
    private void doTestExecute (IgniteCache<?, ?> cache, SqlFieldsQuery fldsQry) throws Exception {
        QueryCursor<List<?>> qry = cache.query(fldsQry);

        List<List<?>> res = new ArrayList<>(qry.getAll());

        assertNotNull(res);

        dedup(res);

        assertEquals(res.size(), 3);

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assertEquals(3, row.size());

            if (cnt == 0) {
                assertEquals(new AffinityKey<>("p1", "o1"), row.get(0));
                assertEquals("John White", row.get(1));
                assertEquals(25, row.get(2));
            }
            else if (cnt == 1) {
                assertEquals(new AffinityKey<>("p2", "o1"), row.get(0));
                assertEquals("Joe Black", row.get(1));
                assertEquals(35, row.get(2));
            }
            if (cnt == 2) {
                assertEquals(new AffinityKey<>("p3", "o2"), row.get(0));
                assertEquals("Mike Green", row.get(1));
                assertEquals(40, row.get(2));
            }

            cnt++;
        }

        assertEquals(3, cnt);
    }

    /** @throws Exception If failed. */
    public void testExecuteWithArguments() throws Exception {
        QueryCursor<List<?>> qry = personCache
            .query(sqlFieldsQuery("select _KEY, name, age from Person where age > ?").setArgs(30));

        List<List<?>> res = new ArrayList<>(qry.getAll());

        dedup(res);

        assertEquals(2, res.size());

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assertEquals(3, row.size());

            if (cnt == 0) {
                assertEquals(new AffinityKey<>("p2", "o1"), row.get(0));
                assertEquals("Joe Black", row.get(1));
                assertEquals(35, row.get(2));
            }
            if (cnt == 1) {
                assertEquals(new AffinityKey<>("p3", "o2"), row.get(0));
                assertEquals("Mike Green", row.get(1));
                assertEquals(40, row.get(2));
            }

            cnt++;
        }

        assert cnt == 2;
    }

    protected boolean isReplicatedOnly() {
        return false;
    }

    private SqlFieldsQuery sqlFieldsQuery(String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (isReplicatedOnly())
            qry.setReplicatedOnly(true);

        return qry;
    }

    /** @throws Exception If failed. */
    public void testSelectAllJoined() throws Exception {
        QueryCursor<List<?>> qry =
            personCache.query(sqlFieldsQuery(
                String.format("select p._key, p._val, p.*, o._key, o._val, o.* from \"%s\".Person p, \"%s\".Organization o where p.orgId = o.id",
                    personCache.getName(), orgCache.getName())));

        List<List<?>> res = new ArrayList<>(qry.getAll());

        dedup(res);

        assertEquals(3, res.size());

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(3)).compareTo((Integer)row2.get(3));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assertEquals(9, row.size());

            if (cnt == 0) {
                assert new AffinityKey<>("p1", "o1").equals(row.get(0));
                assert Person.class.getName().equals(row.get(1).getClass().getName());
                assert "John White".equals(row.get(2));
                assert row.get(3).equals(25);
                assert row.get(4).equals(1);
                assert "o1".equals(row.get(5));
                assert Organization.class.getName().equals(row.get(6).getClass().getName());
                assert row.get(7).equals(1);
                assert "A".equals(row.get(8));
            }
            else if (cnt == 1) {
                assert new AffinityKey<>("p2", "o1").equals(row.get(0));
                assert Person.class.getName().equals(row.get(1).getClass().getName());
                assert "Joe Black".equals(row.get(2));
                assert row.get(3).equals(35);
                assert row.get(4).equals(1);
                assert "o1".equals(row.get(5));
                assert Organization.class.getName().equals(row.get(6).getClass().getName());
                assert row.get(7).equals(1);
                assert "A".equals(row.get(8));
            }
            if (cnt == 2) {
                assert new AffinityKey<>("p3", "o2").equals(row.get(0));
                assert Person.class.getName().equals(row.get(1).getClass().getName());
                assert "Mike Green".equals(row.get(2));
                assert row.get(3).equals(40);
                assert row.get(4).equals(2);
                assert "o2".equals(row.get(5));
                assert Organization.class.getName().equals(row.get(6).getClass().getName());
                assert row.get(7).equals(2);
                assert "B".equals(row.get(8));
            }

            cnt++;
        }

        assert cnt == 3;
    }

    /** @throws Exception If failed. */
    public void testEmptyResult() throws Exception {
        QueryCursor<List<?>> qry =
            personCache.query(sqlFieldsQuery("select name from Person where age = 0"));

        Collection<List<?>> res = qry.getAll();

        assertNotNull(res);
        assertTrue(res.isEmpty());
    }

    /**
     * Verifies that exactly one record is found when we have equality comparison in where clause (which is supposed
     * to use {@link BPlusTree#findOne(Object, Object)} instead of {@link BPlusTree#find(Object, Object, Object)}.
     *
     * @throws Exception If failed.
     */
    public void testSingleResultUsesFindOne() throws Exception {
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select _val from Integer where _key = 25"));

        List<List<?>> res = qry.getAll();

        assertNotNull(res);
        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(25, res.get(0).get(0));
    }

    /**
     * Verifies that zero records are found when we have equality comparison in where clause (which is supposed
     * to use {@link BPlusTree#findOne(Object, Object)} instead of {@link BPlusTree#find(Object, Object, Object)}
     * and the key is not in the cache.
     *
     * @throws Exception If failed.
     */
    public void testEmptyResultUsesFindOne() throws Exception {
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select _val from Integer where _key = -10"));

        List<List<?>> res = qry.getAll();

        assertNotNull(res);
        assertEquals(0, res.size());
    }

    /** @throws Exception If failed. */
    public void testQueryString() throws Exception {
        QueryCursor<List<?>> qry = strCache.query(sqlFieldsQuery("select * from String"));

        Collection<List<?>> res = qry.getAll();

        assert res != null;
        assert res.size() == 1;

        for (List<?> row : res) {
            assert row != null;
            assert row.size() == 2;
            assert "key".equals(row.get(0));
            assert "val".equals(row.get(1));
        }
    }

    /** @throws Exception If failed. */
    public void testQueryIntegersWithJoin() throws Exception {
        QueryCursor<List<?>> qry = intCache.query(sqlFieldsQuery(
            "select i._KEY, i._VAL, j._KEY, j._VAL from Integer i join Integer j where i._VAL >= 100"));

        Collection<List<?>> res = qry.getAll();

        assert res != null;

        if (cacheMode() == CacheMode.LOCAL)
            assert res.size() == 20000;
        else
            assert res.size() <= 20000;

        for (List<?> row : res) {
            assert (Integer)row.get(0) >= 100;
            assert (Integer)row.get(1) >= 100;
            assert (Integer)row.get(2) >= 0;
            assert (Integer)row.get(3) >= 0;
        }
    }

    /** @throws Exception If failed. */
    public void testPagination() throws Exception {
        // Query with page size 20.
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select * from Integer").setPageSize(20));

        List<List<?>> res = new ArrayList<>(qry.getAll());

        dedup(res);

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> r1, List<?> r2) {
                return ((Integer)r1.get(0)).compareTo((Integer)r2.get(0));
            }
        });

        assertEquals(200, res.size());

        for (List<?> row : res)
            assertEquals("Wrong row size: " + row, 2, row.size());
    }

    /** @throws Exception If failed. */
    public void testNamedCache() throws Exception {
        try {
            IgniteCache<Integer, Integer> cache = jcache("tmp_int", Integer.class, Integer.class);

            for (int i = 0; i < 200; i++)
                cache.put(i, i);

            QueryCursor<List<?>> qry = cache.query(sqlFieldsQuery("select * from Integer"));

            Collection<List<?>> res = qry.getAll();

            assert res != null;
            assert res.size() == 200;
        }
        finally {
            grid(0).destroyCache("tmp_int");
        }
    }

    /** @throws Exception If failed. */
    public void testNoPrimitives() throws Exception {
        try {
            final IgniteCache<Object, Object> cache = grid(0).getOrCreateCache("tmp_without_index");

            cache.put("key", "val");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return cache.query(sqlFieldsQuery("select * from String"));
                }
            }, CacheException.class, null);
        }
        finally {
            grid(0).destroyCache("tmp_without_index");
        }
    }

    /** @throws Exception If failed. */
    public void testComplexKeys() throws Exception {
        IgniteCache<PersonKey, Person> cache = jcache(PersonKey.class, Person.class);

        UUID id = UUID.randomUUID();

        PersonKey key = new PersonKey(id);
        Person val = new Person("John", 20, 1);

        cache.put(key, val);

        Collection<List<?>> res = cache.query(sqlFieldsQuery("select _key, _val, * from Person")).getAll();

        assertEquals(1, res.size());

        for (Collection<?> row : res) {
            int cnt = 0;

            for (Object fieldVal : row) {
                if (cnt == 0)
                    assertEquals(key, fieldVal);
                else if (cnt == 1)
                    assertEquals(val, fieldVal);
                else if (cnt == 2)
                    assertEquals(id, fieldVal);
                else if (cnt == 3)
                    assertEquals("John", fieldVal);
                else if (cnt == 4)
                    assertEquals(20, fieldVal);
                else if (cnt == 5)
                    assertEquals(1, fieldVal);

                cnt++;
            }
        }

        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationIterator() throws Exception {
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        int cnt = 0;

        for (List<?> row : qry) {
            assertEquals(2, row.size());
            assertEquals(row.get(0), row.get(1));
            assertTrue((Integer)row.get(0) >= 0 && (Integer)row.get(0) < 200);

            cnt++;
        }

        int size = 200;

        assertEquals(size, cnt);
    }

    /** @throws Exception If failed. */
    public void testPaginationIteratorKeepAll() throws Exception {
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        int cnt = 0;

        for (List<?> row : qry) {
            assertEquals(2, row.size());
            assertEquals(row.get(0), row.get(1));
            assertTrue((Integer)row.get(0) >= 0 && (Integer)row.get(0) < 200);

            cnt++;
        }

        int size = 200;

        assertEquals(size, cnt);

        qry = intCache.query(sqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        List<List<?>> list = new ArrayList<>(qry.getAll());

        dedup(list);

        Collections.sort(list, new Comparator<List<?>>() {
            @Override public int compare(List<?> r1, List<?> r2) {
                return ((Integer)r1.get(0)).compareTo((Integer)r2.get(0));
            }
        });

        for (int i = 0; i < 200; i++) {
            List<?> r = list.get(i);

            assertEquals(i, r.get(0));
            assertEquals(i, r.get(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPaginationGet() throws Exception {
        QueryCursor<List<?>> qry =
            intCache.query(sqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        List<List<?>> list = new ArrayList<>(qry.getAll());

        dedup(list);

        Collections.sort(list, new Comparator<List<?>>() {
            @Override public int compare(List<?> r1, List<?> r2) {
                return ((Integer)r1.get(0)).compareTo((Integer)r2.get(0));
            }
        });

        for (int i = 0; i < 200; i++) {
            List<?> row = list.get(i);

            assertEquals(i, row.get(0));
            assertEquals(i, row.get(1));
        }
    }

    /** @throws Exception If failed. */
    public void testEmptyGrid() throws Exception {
        QueryCursor<List<?>> qry = personCache
            .query(sqlFieldsQuery("select name, age from Person where age = 25"));

        List<?> res = F.first(qry.getAll());

        assert res != null;
        assert res.size() == 2;
        assert "John White".equals(res.get(0));
        assert res.get(1).equals(25);
    }

    /**
     * Dedups result.
     *
     * @param res Result.
     * @throws Exception In case of error.
     */
    private void dedup(Collection<List<?>> res) throws Exception {
        assert res != null;

        if (cacheMode() != REPLICATED)
            return;

        Collection<List<?>> res0 = new ArrayList<>(res.size());

        Collection<Object> keys = new HashSet<>();

        for (List<?> row : res) {
            Object key = row.get(0);

            if (!keys.contains(key)) {
                res0.add(row);
                keys.add(key);
            }
        }

        res.clear();

        res.addAll(res0);
    }

    /**
     * Person key.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class PersonKey implements Serializable {
        /** ID. */
        @QuerySqlField
        private final UUID id;

        /** @param id ID. */
        private PersonKey(UUID id) {
            assert id != null;

            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PersonKey key = (PersonKey)o;

            return id.equals(key.id);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /** Age. */
        @QuerySqlField(index = true)
        private final int age;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private final int orgId;

        /**
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         */
        private Person(String name, int age, int orgId) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId > 0;

            this.name = name;
            this.age = age;
            this.orgId = orgId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return age == person.age && orgId == person.orgId && name.equals(person.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = name.hashCode();

            res = 31 * res + age;
            res = 31 * res + orgId;

            return res;
        }
    }

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            assert id > 0;
            assert !F.isEmpty(name);

            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Organization that = (Organization)o;

            return id == that.id && name.equals(that.name);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = id;

            res = 31 * res + name.hashCode();

            return res;
        }
    }
}
