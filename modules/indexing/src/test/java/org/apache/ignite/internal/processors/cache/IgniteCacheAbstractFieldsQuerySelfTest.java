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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for fields queries.
 */
public abstract class IgniteCacheAbstractFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE = "cache";

    /** Empty cache name. */
    private static final String EMPTY_CACHE = "emptyCache";

    /** Name of the cache that doesn't index primitives. */
    private static final String CACHE_NO_PRIMITIVES = "cacheNoPrimitives";

    /** Name of the cache that doesn't index primitives. */
    private static final String CACHE_COMPLEX_KEYS = "cacheComplexKeys";

    /** Flag indicating if starting node should have cache. */
    protected boolean hasCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        if (hasCache)
            cfg.setCacheConfiguration(cache(null, true), cache(CACHE, true), cache(EMPTY_CACHE, true),
                cache(CACHE_NO_PRIMITIVES, false), cache(CACHE_COMPLEX_KEYS, false));
        else
            cfg.setCacheConfiguration();

        cfg.setDiscoverySpi(discovery());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param primitives Index primitives.
     * @return Cache.
     */
    protected CacheConfiguration cache(@Nullable String name, boolean primitives) {
        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cache.setRebalanceMode(CacheRebalanceMode.SYNC);

        List<Class<?>> indexedTypes;

        if (EMPTY_CACHE.equals(name))
            indexedTypes = new ArrayList<>();
        else {
            indexedTypes = new ArrayList<>(F.<Class<?>>asList(
                String.class, Organization.class
            ));

            if (CACHE_COMPLEX_KEYS.equals(name)) {
                indexedTypes.addAll(F.<Class<?>>asList(
                    PersonKey.class, Person.class
                ));
            }
            else {
                indexedTypes.addAll(F.<Class<?>>asList(
                    AffinityKey.class, Person.class
                ));
            }

            if (!CACHE_NO_PRIMITIVES.equals(name)) {
                indexedTypes.addAll(F.<Class<?>>asList(
                    String.class, String.class,
                    Integer.class, Integer.class
                ));
            }
        }

        cache.setIndexedTypes(indexedTypes.toArray(new Class[indexedTypes.size()]));

        if (cacheMode() == PARTITIONED)
            cache.setBackups(1);

        return cache;
    }

    /** @return Discovery SPI. */
    private DiscoverySpi discovery() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        hasCache = true;

        startGridsMultiThreaded(gridCount());

        hasCache = false;

        startGrid(gridCount());

        IgniteCache<String, Organization> orgCache = grid(0).cache(null);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<AffinityKey<String>, Person> personCache = grid(0).cache(null);

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));

        IgniteCache<String, String> strCache = grid(0).cache(null);

        assert strCache != null;

        strCache.put("key", "val");

        IgniteCache<Integer, Integer> intCache = grid(0).cache(null);

        assert intCache != null;

        for (int i = 0; i < 200; i++)
            intCache.put(i, i);

        IgniteCache<Integer, Integer> namedCache = grid(0).cache(CACHE);

        for (int i = 0; i < 200; i++)
            namedCache.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
        ((IgniteKernal)grid(0)).getCache(null).getAndPut(new GridCacheInternalKeyImpl("LONG"), new GridCacheAtomicLongValue(0));

        try {
            Collection<GridCacheSqlMetadata> metas =
                ((IgniteKernal)grid(0)).getCache(null).context().queries().sqlMetadata();

            assert metas != null;

            boolean wasNull = false;
            boolean wasNamed = false;
            boolean wasEmpty = false;
            boolean wasNoPrimitives = false;
            boolean wasComplexKeys = false;

            for (GridCacheSqlMetadata meta : metas) {
                if (meta.cacheName() == null) {
                    Collection<String> types = meta.types();

                    assert types != null;
                    assert types.size() == 4;
                    assert types.contains("Person");
                    assert types.contains("Organization");
                    assert types.contains("String");
                    assert types.contains("Integer");

                    assert AffinityKey.class.getName().equals(meta.keyClass("Person"));
                    assert String.class.getName().equals(meta.keyClass("Organization"));
                    assert String.class.getName().equals(meta.keyClass("String"));

                    assert Person.class.getName().equals(meta.valueClass("Person"));
                    assert Organization.class.getName().equals(meta.valueClass("Organization"));
                    assert String.class.getName().equals(meta.valueClass("String"));

                    Map<String, String> fields = meta.fields("Person");

                    assert fields != null;
                    assert fields.size() == 5;
                    assert AffinityKey.class.getName().equals(fields.get("_KEY"));
                    assert Person.class.getName().equals(fields.get("_VAL"));
                    assert String.class.getName().equals(fields.get("NAME"));
                    assert int.class.getName().equals(fields.get("AGE"));
                    assert int.class.getName().equals(fields.get("ORGID"));

                    fields = meta.fields("Organization");

                    assert fields != null;
                    assert fields.size() == 4;
                    assert String.class.getName().equals(fields.get("_KEY"));
                    assert Organization.class.getName().equals(fields.get("_VAL"));
                    assert int.class.getName().equals(fields.get("ID"));
                    assert String.class.getName().equals(fields.get("NAME"));

                    fields = meta.fields("String");

                    assert fields != null;
                    assert fields.size() == 2;
                    assert String.class.getName().equals(fields.get("_KEY"));
                    assert String.class.getName().equals(fields.get("_VAL"));

                    fields = meta.fields("Integer");

                    assert fields != null;
                    assert fields.size() == 2;
                    assert Integer.class.getName().equals(fields.get("_KEY"));
                    assert Integer.class.getName().equals(fields.get("_VAL"));

                    Collection<GridCacheSqlIndexMetadata> indexes = meta.indexes("Person");

                    assertEquals(2, indexes.size());

                    wasNull = true;
                }
                else if (CACHE.equals(meta.cacheName()))
                    wasNamed = true;
                else if (EMPTY_CACHE.equals(meta.cacheName())) {
                    assert meta.types().isEmpty();

                    wasEmpty = true;
                }
                else if (CACHE_NO_PRIMITIVES.equals(meta.cacheName()))
                    wasNoPrimitives = true;
                else if (CACHE_COMPLEX_KEYS.equals(meta.cacheName()))
                    wasComplexKeys = true;
            }

            assert wasNull;
            assert wasNamed;
            assert wasEmpty;
            assert wasNoPrimitives;
            assert wasComplexKeys;
        }
        finally {
            ((IgniteKernal)grid(0)).getCache(null).remove(new GridCacheInternalKeyImpl("LONG"));
        }
    }

    /**
     *
     */
    public void testExplain() {
        List<List<?>> res = grid(0).cache(null).query(new SqlFieldsQuery(
            "explain select p.age, p.name, o.name " +
            "from Person p, Organization o where p.orgId = o.id")).getAll();

        for (List<?> row : res)
            X.println("____ : " + row);

        if (cacheMode() == PARTITIONED) {
            assertEquals(2, res.size());

            assertTrue(((String)res.get(1).get(0)).contains(GridSqlQuerySplitter.table(0).getSQL()));
        }
        else
            assertEquals(1, res.size());
    }

    /** @throws Exception If failed. */
    public void testExecuteWithMetaData() throws Exception {
        QueryCursorImpl<List<?>> cursor = (QueryCursorImpl<List<?>>)grid(0).cache(null).query(new SqlFieldsQuery(
            "select p._KEY, p.name, p.age, o.name " +
            "from Person p, Organization o where p.orgId = o.id"));

        Collection<GridQueryFieldMetadata> meta = cursor.fieldsMeta();

        assert meta != null;
        assert meta.size() == 4;

        Iterator<GridQueryFieldMetadata> metaIt = meta.iterator();

        assert metaIt != null;
        assert metaIt.hasNext();

        GridQueryFieldMetadata field = metaIt.next();

        assert field != null;
        assert "".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());
        assert Object.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "AGE".equals(field.fieldName());
        assert Integer.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

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
                assert new AffinityKey<>("p1", "o1").equals(row.get(0));
                assert "John White".equals(row.get(1));
                assert row.get(2).equals(25);
                assert "A".equals(row.get(3));
            }
            else if (cnt == 1) {
                assert new AffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
                assert "A".equals(row.get(3));
            }
            if (cnt == 2) {
                assert new AffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
                assert "B".equals(row.get(3));
            }

            cnt++;
        }

        assert cnt == 3;
    }

    /** @throws Exception If failed. */
    public void testExecute() throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select _KEY, name, age from Person"));

        List<List<?>> res = new ArrayList<>(qry.getAll());

        assert res != null;

        dedup(res);

        assertEquals(res.size(), 3);

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assert row.size() == 3;

            if (cnt == 0) {
                assert new AffinityKey<>("p1", "o1").equals(row.get(0));
                assert "John White".equals(row.get(1));
                assert row.get(2).equals(25);
            }
            else if (cnt == 1) {
                assert new AffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
            }
            if (cnt == 2) {
                assert new AffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
            }

            cnt++;
        }

        assert cnt == 3;
    }

    /** @throws Exception If failed. */
    public void testExecuteWithArguments() throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select _KEY, name, age from Person where age > ?").setArgs(30));

        List<List<?>> res = new ArrayList<>(qry.getAll());

        assert res != null;

        dedup(res);

        assert res.size() == 2;

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assert row.size() == 3;

            if (cnt == 0) {
                assert new AffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
            }
            else if (cnt == 1) {
                assert new AffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
            }

            cnt++;
        }

        assert cnt == 2;
    }

    /** @throws Exception If failed. */
    public void testSelectAllJoined() throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select * from Person p, Organization o where p.orgId = o.id"));

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
            assert row.size() == 9;

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
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select name from Person where age = 0"));

        Collection<List<?>> res = qry.getAll();

        assert res != null;
        assert res.isEmpty();
    }

    /** @throws Exception If failed. */
    public void testQueryString() throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(null).query(new SqlFieldsQuery("select * from String"));

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
        QueryCursor<List<?>> qry = grid(0).cache(null).query(new SqlFieldsQuery(
            "select i._KEY, i._VAL, j._KEY, j._VAL from Integer i join Integer j where i._VAL >= 100"));

        Collection<List<?>> res = qry.getAll();

        assert res != null;

        if (cacheMode() == LOCAL)
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
        QueryCursor<List<?>> qry = grid(0).cache(null)
                .query(new SqlFieldsQuery("select * from Integer").setPageSize(20));

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
        IgniteCache<Integer, Integer> cache = grid(0).cache(CACHE);

        for (int i = 0; i < 200; i++)
            cache.put(i, i);

        QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery("select * from Integer"));

        Collection<List<?>> res = qry.getAll();

        assert res != null;
        assert res.size() == 200;
    }

    /** @throws Exception If failed. */
    public void testNoPrimitives() throws Exception {
        final IgniteCache<Object, Object> cache = grid(0).cache(CACHE_NO_PRIMITIVES);

        cache.put("key", "val");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.query(new SqlFieldsQuery("select * from String"));
            }
        }, CacheException.class, null);
    }

    /** @throws Exception If failed. */
    public void testComplexKeys() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(CACHE_COMPLEX_KEYS);

        UUID id = UUID.randomUUID();

        PersonKey key = new PersonKey(id);
        Person val = new Person("John", 20, 1);

        cache.put(key, val);

        Collection<List<?>> res = cache.query(new SqlFieldsQuery("select * from Person")).getAll();

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

    /** @throws Exception If failed. */
    public void testPaginationIteratorDefaultCache() throws Exception {
        testPaginationIterator(null);
    }

    /** @throws Exception If failed. */
    public void testPaginationIteratorNamedCache() throws Exception {
        testPaginationIterator(CACHE);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void testPaginationIterator(@Nullable String cacheName) throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(cacheName)
            .query(new SqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

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
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        int cnt = 0;

        for (List<?> row : qry) {
            assertEquals(2, row.size());
            assertEquals(row.get(0), row.get(1));
            assertTrue((Integer)row.get(0) >= 0 && (Integer)row.get(0) < 200);

            cnt++;
        }

        int size = 200;

        assertEquals(size, cnt);

        qry = grid(0).cache(null).query(new SqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

        List<List<?>> list = new ArrayList<>(qry.getAll());

        dedup(list);

        Collections.sort(list, new Comparator<List<?>>() {
            @Override public int compare(List<?> r1, List<?> r2) {
                return ((Integer) r1.get(0)).compareTo((Integer) r2.get(0));
            }
        });

        for (int i = 0; i < 200; i++) {
            List<?> r = list.get(i);

            assertEquals(i, r.get(0));
            assertEquals(i, r.get(1));
        }
    }

    /** @throws Exception If failed. */
    public void testPaginationGetDefaultCache() throws Exception {
        testPaginationGet(null);
    }

    /** @throws Exception If failed. */
    public void testPaginationGetNamedCache() throws Exception {
        testPaginationGet(CACHE);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void testPaginationGet(@Nullable String cacheName) throws Exception {
        QueryCursor<List<?>> qry = grid(0).cache(cacheName)
            .query(new SqlFieldsQuery("select _key, _val from Integer").setPageSize(10));

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
        QueryCursor<List<?>> qry = grid(0).cache(null)
            .query(new SqlFieldsQuery("select name, age from Person where age = 25"));

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