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
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 * Tests for fields queries.
 */
public abstract class GridCacheAbstractFieldsQuerySelfTest extends GridCommonAbstractTest {
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

        cfg.setMarshaller(new OptimizedMarshaller(false));

        if (hasCache)
            cfg.setCacheConfiguration(cache(null, null), cache(CACHE, null), cache(EMPTY_CACHE, null));
        else
            cfg.setCacheConfiguration();

        cfg.setDiscoverySpi(discovery());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param spiName Indexing SPI name.
     * @return Cache.
     */
    protected CacheConfiguration cache(@Nullable String name, @Nullable String spiName) {
        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cache.setRebalanceMode(SYNC);

        CacheQueryConfiguration qcfg = new CacheQueryConfiguration();

        qcfg.setIndexPrimitiveKey(true);
        qcfg.setIndexPrimitiveValue(true);
        qcfg.setIndexFixedTyping(true);

        cache.setQueryConfiguration(qcfg);

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

        GridCache<String, Organization> orgCache = ((IgniteKernal)grid(0)).cache(null);

        assert orgCache != null;

        assert orgCache.putx("o1", new Organization(1, "A"));
        assert orgCache.putx("o2", new Organization(2, "B"));

        GridCache<CacheAffinityKey<String>, Person> personCache = ((IgniteKernal)grid(0)).cache(null);

        assert personCache != null;

        assert personCache.putx(new CacheAffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        assert personCache.putx(new CacheAffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        assert personCache.putx(new CacheAffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));

        GridCache<String, String> strCache = ((IgniteKernal)grid(0)).cache(null);

        assert strCache != null;

        assert strCache.putx("key", "val");

        GridCache<Integer, Integer> intCache = ((IgniteKernal)grid(0)).cache(null);

        assert intCache != null;

        for (int i = 0; i < 200; i++)
            assert intCache.putx(i, i);

        GridCache<Integer, Integer> namedCache = ((IgniteKernal)grid(0)).cache(CACHE);

        for (int i = 0; i < 200; i++)
            assert namedCache.putx(i, i);
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
        ((IgniteKernal)grid(0)).cache(null).put(new GridCacheInternalKeyImpl("LONG"), new GridCacheAtomicLongValue(0));

        try {
            Collection<GridCacheSqlMetadata> metas =
                ((GridCacheQueriesEx<?, ?>)((IgniteKernal)grid(0)).cache(null).queries()).sqlMetadata();

            assert metas != null;
            assertEquals("Invalid meta: " + metas, 3, metas.size());

            boolean wasNull = false;
            boolean wasNamed = false;
            boolean wasEmpty = false;

            for (GridCacheSqlMetadata meta : metas) {
                if (meta.cacheName() == null) {
                    Collection<String> types = meta.types();

                    assert types != null;
                    assert types.size() == 4;
                    assert types.contains("Person");
                    assert types.contains("Organization");
                    assert types.contains("String");
                    assert types.contains("Integer");

                    assert CacheAffinityKey.class.getName().equals(meta.keyClass("Person"));
                    assert String.class.getName().equals(meta.keyClass("Organization"));
                    assert String.class.getName().equals(meta.keyClass("String"));

                    assert Person.class.getName().equals(meta.valueClass("Person"));
                    assert Organization.class.getName().equals(meta.valueClass("Organization"));
                    assert String.class.getName().equals(meta.valueClass("String"));

                    Map<String, String> fields = meta.fields("Person");

                    assert fields != null;
                    assert fields.size() == 5;
                    assert CacheAffinityKey.class.getName().equals(fields.get("_KEY"));
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
            }

            assert wasNull;
            assert wasNamed;
            assert wasEmpty;
        }
        finally {
            ((IgniteKernal)grid(0)).cache(null).removex(new GridCacheInternalKeyImpl("LONG"));
        }
    }

    /** @throws Exception If failed. */
    public void testExecute() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery(
            "select _KEY, name, age from Person");

        CacheQueryFuture<List<?>> fut = qry.execute();

        assert metadata(fut) == null;

        List<List<?>> res = new ArrayList<>(fut.get());

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
                assert new CacheAffinityKey<>("p1", "o1").equals(row.get(0));
                assert "John White".equals(row.get(1));
                assert row.get(2).equals(25);
            }
            else if (cnt == 1) {
                assert new CacheAffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
            }
            if (cnt == 2) {
                assert new CacheAffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
            }

            cnt++;
        }

        assert cnt == 3;
    }

    /** @throws Exception If failed. */
    public void testExecuteWithArguments() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery(
            "select _KEY, name, age from Person where age > ?");

        CacheQueryFuture<List<?>> fut = qry.execute(30);

        assert metadata(fut) == null;

        List<List<?>> res = new ArrayList<>(fut.get());

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
                assert new CacheAffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
            }
            else if (cnt == 1) {
                assert new CacheAffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
            }

            cnt++;
        }

        assert cnt == 2;
    }

    /** @throws Exception If failed. */
    public void testExecuteWithMetaData() throws Exception {
        CacheQuery<List<?>> qry = ((GridCacheQueriesEx<?, ?>)((IgniteKernal)grid(0)).cache(null).queries()).createSqlFieldsQuery(
            "select p._KEY, p.name, p.age, o.name " +
                "from Person p, Organization o where p.orgId = o.id",
            true);

        CacheQueryFuture<List<?>> fut = qry.execute();

        List<GridQueryFieldMetadata> meta = metadata(fut);

        assert meta != null;
        assert meta.size() == 4;

        Iterator<GridQueryFieldMetadata> metaIt = meta.iterator();

        assert metaIt != null;
        assert metaIt.hasNext();

        GridQueryFieldMetadata field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());
        assert Object.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "AGE".equals(field.fieldName());
        assert Integer.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        assert !metaIt.hasNext();

        List<List<?>> res = new ArrayList<>(fut.get());

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
                assert new CacheAffinityKey<>("p1", "o1").equals(row.get(0));
                assert "John White".equals(row.get(1));
                assert row.get(2).equals(25);
                assert "A".equals(row.get(3));
            }
            else if (cnt == 1) {
                assert new CacheAffinityKey<>("p2", "o1").equals(row.get(0));
                assert "Joe Black".equals(row.get(1));
                assert row.get(2).equals(35);
                assert "A".equals(row.get(3));
            }
            if (cnt == 2) {
                assert new CacheAffinityKey<>("p3", "o2").equals(row.get(0));
                assert "Mike Green".equals(row.get(1));
                assert row.get(2).equals(40);
                assert "B".equals(row.get(3));
            }

            cnt++;
        }

        assert cnt == 3;
    }

    /** @throws Exception If failed. */
    public void testSelectAllJoined() throws Exception {
        CacheQuery<List<?>> qry = ((GridCacheQueriesEx<?, ?>)((IgniteKernal)grid(0)).cache(null).queries()).createSqlFieldsQuery(
            "select * from Person p, Organization o where p.orgId = o.id",
            true);

        CacheQueryFuture<List<?>> fut = qry.execute();

        List<GridQueryFieldMetadata> meta = metadata(fut);

        assert meta != null;
        assert meta.size() == 9;

        Iterator<GridQueryFieldMetadata> metaIt = meta.iterator();

        assert metaIt != null;
        assert metaIt.hasNext();

        GridQueryFieldMetadata field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());
        assert Object.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "_VAL".equals(field.fieldName());
        assert Object.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "AGE".equals(field.fieldName());
        assert Integer.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "ORGID".equals(field.fieldName());
        assert Integer.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName()) : field.fieldTypeName();

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "_VAL".equals(field.fieldName());
        assert Object.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "ID".equals(field.fieldName());
        assert Integer.class.getName().equals(field.fieldTypeName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "ORGANIZATION".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        assert !metaIt.hasNext();

        List<List<?>> res = new ArrayList<>(fut.get());

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
                assert new CacheAffinityKey<>("p1", "o1").equals(row.get(0));
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
                assert new CacheAffinityKey<>("p2", "o1").equals(row.get(0));
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
                assert new CacheAffinityKey<>("p3", "o2").equals(row.get(0));
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
        CacheQuery<List<?>> qry = ((GridCacheQueriesEx<?, ?>)((IgniteKernal)grid(0)).cache(null).queries()).createSqlFieldsQuery(
            "select name from Person where age = 0", true);

        CacheQueryFuture<List<?>> fut = qry.execute();

        assert fut != null;

        List<GridQueryFieldMetadata> meta = metadata(fut);

        assert meta != null;
        assert meta.size() == 1;

        GridQueryFieldMetadata field = F.first(meta);

        assert field != null;
        assert "PUBLIC".equals(field.schemaName());
        assert "PERSON".equals(field.typeName());
        assert "NAME".equals(field.fieldName());
        assert String.class.getName().equals(field.fieldTypeName());

        Collection<List<?>> res = fut.get();

        assert res != null;
        assert res.isEmpty();
    }

    /** @throws Exception If failed. */
    public void testQueryString() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery("select * from String");

        Collection<List<?>> res = qry.execute().get();

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
        CacheQuery<List<?>> qry = ((GridCacheQueriesEx<?, ?>)((IgniteKernal)grid(0)).cache(null).queries()).createSqlFieldsQuery(
            "select i._KEY, i._VAL, j._KEY, j._VAL from Integer i join Integer j where i._VAL >= 100", true)
            .projection(grid(0).cluster());

        CacheQueryFuture<List<?>> fut = qry.execute();

        List<GridQueryFieldMetadata> meta = metadata(fut);

        assert meta != null;
        assert meta.size() == 4;

        Iterator<GridQueryFieldMetadata> metaIt = meta.iterator();

        assert metaIt.hasNext();

        GridQueryFieldMetadata field = metaIt.next();

        assert field != null;
        assert "INTEGER".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "INTEGER".equals(field.typeName());
        assert "_VAL".equals(field.fieldName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "INTEGER".equals(field.typeName());
        assert "_KEY".equals(field.fieldName());

        assert metaIt.hasNext();

        field = metaIt.next();

        assert field != null;
        assert "INTEGER".equals(field.typeName());
        assert "_VAL".equals(field.fieldName());

        assert !metaIt.hasNext();

        Collection<List<?>> res = fut.get();

        assert res != null;

        if (cacheMode() == LOCAL)
            assert res.size() == 20000;
        else if (cacheMode() == REPLICATED)
            assert res.size() == 60000;
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
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery("select * from Integer");

        qry.pageSize(20);

        List<List<?>> res = new ArrayList<>(qry.execute().get());

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
        GridCache<Integer, Integer> cache = ((IgniteKernal)grid(0)).cache(CACHE);

        for (int i = 0; i < 200; i++)
            assert cache.putx(i, i);

        CacheQuery<List<?>> qry =
            cache.queries().createSqlFieldsQuery("select * from Integer").projection(grid(0).cluster());

        Collection<List<?>> res = qry.execute().get();

        assert res != null;
        assert res.size() == (cacheMode() == REPLICATED ? 200 * gridCount() : 200);
    }

    /** @throws Exception If failed. */
    public void _testNoPrimitives() throws Exception { // TODO
        GridCache<Object, Object> cache = ((IgniteKernal)grid(0)).cache(CACHE_NO_PRIMITIVES);

        assert cache.putx("key", "val");

        Collection<GridCacheSqlMetadata> metas = ((GridCacheQueriesEx<?, ?>)cache.queries()).sqlMetadata();

        assertEquals(1, metas.size());

        assert F.first(metas).types().isEmpty() : "Non empty types: " + F.first(metas).types();

        CacheQuery<List<?>> qry = cache.queries().createSqlFieldsQuery("select * from String");

        assert qry.execute().get().isEmpty();

        cache.removeAll();
    }

    /** @throws Exception If failed. */
    public void _testComplexKeys() throws Exception { // TODO
        GridCache<PersonKey, Person> cache = ((IgniteKernal)grid(0)).cache(CACHE_COMPLEX_KEYS);

        UUID id = UUID.randomUUID();

        PersonKey key = new PersonKey(id);
        Person val = new Person("John", 20, 1);

        assert cache.putx(key, val);

        Collection<GridCacheSqlMetadata> metas = ((GridCacheQueriesEx<?, ?>)cache.queries()).sqlMetadata();

        assertEquals(1, metas.size());

        GridCacheSqlMetadata meta = F.first(metas);

        assertEquals(CACHE_COMPLEX_KEYS, meta.cacheName());

        Collection<String> types = meta.types();

        assertEquals(1, types.size());
        assert types.contains("Person");

        assertEquals(PersonKey.class.getName(), meta.keyClass("Person"));
        assertEquals(Person.class.getName(), meta.valueClass("Person"));

        Map<String, String> fields = meta.fields("Person");

        assertEquals(6, fields.size());

        int cnt = 0;

        for (Map.Entry<String, String> e : fields.entrySet()) {
            if (cnt == 0) {
                assertEquals("_KEY", e.getKey());
                assertEquals(PersonKey.class.getName(), e.getValue());
            }
            else if (cnt == 1) {
                assertEquals("_VAL", e.getKey());
                assertEquals(Person.class.getName(), e.getValue());
            }
            else if (cnt == 2) {
                assertEquals("ID", e.getKey());
                assertEquals(UUID.class.getName(), e.getValue());
            }
            else if (cnt == 3) {
                assertEquals("NAME", e.getKey());
                assertEquals(String.class.getName(), e.getValue());
            }
            else if (cnt == 4) {
                assertEquals("AGE", e.getKey());
                assertEquals(int.class.getName(), e.getValue());
            }
            else if (cnt == 5) {
                assertEquals("ORGID", e.getKey());
                assertEquals(int.class.getName(), e.getValue());
            }

            cnt++;
        }

        Collection<List<?>> res = cache.queries().createSqlFieldsQuery("select * from Person").execute().get();

        assertEquals(1, res.size());

        for (Collection<?> row : res) {
            cnt = 0;

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
        CacheQuery<List<?>> q = ((IgniteKernal)grid(0)).cache(cacheName).queries().createSqlFieldsQuery("select _key, _val from " +
            "Integer")
            .projection(grid(0).cluster());

        q.pageSize(10);
        q.keepAll(false);

        CacheQueryFuture<List<?>> f = q.execute();

        int cnt = 0;

        List<?> row;

        while ((row = f.next()) != null) {
            assertEquals(2, row.size());
            assertEquals(row.get(0), row.get(1));
            assertTrue((Integer)row.get(0) >= 0 && (Integer)row.get(0) < 200);

            cnt++;
        }

        int size = cacheMode() == REPLICATED ? 200 * gridCount() : 200;

        assertEquals(size, cnt);

        assertTrue(f.isDone());

        if (cacheMode() != LOCAL)
            assertTrue(f.get().size() < size);
    }

    /** @throws Exception If failed. */
    public void testPaginationIteratorKeepAll() throws Exception {
        CacheQuery<List<?>> q = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery(
            "select _key, _val from Integer");

        q.pageSize(10);
        q.keepAll(true);

        CacheQueryFuture<List<?>> f = q.execute();

        int cnt = 0;

        List<?> row;

        while ((row = f.next()) != null) {
            assertEquals(2, row.size());
            assertEquals(row.get(0), row.get(1));
            assertTrue((Integer)row.get(0) >= 0 && (Integer)row.get(0) < 200);

            cnt++;
        }

        int size = 200;

        assertEquals(size, cnt);

        assertTrue(f.isDone());

        List<List<?>> list = new ArrayList<>(f.get());

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
        CacheQuery<List<?>> q = ((IgniteKernal)grid(0)).cache(cacheName).queries().createSqlFieldsQuery("select _key, _val from " +
            "Integer");

        q.pageSize(10);
        q.keepAll(true);

        CacheQueryFuture<List<?>> f = q.execute();

        List<List<?>> list = new ArrayList<>(f.get());

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
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).cache(null).queries().createSqlFieldsQuery("select name, " +
            "age from Person where age = 25");

        List<?> res = F.first(qry.execute().get());

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
     * @param fut Query future.
     * @return Metadata.
     * @throws IgniteCheckedException In case of error.
     */
    private List<GridQueryFieldMetadata> metadata(CacheQueryFuture<List<?>> fut) throws IgniteCheckedException {
        assert fut != null;

        return ((GridCacheQueryMetadataAware)fut).metadata().get();
    }

    /**
     */
    private static class RemoteSumReducerFactory implements C1<Object[], IgniteReducer<List<Object>, Integer>> {
        /** {@inheritDoc} */
        @Override public IgniteReducer<List<Object>, Integer> apply(Object[] args) {
            return new RemoteSumReducer();
        }
    }

    /**
     */
    private static class RemoteSumReducer implements R1<List<Object>, Integer> {
        /** */
        private int sum;

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable List<Object> row) {
            if (row != null)
                sum += (int)row.get(0);

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return sum;
        }
    }

    /**
     */
    private static class LocalSumReducerFactory implements C1<Object[], IgniteReducer<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public IgniteReducer<Integer, Integer> apply(Object[] args) {
            return new LocalSumReducer();
        }
    }

    /**
     */
    private static class LocalSumReducer implements R1<Integer, Integer> {
        /** */
        private int sum;

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Integer val) {
            if (val != null)
                sum += val;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return sum;
        }
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
