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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Geo-indexing test.
 */
public abstract class H2IndexingAbstractGeoSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int CNT = 100;

    /** */
    private static final long DUR = 60000L;

    /** Number of generated samples. */
    public static final int ENEMYCAMP_SAMPLES_COUNT = 500;

    /** Number of generated samples. */
    public static final int ENEMY_SAMPLES_COUNT = 1000;

    /** */
    private static final int QRY_PARALLELISM_LVL = 7;

    /** Segmented index flag. */
    private final boolean segmented;

    /**
     * Constructor.
     *
     * @param segmented Segmented index flag.
     */
    protected H2IndexingAbstractGeoSelfTest(boolean segmented) {
        this.segmented = segmented;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DUR * 3;
    }

    /**
     * Create cache.
     *
     * @param name Name.
     * @param partitioned Partitioned flag.
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected <K, V> IgniteCache<K, V> createCache(String name, boolean partitioned, Class<?> keyCls, Class<?> valCls)
        throws Exception {
        return createCache(name, partitioned, keyCls, valCls, false);
    }

    /**
     * Create cache.
     *
     * @param name Name.
     * @param partitioned Partitioned flag.
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param dynamicIdx Whether index should be created dynamically.
     * @return Cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected <K, V> IgniteCache<K, V> createCache(String name, boolean partitioned, Class<?> keyCls, Class<?> valCls,
        boolean dynamicIdx) throws Exception {
        CacheConfiguration<K, V> ccfg = cacheConfig(name, partitioned, keyCls, valCls);

        if (dynamicIdx) {
            Collection<QueryEntity> entities = ccfg.getQueryEntities();

            assertEquals(1, entities.size());

            QueryEntity entity = entities.iterator().next();

            Collection<QueryIndex> idxs = new ArrayList<>(entity.getIndexes());

            entity.setIndexes(null);

            grid(0).context().cache().dynamicStartSqlCache(ccfg).get();

            IgniteCache<K, V> cache = grid(0).cache(name);

            // Process indexes dynamically.
            for (QueryIndex idx : idxs) {
                QueryEntity normalEntity = QueryUtils.normalizeQueryEntity(entity, ccfg.isSqlEscapeAll());

                createDynamicIndex(cache, normalEntity, idx);
            }

            return cache;
        }
        else
            return grid(0).getOrCreateCache(ccfg);
    }

    /**
     * Create dynamic index.
     *
     * @param cache Cache.
     * @param entity Entity.
     * @param idx Index.
     * @throws Exception If failed.
     */
    private void createDynamicIndex(IgniteCache cache, QueryEntity entity, QueryIndex idx) throws Exception {
        boolean spatial = idx.getIndexType() == QueryIndexType.GEOSPATIAL;

        GridStringBuilder sb = new SB("CREATE ")
            .a(spatial ? "SPATIAL " : "")
            .a("INDEX ")
            .a("\"" + idx.getName() + "\"")
            .a(" ON ")
            .a(QueryUtils.tableName(entity))
            .a(" (");

        boolean first = true;

        for (Map.Entry<String, Boolean> fieldEntry : idx.getFields().entrySet()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            String name = fieldEntry.getKey();
            boolean asc = fieldEntry.getValue();

            sb.a("\"" + name + "\"").a(" ").a(asc ? "ASC" : "DESC");
        }

        sb.a(')');

        String sql = sb.toString();

        cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> cacheConfig(@NotNull String name, boolean partitioned, Class<?> keyCls,
        Class<?> valCls) throws Exception {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<K, V>(name)
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setIndexedTypes(keyCls, valCls);

        if (segmented)
            ccfg.setQueryParallelism(partitioned ? QRY_PARALLELISM_LVL : 1);

        return ccfg;
    }

    /**
     * Destroy cache.
     *
     * @param cache Cache.
     * @param grid Node.
     * @param dynamic Dynamic flag.
     */
    private static void destroy(IgniteCache cache, IgniteEx grid, boolean dynamic) {
        if (!dynamic)
            cache.destroy();
        else
            grid.context().cache().dynamicDestroyCache(cache.getName(), true, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveGeometry() throws Exception {
        IgniteCache<Long, Geometry> cache = createCache("geom", true, Long.class, Geometry.class);

        try {
            WKTReader r = new WKTReader();

            for (long i = 0; i < 100; i++)
                cache.put(i, r.read("POINT(" + i + " " + i + ")"));

            String plan = cache.query(new SqlFieldsQuery("explain select _key from Geometry where _val && ?")
                .setArgs(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))")).setLocal(true))
                .getAll().get(0).get(0).toString().toLowerCase();

            assertTrue("__ explain: " + plan, plan.contains("_val_idx"));
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * Test geo-index (static).
     *
     * @throws Exception If failed.
     */
    public void testGeo() throws Exception {
        checkGeo(false);
    }

    /**
     * Test geo-index (dynamic).
     *
     * @throws Exception If failed.
     */
    public void testGeoDynamic() throws Exception {
        checkGeo(true);
    }

    /**
     * Check geo-index (dynamic).
     *
     * @param dynamic Whether index should be created dynamically.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private void checkGeo(boolean dynamic) throws Exception {
        IgniteCache<Integer, EnemyCamp> cache = createCache("camp", true, Integer.class, EnemyCamp.class, dynamic);

        try {
            WKTReader r = new WKTReader();

            cache.getAndPut(0, new EnemyCamp(r.read("POINT(25 75)"), "A"));
            cache.getAndPut(1, new EnemyCamp(r.read("POINT(70 70)"), "B"));
            cache.getAndPut(2, new EnemyCamp(r.read("POINT(70 30)"), "C"));
            cache.getAndPut(3, new EnemyCamp(r.read("POINT(75 25)"), "D"));

            SqlQuery<Integer, EnemyCamp> qry = new SqlQuery(EnemyCamp.class, "coords && ?");

            Collection<Cache.Entry<Integer, EnemyCamp>> res = cache.query(
                qry.setArgs(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))"))).getAll();

            checkPoints(res, "A");

            res = cache.query(
                qry.setArgs(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))"))).getAll();

            checkPoints(res, "C", "D");

            // Move B to the first polygon.
            cache.getAndPut(1, new EnemyCamp(r.read("POINT(20 75)"), "B"));

            res = cache.query(
                qry.setArgs(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))"))).getAll();

            checkPoints(res, "A", "B");

            // Move B to the second polygon.
            cache.getAndPut(1, new EnemyCamp(r.read("POINT(30 30)"), "B"));

            res = cache.query(
                qry.setArgs(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))"))).getAll();

            checkPoints(res, "B", "C", "D");

            // Remove B.
            cache.getAndRemove(1);

            res = cache.query(
                qry.setArgs(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))"))).getAll();

            checkPoints(res, "A");

            res = cache.query(
                qry.setArgs(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))"))).getAll();

            checkPoints(res, "C", "D");

            // Check explain request.
            String plan = cache.query(new SqlFieldsQuery("explain select * from EnemyCamp " +
                "where coords && 'POINT(25 75)'")).getAll().get(0).get(0).toString().toLowerCase();

            assertTrue("__ explain: " + plan, plan.contains("coords_idx"));

            if (dynamic)
                cache.query(new SqlFieldsQuery("DROP INDEX \"EnemyCamp_coords_idx\"")).getAll();
        }
        finally {
            destroy(cache, grid(0), dynamic);
        }
    }

    /**
     * Test geo indexing multithreaded.
     *
     * @throws Exception If failed.
     */
    public void testGeoMultithreaded() throws Exception {
        checkGeoMultithreaded(false);
    }

    /**
     * Test geo indexing multithreaded with dynamic index creation.
     *
     * @throws Exception If failed.
     */
    public void testGeoMultithreadedDynamic() throws Exception {
        checkGeoMultithreaded(true);
    }

    /**
     * Check geo indexing multithreaded with dynamic index creation.
     *
     * @param dynamic Whether index should be created dynamically.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkGeoMultithreaded(boolean dynamic) throws Exception {
        final IgniteCache<Integer, EnemyCamp> cache1 =
            createCache("camp", true, Integer.class, EnemyCamp.class, dynamic);

        final IgniteCache<Integer, EnemyCamp> cache2 = grid(1).cache("camp");
        final IgniteCache<Integer, EnemyCamp> cache3 = grid(2).cache("camp");

        try {
            final String[] points = new String[CNT];

            WKTReader r = new WKTReader();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int idx = 0; idx < CNT; idx++) {
                int x = rnd.nextInt(1, 100);
                int y = rnd.nextInt(1, 100);

                cache1.getAndPut(idx, new EnemyCamp(r.read("POINT(" + x + " " + y + ")"), Integer.toString(idx)));

                points[idx] = Integer.toString(idx);
            }

            Thread.sleep(200);

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Exception> err = new AtomicReference<>();

            IgniteInternalFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    WKTReader r = new WKTReader();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        int cacheIdx = rnd.nextInt(0, 3);

                        IgniteCache<Integer, EnemyCamp> cache = cacheIdx == 0 ? cache1 : cacheIdx == 1 ? cache2 : cache3;

                        int idx = rnd.nextInt(CNT);
                        int x = rnd.nextInt(1, 100);
                        int y = rnd.nextInt(1, 100);

                        cache.getAndPut(idx, new EnemyCamp(r.read("POINT(" + x + " " + y + ")"), Integer.toString(idx)));

                        U.sleep(50);
                    }

                    return null;
                }
            }, Runtime.getRuntime().availableProcessors(), "put-thread");

            IgniteInternalFuture<?> qryFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    WKTReader r = new WKTReader();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        try {
                            int cacheIdx = rnd.nextInt(0, 3);

                            IgniteCache<Integer, EnemyCamp> cache = cacheIdx == 0 ? cache1 : cacheIdx == 1 ? cache2 : cache3;

                            SqlQuery<Integer, EnemyCamp> qry = new SqlQuery<>(
                                EnemyCamp.class, "coords && ?");

                            Collection<Cache.Entry<Integer, EnemyCamp>> res = cache.query(qry.setArgs(
                                r.read("POLYGON((0 0, 0 100, 100 100, 100 0, 0 0))"))).getAll();

                            checkPoints(res, points);

                            U.sleep(5);
                        }
                        catch (Exception e) {
                            err.set(e);

                            stop.set(true);

                            break;
                        }
                    }

                    return null;
                }
            }, 4, "qry-thread");

            U.sleep(6000L);

            stop.set(true);

            putFut.get();
            qryFut.get();

            Exception err0 = err.get();

            if (err0 != null)
                throw err0;
        }
        finally {
            destroy(cache1, grid(0), dynamic);
        }
    }

    /**
     * Check whether result contains all required points.
     *
     * @param res Result.
     * @param points Expected points.
     */
    private void checkPoints(Collection<Cache.Entry<Integer, EnemyCamp>> res, String... points) {
        Set<String> set = new HashSet<>(Arrays.asList(points));

        assertEquals(set.size(), res.size());

        for (Cache.Entry<Integer, EnemyCamp> e : res)
            assertTrue(set.remove(e.getValue().name));
    }

    /**
     * Test segmented geo-index join on PARTITIONED cache.
     *
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoinPartitioned() throws Exception {
        checkSegmentedGeoIndexJoin(true, false);
    }

    /**
     * Test segmented geo-index join on PARTITIONED cache with dynamically created index.
     *
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoinPartitionedDynamic() throws Exception {
        checkSegmentedGeoIndexJoin(true, true);
    }

    /**
     * Test segmented geo-index join on REPLICATED cache.
     *
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoinReplicated() throws Exception {
        checkSegmentedGeoIndexJoin(false, false);
    }

    /**
     * Test segmented geo-index join on REPLICATED cache with dynamically created index.
     *
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoinReplicatedDynamic() throws Exception {
        checkSegmentedGeoIndexJoin(false, true);
    }

    /**
     * Check segmented geo-index join.
     *
     * @param partitioned Partitioned flag.
     * @param dynamic Whether index should be created dynamically.
     * @throws Exception If failed.
     */
    private void checkSegmentedGeoIndexJoin(boolean partitioned, boolean dynamic) throws Exception {
        IgniteCache<Integer, Enemy> c1 = createCache("enemy", true, Integer.class, Enemy.class);
        IgniteCache<Integer, EnemyCamp> c2 = createCache("camp", partitioned, Integer.class, EnemyCamp.class, dynamic);

        try {
            final ThreadLocalRandom rnd = ThreadLocalRandom.current();

            WKTReader r = new WKTReader();

            for (int i = 0; i < ENEMYCAMP_SAMPLES_COUNT; i++) {
                final String point = String.format("POINT(%d %d)", rnd.nextInt(100), rnd.nextInt(100));

                c2.put(i, new EnemyCamp(r.read(point), "camp-" + i));
            }

            for (int i = 0; i < ENEMY_SAMPLES_COUNT; i++) {
                int campID = 30 + rnd.nextInt(ENEMYCAMP_SAMPLES_COUNT + 10);

                c1.put(i, new Enemy(campID, "enemy-" + i));
            }

            checkDistributedQuery();

            checkLocalQuery();
        }
        finally {
            destroy(c1, grid(0), dynamic);
            destroy(c2, grid(0), dynamic);
        }
    }

    /**
     * Check distributed query.
     *
     * @throws ParseException If failed.
     */
    private void checkDistributedQuery() throws ParseException {
        IgniteCache<Integer, Enemy> c1 = grid(0).cache("enemy");
        IgniteCache<Integer, EnemyCamp> c2 = grid(0).cache("camp");

        final Geometry lethalArea = new WKTReader().read("POLYGON((30 30, 30 70, 70 70, 70 30, 30 30))");

        int expectedEnemies = 0;

        for (Cache.Entry<Integer, Enemy> e : c1) {
            final Integer campID = e.getValue().campId;

            if (30 <= campID && campID < ENEMYCAMP_SAMPLES_COUNT) {
                final EnemyCamp camp = c2.get(campID);

                if (lethalArea.covers(camp.coords))
                    expectedEnemies++;
            }
        }

        final SqlFieldsQuery query = new SqlFieldsQuery("select e._val, c._val from \"enemy\".Enemy e, " +
            "\"camp\".EnemyCamp c where e.campId = c._key and c.coords && ?").setArgs(lethalArea);

        List<List<?>> result = c1.query(query.setDistributedJoins(true)).getAll();

        assertEquals(expectedEnemies, result.size());
    }

    /**
     * Check local query.
     *
     * @throws ParseException If failed.
     */
    private void checkLocalQuery() throws ParseException {
        IgniteCache<Integer, Enemy> c1 = grid(0).cache("enemy");
        IgniteCache<Integer, EnemyCamp> c2 = grid(0).cache("camp");

        final Geometry lethalArea = new WKTReader().read("POLYGON((30 30, 30 70, 70 70, 70 30, 30 30))");

        Set<Integer> localCampsIDs = new HashSet<>();

        for(Cache.Entry<Integer, EnemyCamp> e : c2.localEntries())
            localCampsIDs.add(e.getKey());

        int expectedEnemies = 0;

        for (Cache.Entry<Integer, Enemy> e : c1.localEntries()) {
            final Integer campID = e.getValue().campId;

            if (localCampsIDs.contains(campID)) {
                final EnemyCamp camp = c2.get(campID);

                if (lethalArea.covers(camp.coords))
                    expectedEnemies++;
            }
        }

        final SqlFieldsQuery query = new SqlFieldsQuery("select e._val, c._val from \"enemy\".Enemy e, " +
            "\"camp\".EnemyCamp c where e.campId = c._key and c.coords && ?").setArgs(lethalArea);

        List<List<?>> result = c1.query(query.setLocal(true)).getAll();

        assertEquals(expectedEnemies, result.size());
    }

    /**
     *
     */
    private static class Enemy {
        /** */
        @QuerySqlField(index = true)
        int campId;

        /** */
        @QuerySqlField
        String name;

        /**
         * @param campId Camp ID.
         * @param name Name.
         */
        public Enemy(int campId, String name) {
            this.campId = campId;
            this.name = name;
        }
    }

    /**
     *
     */
    protected static class EnemyCamp implements Serializable {
        /** */
        @QuerySqlField(index = true)
        Geometry coords;

        /** */
        @QuerySqlField
        private String name;

        /**
         * @param coords Coordinates.
         * @param name Name.
         */
        EnemyCamp(Geometry coords, String name) {
            this.coords = coords;
            this.name = name;
        }
    }
}