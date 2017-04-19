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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Geo-indexing test.
 */
public class GridH2IndexingGeoSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int CNT = 100;

    /** */
    private static final long DUR = 60000L;

    /** Number of generated samples. */
    public static final int ENEMYCAMP_SAMPLES_COUNT = 500;

    /** Number of generated samples. */
    public static final int ENEMY_SAMPLES_COUNT = 1000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DUR * 3;
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned,
        Class<?>... idxTypes) throws Exception {
        return new CacheConfiguration<K, V>(name)
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setIndexedTypes(idxTypes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveGeometry() throws Exception {
        IgniteCache<Long, Geometry> cache = grid(0).getOrCreateCache(
            this.<Long, Geometry>cacheConfig("geom", true, Long.class, Geometry.class));

        try {
            WKTReader r = new WKTReader();

            for (long i = 0; i < 100; i++)
                cache.put(i, r.read("POINT(" + i + " " + i + ")"));

            List<List<?>> res = cache.query(new SqlFieldsQuery("explain select _key from Geometry where _val && ?")
                .setArgs(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))")).setLocal(true)).getAll();

            assertTrue("__ explain: " + res, res.get(0).get(0).toString().toLowerCase().contains("_val_idx"));
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testGeo() throws Exception {
        IgniteCache<Integer, EnemyCamp> cache = grid(0).getOrCreateCache(
            this.<Integer, EnemyCamp>cacheConfig("camp", true, Integer.class, EnemyCamp.class));

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

            // Check explaint request.
            assertTrue(F.first(cache.query(new SqlFieldsQuery("explain select * from EnemyCamp " +
                "where coords && 'POINT(25 75)'")).getAll()).get(0).toString().toLowerCase().contains("coords_idx"));
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testGeoMultithreaded() throws Exception {
        final CacheConfiguration<Integer, EnemyCamp> ccfg = cacheConfig("camp", true, Integer.class, EnemyCamp.class);

        final IgniteCache<Integer, EnemyCamp> cache1 = grid(0).getOrCreateCache(ccfg);
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
            cache1.destroy();
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
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoin() throws Exception {
        IgniteCache<Integer, Enemy> c1 = ignite(0).getOrCreateCache(
            this.<Integer, Enemy>cacheConfig("enemy", true, Integer.class, Enemy.class));
        IgniteCache<Integer, EnemyCamp> c2 = ignite(0).getOrCreateCache(
            this.<Integer, EnemyCamp>cacheConfig("camp", true, Integer.class, EnemyCamp.class));

        try {
            fillCache();

            checkDistributedQuery();

            checkLocalQuery();
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /**
     * @throws Exception if fails.
     */
    public void testSegmentedGeoIndexJoin2() throws Exception {
        IgniteCache<Integer, Enemy> c1 = ignite(0).getOrCreateCache(
            this.<Integer, Enemy>cacheConfig("enemy", true, Integer.class, Enemy.class));
        IgniteCache<Integer, EnemyCamp> c2 = ignite(0).getOrCreateCache(
            this.<Integer, EnemyCamp>cacheConfig("camp", false, Integer.class, EnemyCamp.class));

        try {
            fillCache();

            checkDistributedQuery();

            checkLocalQuery();
        }
        finally {
            c1.destroy();
            c2.destroy();
        }
    }

    /** */
    private void checkDistributedQuery() throws ParseException {
        IgniteCache<Integer, Enemy> c1 = ignite(0).cache("enemy");
        IgniteCache<Integer, EnemyCamp> c2 = ignite(0).cache("camp");

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

        final SqlFieldsQuery query = new SqlFieldsQuery("select e._val, c._val from \"enemy\".Enemy e, \"camp\".EnemyCamp c " +
            "where e.campId = c._key and c.coords && ?").setArgs(lethalArea);

        List<List<?>> result = c1.query(query.setDistributedJoins(true)).getAll();

        assertEquals(expectedEnemies, result.size());
    }

    /** */
    private void checkLocalQuery() throws ParseException {
        IgniteCache<Integer, Enemy> c1 = ignite(0).cache("enemy");
        IgniteCache<Integer, EnemyCamp> c2 = ignite(0).cache("camp");

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

        final SqlFieldsQuery query = new SqlFieldsQuery("select e._val, c._val from \"enemy\".Enemy e, \"camp\".EnemyCamp c " +
            "where e.campId = c._key and c.coords && ?").setArgs(lethalArea);

        List<List<?>> result = c1.query(query.setLocal(true)).getAll();

        assertEquals(expectedEnemies, result.size());
    }

    /** */
    private void fillCache() throws ParseException {
        IgniteCache<Integer, Enemy> c1 = ignite(0).cache("enemy");
        IgniteCache<Integer, EnemyCamp> c2 = ignite(0).cache("camp");

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
    }

    /**
     *
     */
    private static class Enemy {
        /** */
        @QuerySqlField
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