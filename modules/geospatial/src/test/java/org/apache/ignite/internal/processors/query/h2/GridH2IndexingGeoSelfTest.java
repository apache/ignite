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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridH2IndexingGeoSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int CNT = 100;

    /** */
    private static final long DUR = 60000L;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DUR * 3;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[]{
            Integer.class, EnemyCamp.class
        };
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testGeo() throws Exception {
        GridCache<Integer, EnemyCamp> cache = ((IgniteKernal)grid(0)).getCache(null);

        WKTReader r = new WKTReader();

        cache.put(0, new EnemyCamp(r.read("POINT(25 75)"), "A"));
        cache.put(1, new EnemyCamp(r.read("POINT(70 70)"), "B"));
        cache.put(2, new EnemyCamp(r.read("POINT(70 30)"), "C"));
        cache.put(3, new EnemyCamp(r.read("POINT(75 25)"), "D"));

        CacheQuery<Map.Entry<Integer, EnemyCamp>> qry = cache.queries().createSqlQuery(EnemyCamp.class,
            "coords && ?");

        Collection<Map.Entry<Integer, EnemyCamp>> res = qry.execute(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))"))
            .get();

        checkPoints(res, "A");

        res = qry.execute(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))")).get();

        checkPoints(res, "C", "D");

        // Move B to the first polygon.
        cache.put(1, new EnemyCamp(r.read("POINT(20 75)"), "B"));

        res = qry.execute(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))")).get();

        checkPoints(res, "A", "B");

        // Move B to the second polygon.
        cache.put(1, new EnemyCamp(r.read("POINT(30 30)"), "B"));

        res = qry.execute(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))")).get();

        checkPoints(res, "B", "C", "D");

        // Remove B.
        cache.remove(1);

        res = qry.execute(r.read("POLYGON((5 70, 5 80, 30 80, 30 70, 5 70))")).get();

        checkPoints(res, "A");

        res = qry.execute(r.read("POLYGON((10 5, 10 35, 70 30, 75 25, 10 5))")).get();

        checkPoints(res, "C", "D");

        // Check explaint request.
        assertTrue(F.first(cache.queries().createSqlFieldsQuery("explain select * from EnemyCamp " +
            "where coords && 'POINT(25 75)'").execute().get()).get(0).toString().contains("coords_idx"));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testGeoMultithreaded() throws Exception {
        final GridCache<Integer, EnemyCamp> cache1 = ((IgniteKernal)grid(0)).getCache(null);
        final GridCache<Integer, EnemyCamp> cache2 = ((IgniteKernal)grid(1)).getCache(null);
        final GridCache<Integer, EnemyCamp> cache3 = ((IgniteKernal)grid(2)).getCache(null);

        final String[] points = new String[CNT];

        WKTReader r = new WKTReader();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int idx = 0; idx < CNT; idx++) {
            int x = rnd.nextInt(1, 100);
            int y = rnd.nextInt(1, 100);

            cache1.put(idx, new EnemyCamp(r.read("POINT(" + x + " " + y + ")"), Integer.toString(idx)));

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

                    GridCache<Integer, EnemyCamp> cache = cacheIdx == 0 ? cache1 : cacheIdx == 1 ? cache2 : cache3;

                    int idx = rnd.nextInt(CNT);
                    int x = rnd.nextInt(1, 100);
                    int y = rnd.nextInt(1, 100);

                    cache.put(idx, new EnemyCamp(r.read("POINT(" + x + " " + y + ")"), Integer.toString(idx)));

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

                        GridCache<Integer, EnemyCamp> cache = cacheIdx == 0 ? cache1 : cacheIdx == 1 ? cache2 : cache3;

                        CacheQuery<Map.Entry<Integer, EnemyCamp>> qry = cache.queries().createSqlQuery(
                            EnemyCamp.class, "coords && ?");

                        Collection<Map.Entry<Integer, EnemyCamp>> res = qry.execute(
                            r.read("POLYGON((0 0, 0 100, 100 100, 100 0, 0 0))")).get();

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

        U.sleep(60000L);

        stop.set(true);

        putFut.get();
        qryFut.get();

        Exception err0 = err.get();

        if (err0 != null)
            throw err0;
    }

    /**
     * Check whether result contains all required points.
     *
     * @param res Result.
     * @param points Expected points.
     */
    private void checkPoints( Collection<Map.Entry<Integer, EnemyCamp>> res, String... points) {
        Set<String> set = new HashSet<>(Arrays.asList(points));

        assertEquals(set.size(), res.size());

        for (Map.Entry<Integer, EnemyCamp> e : res)
            assertTrue(set.remove(e.getValue().name));
    }

    /**
     *
     */
    private static class EnemyCamp implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private Geometry coords;

        /** */
        @QuerySqlField
        private String name;

        /**
         * @param coords Coordinates.
         * @param name Name.
         */
        private EnemyCamp(Geometry coords, String name) {
            this.coords = coords;
            this.name = name;
        }
    }
}
