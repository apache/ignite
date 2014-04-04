/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridH2IndexingSpiGeoSelfTest extends GridCacheAbstractSelfTest {
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

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testGeo() throws Exception {
        GridCache<Integer, EnemyCamp> cache = grid(0).cache(null);

        WKTReader r = new WKTReader();

        cache.put(0, new EnemyCamp(r.read("POINT(25 75)"), "A"));
        cache.put(1, new EnemyCamp(r.read("POINT(70 70)"), "B"));
        cache.put(2, new EnemyCamp(r.read("POINT(70 30)"), "C"));
        cache.put(3, new EnemyCamp(r.read("POINT(75 25)"), "D"));

        GridCacheQuery<Map.Entry<Integer, EnemyCamp>> qry = cache.queries().createSqlQuery(EnemyCamp.class,
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
        final GridCache<Integer, EnemyCamp> cache1 = grid(0).cache(null);
        final GridCache<Integer, EnemyCamp> cache2 = grid(1).cache(null);
        final GridCache<Integer, EnemyCamp> cache3 = grid(2).cache(null);

        final String[] points = new String[CNT];

        WKTReader r = new WKTReader();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int idx = 0; idx < CNT; idx++) {
            int x = rnd.nextInt(1, 100);
            int y = rnd.nextInt(1, 100);

            cache1.put(idx, new EnemyCamp(r.read("POINT(" + x + " " + y + ")"), Integer.toString(idx)));

            points[idx] = Integer.toString(idx);
        }

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Exception> err = new AtomicReference<>();

        GridFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
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

        GridFuture<?> qryFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                WKTReader r = new WKTReader();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    try {
                        int cacheIdx = rnd.nextInt(0, 3);

                        GridCache<Integer, EnemyCamp> cache = cacheIdx == 0 ? cache1 : cacheIdx == 1 ? cache2 : cache3;

                        GridCacheQuery<Map.Entry<Integer, EnemyCamp>> qry = cache.queries().createSqlQuery(
                            EnemyCamp.class, "coords && ?");

                        Collection<Map.Entry<Integer, EnemyCamp>> res = qry.execute(
                            r.read("POLYGON((0 0, 0 100, 100 100, 100 0, 0 0))")).get();

                        checkPoints(res, points);

                        U.sleep(50);
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
        @GridCacheQuerySqlField(index = true)
        private Geometry coords;

        /** */
        @GridCacheQuerySqlField
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
