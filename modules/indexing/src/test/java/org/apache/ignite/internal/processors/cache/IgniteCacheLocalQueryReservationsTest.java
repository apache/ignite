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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Test for local query partitions reservation.
 */
public class IgniteCacheLocalQueryReservationsTest extends GridCommonAbstractTest {

    /** Cache name */
    private static final String PERSON_CACHE = "person";

    /** Timeout */
    private static final long TIMEOUT_MS = 30_000L;

    /** Cache size. Should be quite enough for a long time of query execution (hundreds of milliseconds).*/
    private static final int CACHE_SIZE = 2_000_000;

    /**
     * Test for the local {@link SqlFieldsQuery} reservations.
     *
     * @throws Exception If failed.
     */
    public void testLocalSqlFieldsQueryReservations() throws Exception {
        Query qry = new SqlFieldsQuery("select name, age from person where age >= 0 or age < 1000000000");

        checkReservations(qry);
    }

    /**
     * Test for the local {@link SqlQuery} reservations.
     *
     * @throws Exception If failed.
     */
    public void testLocalSqlQueryReservations() throws Exception {
        String sql = "age >= 0 or age < 1000000000";

        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, sql);

        checkReservations(qry);
    }

    /**
     * Test for the local {@link SqlFieldsQuery} reservations.
     *
     * @throws Exception If failed.
     */
    public void testLocalSqlFieldsQueryCursorCloseReservations() throws Exception {
        Query qry = new SqlFieldsQuery("select name, age from person where age >= 0 or age < 1000000000");

        checkEagerCursorClosingReservations(qry);
    }

    /**
     * Test for the local {@link SqlQuery} reservations.
     *
     * @throws Exception If failed.
     */
    public void testLocalSqlQueryCursorCloseReservations() throws Exception {
        String sql = "age >= 0 or age < 1000000000";

        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, sql);

        checkEagerCursorClosingReservations(qry);
    }



    /**
     * Checks if a query partitions reservations occurs.
     *
     * @param qry Query for entries counting.
     * @throws Exception If failed.
     */
    private void checkReservations(final Query qry) throws Exception {
        final IgniteEx grid = startGrid(0);

        createCache();

        fillCache();

        qry.setLocal(true);

        final IgniteCache<Integer, Person> cache = grid(0).cache(PERSON_CACHE);

        final List<GridDhtLocalPartition> parts = grid.context().cache().internalCache(PERSON_CACHE).context()
            .topology().localPartitions();

        // 1. Before the cache.query() execution all partitions should be released.
        checkReservationsAfterClosure(false, parts, "Partitions should be released before query.",
            new IgniteClosure<Object, Object>() {
                @Override public Object apply(Object o) {
                    return null; // Do nothing - checking state before the query.
                }
            });

        // 2. When a cursor obtained from the cache.query(), all partitions should be reserved.
        final QueryCursor<List<?>> cursor = checkReservationsAfterClosure(true, parts,
            "Partitions should be reserved when got cursor.",
            new IgniteClosure<Object, QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> apply(Object o) {
                    return (QueryCursor<List<?>>)cache.query(qry);
                }
            });

        // 3. During the obtaining iterator from the Cursor partitions reservations should be released for
        // an SqlQuery and reserved for an SqlFieldsQuery (due to its laziness).
        boolean shouldBeReserved = qry instanceof SqlFieldsQuery;

        final Iterator<List<?>> it = checkReservationsAfterClosure(shouldBeReserved, parts,
            "Partitions should be " + (shouldBeReserved ? "reserved" : "released" ) + " when obtaining iterator.",
            new IgniteClosure<Object, Iterator<List<?>>>() {
                @Override public Iterator<List<?>> apply(Object o) {
                    return cursor.iterator();
                }
            });

        // 4. When iterating over the result, all partitions should be released because entire result set is in the
        // memory now.
        final List<?> res = checkReservationsAfterClosure(false, parts,
            "Partitions should be released when iterating over the result set.",
            new IgniteClosure<Object, List<?>>() {
                @Override public List<?> apply(Object o) {
                    List r = new ArrayList();

                    while (it.hasNext())
                        r.add(it.next());

                    return r;
                }
            });

        assertEquals("Wrong result set size.", CACHE_SIZE, res.size());

        // 5. Should be released after the cursor has been closed.
        checkReservationsAfterClosure(false, parts,
            "Partitions should be released after the cursor has been closed.",
            new IgniteClosure<Object, Object>() {
                @Override public Object apply(Object o) {
                    cursor.close();

                    return null;
                }
            });
    }

    /**
     * Checks if reservations have been released after the cursor
     * has been closed eagerly (before the iterator obtaining)
     *
     * @param qry Query.
     * @throws Exception If failed.
     */
    private void checkEagerCursorClosingReservations(final Query qry) throws Exception {
        final IgniteEx grid = startGrid(0);

        createCache();

        fillCache();

        qry.setLocal(true);

        final IgniteCache<Integer, Person> cache = grid(0).cache(PERSON_CACHE);

        final List<GridDhtLocalPartition> parts = grid.context().cache().internalCache(PERSON_CACHE).context()
            .topology().localPartitions();

        // 1. Before the cache.query() execution all partitions should be released.
        checkReservationsAfterClosure(false, parts, "Partitions should be released before query.",
            new IgniteClosure<Object, Object>() {
                @Override public Object apply(Object o) {
                    return null; // Do nothing - checking state before the query.
                }
            });

        // 2. When a cursor obtained from the cache.query(), all partitions should be reserved.
        final QueryCursor<List<?>> cursor = checkReservationsAfterClosure(true, parts,
            "Partitions should be reserved when got cursor.",
            new IgniteClosure<Object, QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> apply(Object o) {
                    return (QueryCursor<List<?>>)cache.query(qry);
                }
            });

        // 3. Partitions should be released when cursor closed.
        checkReservationsAfterClosure(false, parts,
            "Partitions should be released when cursor is closed.",
            new IgniteClosure<Object, Object>() {
                @Override public Object apply(Object o) {
                    cursor.close();

                    return null;
                }
            });
    }

    /**
     * Checks reservations status after the given closure having been invoked.
     *
     * @param shouldBeReserved Expected reservation status. {@code True} if reserved.
     * @param parts Partitions.
     * @param msg Warning message.
     * @param clo Closure.
     * @param <E> Closure argument parameter.
     * @param <R> Closure return parameter.
     * @return Closure invocation result.
     * @throws Exception If failed.
     */
    private <E, R> R checkReservationsAfterClosure(final boolean shouldBeReserved,
        final List<GridDhtLocalPartition> parts, final String msg, final IgniteClosure<E, R> clo)  throws Exception {
        final CyclicBarrier crd = new CyclicBarrier(2);

        Callable<Boolean> reservationsChecker = new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                // After this barrier closure will be invoked.
                crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

                // Check if the partitions are in the proper state.
                if (shouldBeReserved) {
                    if (!isReserved(parts, msg))
                        return false;
                } else {
                    if (!isReleased(parts, msg))
                        return false;
                }

                // Wait for the end of checking.
                crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

                return true;
            }
        };

        FutureTask<Boolean> reservationsRes = new FutureTask<>(reservationsChecker);

        Thread checkerThread = new Thread(reservationsRes);

        checkerThread.start();

        crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        R res = clo.apply(null);

        crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertTrue("Partitions are not properly " + (shouldBeReserved ? "reserved." : "released."),
            reservationsRes.get());

        return res;
    }

    /**
     * Checks if all partitions are reserved.
     *
     * @param parts Partitions.
     * @param msg Warning message.
     * @return {@code True} if reserved.
     * @throws IgniteInterruptedCheckedException If failed.
     */
    @NotNull private Boolean isReserved(List<GridDhtLocalPartition> parts, final String msg)
        throws IgniteInterruptedCheckedException {
        final Set<GridDhtLocalPartition> partsSet = new HashSet<>(parts);

        return GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Iterator<GridDhtLocalPartition> partsIt = partsSet.iterator(); partsIt.hasNext();) {
                    GridDhtLocalPartition part = partsIt.next();

                    if (part.reservations() == 1)
                        partsIt.remove();
                }

                if (partsSet.isEmpty())
                    return true; // All partitions have been reserved.
                else {
                    log.warning(msg);

                    return false;
                }
            }
        }, TIMEOUT_MS);
    }

    /**
     * Checks if all partitions are released.
     *
     * @param parts Partitions.
     * @param msg Warning message.
     * @return {@code True} if released.
     * @throws IgniteInterruptedCheckedException If failed.
     */
    private boolean isReleased(final List<GridDhtLocalPartition> parts, final String msg)
        throws IgniteInterruptedCheckedException {
        return GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridDhtLocalPartition part : parts) {
                    if (part.reservations() > 0) {
                        log.warning(msg);

                        return false;
                    }
                }

                return true; // All partitions have been released.
            }
        }, TIMEOUT_MS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Creates  cache.
     */
    private void createCache() {
        CacheConfiguration<Integer, Person> cacheConf = new CacheConfiguration<>(PERSON_CACHE);

        cacheConf.setCacheMode(CacheMode.PARTITIONED)
        .setBackups(0)
        .setIndexedTypes(Integer.class, Person.class)
        .setName(PERSON_CACHE);

        grid(0).createCache(cacheConf);
    }

    /**
     * Populates cache with a data.
     *
     * @return Number of filled entities.
     */
    private int fillCache() {
        IgniteDataStreamer streamer = grid(0).dataStreamer(PERSON_CACHE);

        for (int i = 0; i < CACHE_SIZE; i++)
            streamer.addData(i, new Person("p_"+ i, i));

        streamer.flush();

        streamer.close();

        return CACHE_SIZE;
    }

    /**
     *
     */
    public static class Person implements Serializable {
        /** Name. */
        @QuerySqlField
        private String name;

        /** Age. */
        @QuerySqlField
        private int age;

        /**
         * @param name Name.
         * @param age Age.
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /**
         *
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         *
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age Age.
         */
        public void setAge(int age) {
            this.age = age;
        }
    }
}
