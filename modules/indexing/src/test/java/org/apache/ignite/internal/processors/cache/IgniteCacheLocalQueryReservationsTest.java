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
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for local query partitions reservation.
 */
public class IgniteCacheLocalQueryReservationsTest extends GridCommonAbstractTest {

    /** Cache name */
    private static final String PERSON_CACHE = "person";

    /** Timeout */
    private static final long TIMEOUT_MS = 30_000L;

    /** Cache size. */
    private static final int CACHE_SIZE = 2_000_000;

    /**
     * Test for the local {@link SqlFieldsQuery} reservations.
     *
     * @throws Exception If failed.
     */
    public void testLocalSqlFieldsQueryReservations() throws Exception {
        Query qry = new SqlFieldsQuery("select count(*) from person where age >= 0 or age < 1000000000");

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
     * Checks if a query partitions reservations occurs.
     *
     * @param qry Query for entries counting.
     * @throws Exception If failed.
     */
    private void checkReservations(final Query qry) throws Exception {
        final IgniteEx grid = startGrid(0);

        createAndFillCache();

        qry.setLocal(true);

        final IgniteCache<Integer, Person> cache = grid(0).cache(PERSON_CACHE);

        final CyclicBarrier crd = new CyclicBarrier(2);

        /*
         * Checks this scenario:
         * 1. Before the query execution all partitions should not be reserved.
         * 2. During the query execution all partitions should be reserved.
         * 3. After the query execution all partitions should be released.
         */
        Callable<Boolean> reservationsChecker = new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                final List<GridDhtLocalPartition> parts = grid.context().cache().internalCache(PERSON_CACHE).context()
                    .topology().localPartitions();

                // 1. Before the query execution all partitions should not be reserved.
                for (GridDhtLocalPartition part : parts) {
                    if (part.reservations() > 0) {
                        log.error("Partitions should not be reserved before the query execution.");

                        return false; // There are no reservations should be here yet.
                    }
                }

                final Set<GridDhtLocalPartition> partsSet = new HashSet<>(parts);

                crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS); // Query execution starts here.

                // 2. During the query execution all partitions should be reserved.
                boolean wasReserved = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        for (Iterator<GridDhtLocalPartition> partsIt = partsSet.iterator(); partsIt.hasNext();) {
                            GridDhtLocalPartition part = partsIt.next();
                            if (part.reservations() == 1)
                                partsIt.remove();
                        }

                        if (partsSet.isEmpty())
                            return true; // All partitions have been reserved.
                        else {
                            log.warning("Partitions should be reserved during the query execution.");

                            return false;
                        }
                    }
                }, TIMEOUT_MS);

                if (!wasReserved)
                    return false;

                crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

                // 3. After the query execution all partitions should be released.
                boolean wasReleased = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        for (GridDhtLocalPartition part : parts) {
                            if (part.reservations() > 0) {
                                log.error("Partitions should be released after the query execution.");

                                return false;
                            }
                        }

                        return true; // All partitions have been released.
                    }
                }, TIMEOUT_MS);

                return wasReleased;
            }
        };

        FutureTask<Boolean> reservationsResult = new FutureTask<>(reservationsChecker);

        Thread checkerThread = new Thread(reservationsResult);

        checkerThread.start();

        crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS); // Wait before query execution.

        cache.query(qry).getAll();

        crd.await(TIMEOUT_MS, TimeUnit.MILLISECONDS); // Wait after query execution.

        assertTrue("Partitions are not properly reserved or released", reservationsResult.get());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Creates and fills cache.
     *
     * @return Number of filled cache entities.
     */
    private int createAndFillCache() {
        CacheConfiguration<Integer, Person> cacheConf = new CacheConfiguration<>(PERSON_CACHE);

        cacheConf.setCacheMode(CacheMode.PARTITIONED)
        .setBackups(0)
        .setIndexedTypes(Integer.class, Person.class)
        .setName(PERSON_CACHE);

        IgniteCache<Integer, Person> cache = grid(0).createCache(cacheConf);

        Affinity<Integer> aff = grid(0).affinity(PERSON_CACHE);

        return fillCache(cache, aff);
    }

    /**
     * Populates cache with a data.
     *
     * @param cache - Cache to fill all partition to.
     * @param aff Affinity.
     * @return Number of filled entities.
     */
    private int fillCache(IgniteCache<Integer, Person> cache, Affinity<Integer> aff) {
        // Should be quite enough for a long time of query execution (hundreds of milliseconds).

        IgniteDataStreamer streamer = grid(0).dataStreamer(PERSON_CACHE);

        for (int i = 0; i < CACHE_SIZE; i++)
            streamer.addData(i, new Person("p_"+ i, i));

        streamer.flush();

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
