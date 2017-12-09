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
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for local query partitions reservation.
 */
public class IgniteCacheLocalQueryReservationsTest extends GridCommonAbstractTest {

    /** Cache name */
    private static final String PERSON_CACHE = "person";

    /**
     * Checks if a query reservation prevents cache partitions from the rebalancing.
     *
     * @param qry Query for entries counting.
     * @throws Exception If failed.
     */
    public void testReservations() throws Exception {
        startGrid(0);

        int cnt = createAndFillCache();

        System.out.println("cnt=" + cnt);

        final Query qry;

        qry = new SqlFieldsQuery("select count(*) from person where age >= 0 or age < 1000000000");

        qry.setLocal(true);

        final IgniteCache<Integer, Person> cache = grid(0).cache(PERSON_CACHE);


        final long start = System.currentTimeMillis();

        List<List<?>> res; /* = cache.query(qry).getAll();

        System.out.println((System.currentTimeMillis() - start) + " res before1=" + res.get(0).get(0));*/

        startGrid(1);

        /*final long duration = 10000;


        Runnable r = new Runnable() {
            @Override public void run() {
                int i = 0;

                while (System.currentTimeMillis() < start + duration) {
                    List<List<?>> r = cache.query(qry).getAll();
                    System.out.println((System.currentTimeMillis() - start) + " res in cycle" + i + "=" + r.get(0).get(0));
                    //doSleep(1);
                }
            }
        };

        Thread t1 = new Thread(r);
        Thread t2 = new Thread(r);

        t1.start();
        doSleep(200);
        t2.start();

        t1.join();
        t2.join();*/

        doSleep(1000);
        res = cache.query(qry).getAll();

        System.out.println((System.currentTimeMillis() - start) + " res after second started and small sleep=" + res.get(0).get(0));



        doSleep(3000);

        res = cache.query(qry).getAll();

        System.out.println("res after sleep=" + res.get(0).get(0));

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

        return fillAllPartitions(cache, aff);
    }

    /**
     * Fills each partition in the cache with a single data entry.
     *
     * @param cache - Cache to fill all partition to.
     * @param aff Affinity.
     * @return Number of filled partitions
     */
    private int fillAllPartitions(IgniteCache<Integer, Person> cache, Affinity<Integer> aff) {
        /*int partsCnt = aff.partitions();

        Set<Integer> emptyParts = new HashSet<>(partsCnt);

        for (int i = 0; i < partsCnt; i++)
            emptyParts.add(i);

        int cntr = 0;

        while (!emptyParts.isEmpty()) {
            int part = aff.partition(cntr++);

            if (emptyParts.remove(part))
                cache.put(cntr, new Person("p_"+ cntr, cntr));

            if (cntr > 100_000)
                fail("Failed to fill all partitions");
        }*/

        int partsCnt = 1_000_000;

        IgniteDataStreamer streamer = grid(0).dataStreamer(PERSON_CACHE);

        for (int i = 0; i < partsCnt; i++)
            streamer.addData(i, new Person("p_"+ i, i));

        streamer.flush();

        return partsCnt;
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
