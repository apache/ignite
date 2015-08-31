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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteClientReconnectFailoverAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteClientReconnectCacheQueriesFailoverTest extends IgniteClientReconnectFailoverAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        final IgniteCache<Integer, Person> cache = grid(serverCount()).cache(null);

        assertNotNull(cache);

        for (int i = 0; i <= 10_000; i++)
            cache.put(i, new Person(i, "name-" + i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCacheQueries() throws Exception {
        final Ignite client = grid(serverCount());

        final IgniteCache<Integer, Person> cache = client.cache(null);

        assertNotNull(cache);

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                SqlQuery<Integer, Person> sqlQry = new SqlQuery<>(Person.class, "where id > 1");

                try {
                    assertEquals(9999, cache.query(sqlQry).getAll().size());
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof IgniteClientDisconnectedException)
                        throw e;
                    else
                        log.info("Ignore error: " + e);
                }

                try {
                    SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select avg(p.id) from Person p");

                    List<List<?>> res = cache.query(fieldsQry).getAll();

                    assertEquals(1, res.size());

                    Double avg = (Double)res.get(0).get(0);

                    assertEquals(5_000, avg.intValue());
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof IgniteClientDisconnectedException)
                        throw e;
                    else
                        log.info("Ignore error: " + e);
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectScanQuery() throws Exception {
        final Ignite client = grid(serverCount());

        final IgniteCache<Integer, Person> cache = client.cache(null);

        assertNotNull(cache);

        final Affinity<Integer> aff = client.affinity(null);

        final Map<Integer, Integer> partMap = new HashMap<>();

        for (int i = 0; i < aff.partitions(); i++)
            partMap.put(i, 0);

        for (int i = 0; i <= 10_000; i++) {
            Integer part = aff.partition(i);

            Integer size = partMap.get(part);

            partMap.put(part, size + 1);
        }

        reconnectFailover(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ScanQuery<Integer, Person> qry = new ScanQuery<>(new IgniteBiPredicate<Integer, Person>() {
                    @Override public boolean apply(Integer key, Person val) {
                        return val.getId() % 2 == 1;
                    }
                });

                assertEquals(5000, cache.query(qry).getAll().size());

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                Integer part = rnd.nextInt(0, aff.partitions());

                qry = new ScanQuery<>(part);

                assertEquals((int)partMap.get(part), cache.query(qry).getAll().size());

                return null;
            }
        });
    }

    /**
     *
     */
    public static class Person {
        /** */
        @QuerySqlField
        public int id;

        /** */
        @QuerySqlField
        public String name;

        /**
         * @param id Id.
         * @param name Name.
         */
        public Person(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return Id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Set id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Name.
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass()) && id == ((Person)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}