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

package org.apache.ignite.cache.query;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexQueryLocalTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final int CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** Should return full data. */
    @Test
    public void testServerNodeReplicatedCache() throws Exception {
        Ignite crd = startGrids(4);

        IgniteCache cache = crd.getOrCreateCache(ccfg(CacheMode.REPLICATED));

        insertData(crd, cache);

        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", CNT / 2);

        for (int i = 0; i < 4; i++) {
            cache = grid(i).cache(CACHE);

            List result = cache.query(qry.setLocal(true)).getAll();

            assertEquals(CNT / 2, result.size());
        }
    }

    /** Should return part of data only. */
    @Test
    public void testServerNodePartitionedCache() throws Exception {
        Ignite crd = startGrids(4);

        IgniteCache cache = crd.getOrCreateCache(ccfg(CacheMode.PARTITIONED));

        insertData(crd, cache);

        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", CNT / 2);

        for (int i = 0; i < 4; i++) {
            cache = grid(i).cache(CACHE);

            List result = cache.query(qry.setLocal(true)).getAll();

            assertTrue(CNT / 2 > result.size());
        }
    }

    /** Should fail as no data on nodes. */
    @Test
    public void testClientNodeReplicatedCache() throws Exception {
        startGrid();

        Ignite cln = startClientGrid(1);

        IgniteCache cache = cln.getOrCreateCache(ccfg(CacheMode.REPLICATED));

        insertData(cln, cache);

        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", CNT / 2);

        GridTestUtils.assertThrows(null, () -> cache.query(qry.setLocal(true)).getAll(),
            IgniteException.class, "Cluster group is empty");
    }

    /** */
    private void insertData(Ignite ignite, IgniteCache<Long, Person> cache) {
        try (IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long) i, new Person(i));
        }
    }

    /** */
    private CacheConfiguration<Long, Person> ccfg(CacheMode cacheMode) {
        return new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setIndexedTypes(Long.class, Person.class)
            .setCacheMode(cacheMode);
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        final int id;

        /** */
        Person(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person[id=" + id + "]";
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            return Objects.equals(id, person.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}
