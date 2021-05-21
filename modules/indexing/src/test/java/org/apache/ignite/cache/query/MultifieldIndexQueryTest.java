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
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MultifieldIndexQueryTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String INDEX = "TEST_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private Ignite ignite;

    /** */
    private IgniteCache cache;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGrids(2);

        cache = ignite.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>()
            .setName("TEST_CACHE")
            .setIndexedTypes(Long.class, Person.class)
            .setQueryParallelism(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testQueryKeyPKIndex() {
        insertData();

        int pivot = new Random().nextInt(CNT);

        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forIndex(Person.class, "_key_PK")
            .lt("_KEY", (long) pivot);

        checkPerson(cache.query(qry), 0, pivot);
    }

    /** */
    @Test
    public void testEmptyCacheQuery() {
        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", Integer.MAX_VALUE, "secId", Integer.MAX_VALUE);

        QueryCursor<Cache.Entry<Long, Person>> cursor = cache.query(qry);

        assertTrue(cursor.getAll().isEmpty());

        // Check same query with specify index name.
        qry = IndexQuery
            .<Long, Person>forIndex(Person.class, INDEX)
            .lt("id", Integer.MAX_VALUE, "secId", Integer.MAX_VALUE);

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testLtQueryMultipleField() {
        insertData();

        int pivot = new Random().nextInt(CNT);

        // Should return empty result for ID that less any inserted.
        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", -1, "secId", pivot);

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Should return all data for ID and SECID that greater any inserted.
        qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", 1, "secId", pivot * 10);

        checkPerson(cache.query(qry), 0, CNT);

        // Should return part of data, as ID equals to inserted data ID field.
        qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", 0, "secId", pivot);

        checkPerson(cache.query(qry), 0, pivot);

        // Should return all data for ID greater any inserted.
        qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", 1, "secId", pivot);

        checkPerson(cache.query(qry), 0, CNT);

        // Checks the same with query with specified index name.
        qry = IndexQuery
            .<Long, Person>forIndex(Person.class, INDEX)
            .lt("id", -1, "secId", pivot);

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = IndexQuery
            .<Long, Person>forIndex(Person.class, INDEX)
            .lt("id", 0, "secId", pivot);

        checkPerson(cache.query(qry), 0, pivot);

        qry = IndexQuery
            .<Long, Person>forIndex(Person.class, INDEX)
            .lt("id", 1, "secId", pivot);

        checkPerson(cache.query(qry), 0, CNT);
    }

    /** */
    @Test
    public void testWrongBoundaryClass() {
        insertData();

        // Use long boundary instead of int.
        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", (long) 0);

        GridTestUtils.assertThrows(null,
            () -> cache.query(qry).getAll(), CacheException.class, null);

        GridTestUtils.assertThrowsWithCause(
            () -> cache.query(qry).getAll(), ClassCastException.class);
    }

    /** */
    @Test
    public void testQueryIndexWithKeyQuery() {
        insertData();

        int pivot = new Random().nextInt(CNT);

        IndexQuery<Long, Person> qry = IndexQuery
            .<Long, Person>forType(Person.class)
            .lt("id", 0, "secId", pivot, "_KEY", (long) pivot);

        checkPerson(cache.query(qry), 0, pivot);
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Person> streamer = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long) i, new Person(i));
        }
    }

    /** */
    private void checkPerson(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right) {
        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        Set<Long> expKeys = LongStream.range(left, right).boxed().collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            assertTrue(expKeys.remove(entry.getKey()));

            assertEquals(new Person(entry.getKey().intValue()), all.get(i).getValue());
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = INDEX, order = 0))
        final int id;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = INDEX, order = 1))
        final int secId;

        /** */
        Person(int secId) {
            this.id = 0;
            this.secId = secId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            return Objects.equals(id, person.id) && Objects.equals(secId, person.secId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, secId);
        }
    }
}
