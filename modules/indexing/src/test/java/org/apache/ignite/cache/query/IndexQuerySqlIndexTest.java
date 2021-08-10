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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQuerySqlIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String CACHE_TABLE = "TEST_CACHE_TABLE";

    /** */
    private static final String TABLE = "TEST_TABLE";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private IgniteCache<Object, Object> cache;

    /** */
    private Ignite crd;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrids(4);

        cache = crd.cache(CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>()
            .setName(CACHE);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testEmptyCache() {
        prepareTable();

        IgniteCache<Object, Object> tableCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, DESC_ID_IDX)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        assertTrue(tableCache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testRangeQueries() {
        prepareTable();

        insertData();

        int pivot = new Random().nextInt(CNT);

        IgniteCache<Object, Object> tableCache = crd.cache(CACHE_TABLE);

        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, DESC_ID_IDX)
            .setCriteria(lt("descId", pivot));

        check(tableCache.query(qry), 0, pivot);

        // Wrong fields in query.
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> wrongQry = new IndexQuery<Long, Person>(Person.class, DESC_ID_IDX)
                .setCriteria(lt("id", Integer.MAX_VALUE));

            return tableCache.query(wrongQry).getAll();

        }, IgniteCheckedException.class, "Index DESC_ID_IDX doesn't match query.");

        // Wrong cache.
        GridTestUtils.assertThrowsAnyCause(null, () -> {
            IndexQuery<Long, Person> wrongQry = new IndexQuery<Long, Person>(Person.class, DESC_ID_IDX)
                .setCriteria(lt("descId", Integer.MAX_VALUE));

            return cache.query(wrongQry).getAll();

        }, IgniteCheckedException.class, "No table found: " + Person.class.getName());
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right) {
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
    private void prepareTable() {
        SqlFieldsQuery qry = new SqlFieldsQuery("create table " + TABLE + " (prim_id long PRIMARY KEY, id int, descId int)" +
            " with \"VALUE_TYPE=" + Person.class.getName() + ",CACHE_NAME=" + CACHE_TABLE + "\";");

        cache.query(qry);

        qry = new SqlFieldsQuery("create index " + DESC_ID_IDX + " on " + TABLE + " (descId DESC);");

        cache.query(qry);
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Person> streamer = crd.dataStreamer(CACHE_TABLE)) {
            for (int i = 0; i < CNT; i++)
                streamer.addData((long) i, new Person(i));
        }
    }

    /** */
    private static class Person {
        /** */
        final int id;

        /** */
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
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

            Person person = (Person)o;

            return Objects.equals(id, person.id)
                && Objects.equals(descId, person.descId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId);
        }
    }
}
