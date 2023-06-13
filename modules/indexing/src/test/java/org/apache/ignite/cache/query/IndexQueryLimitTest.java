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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQueryLimitTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "PERSON_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private Ignite crd;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<Long, Person>()
                .setName(CACHE)
                .setIndexedTypes(Long.class, Person.class)
                .setAtomicityMode(TRANSACTIONAL)
                .setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testRangeQueriesWithoutDuplicates() throws Exception {
        checkRangeQueries(1);
    }

    /** */
    @Test
    public void testRangeQueriesWithDuplicates() throws Exception {
        checkRangeQueries(10);
    }

    /** */
    @Test
    public void testSetLimit() {
        GridTestUtils.assertThrows(log, () -> new IndexQuery<>(Person.class, IDX).setLimit(0),
            IllegalArgumentException.class, "Limit must be positive.");

        int limit = 1 + new Random().nextInt(1000);

        GridTestUtils.assertThrows(log, () -> new IndexQuery<>(Person.class, IDX).setLimit(0 - limit),
            IllegalArgumentException.class, "Limit must be positive.");

        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, IDX);

        qry.setLimit(limit);

        assertEquals(limit, qry.getLimit());
    }

    /** */
    private void checkRangeQueries(int duplicates) throws Exception {
        // Add data
        insertData(duplicates);

        // All
        checkLimit(null, 0, CNT, duplicates);

        int pivot = new Random().nextInt(CNT);

        // Lt.
        checkLimit(lt("id", pivot), 0, pivot, duplicates);
    }

    /** */
    private void checkLimit(IndexQueryCriterion criterion, int left, int right, int duplicates) throws Exception {
        int rows = right - left;
        int limit = new Random().nextInt(rows) + 1;

        // limit < rows
        checkLimit(criterion, limit, left, left + limit, duplicates);

        // limit >= rows
        if (rows > 1) {
            limit = new Random().nextInt(CNT + 2 - rows) + rows;

            checkLimit(criterion, limit, left, right, duplicates);
        }
    }

    /** */
    private void checkLimit(IndexQueryCriterion criterion, int limit, int left, int right, int duplicates) throws Exception {
        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, IDX);

        if (criterion != null)
            qry.setCriteria(criterion);

        qry.setLimit(limit);

        QueryCursor<Cache.Entry<Long, Person>> cursor = crd.cache(CACHE).query(qry);

        int expSize = (right - left) * duplicates;

        if (limit > 0 && limit < expSize)
            expSize = limit;

        Set<Long> expKeys = new HashSet<>(expSize);
        List<Integer> expOrderedValues = new LinkedList<>();

        loop: for (int i = left; i != right; i++) {
            for (int j = 0; j < duplicates; j++) {
                expOrderedValues.add(i);

                expKeys.add((long)CNT * j + i);
                if (expOrderedValues.size() >= limit)
                    break loop;
            }
        }

        AtomicInteger actSize = new AtomicInteger();
        ((QueryCursorEx<Cache.Entry<Long, Person>>)cursor).getAll(entry -> {
            assertEquals(expOrderedValues.remove(0), (Integer)entry.getValue().id);

            assertTrue(expKeys.remove(entry.getKey()));

            int persId = entry.getKey().intValue() % CNT;

            assertEquals(new Person(persId), entry.getValue());

            actSize.incrementAndGet();
        });

        assertEquals(expSize, actSize.get());

        assertTrue(expKeys.isEmpty());
    }

    /** */
    private void insertData(int duplicates) {
        try (IgniteDataStreamer<Long, Person> streamer = crd.dataStreamer(CACHE)) {
            for (int persId = 0; persId < CNT; persId++) {
                // Create duplicates of data.
                for (int i = 0; i < duplicates; i++)
                    streamer.addData((long)CNT * i + persId, new Person(persId));
            }
        }
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

            Person person = (Person)o;

            return Objects.equals(id, person.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }
}
