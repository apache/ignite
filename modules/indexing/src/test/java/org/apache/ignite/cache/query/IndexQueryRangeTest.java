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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class IndexQueryRangeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String IDX = "PERSON_ID_IDX";

    /** */
    private static final String DESC_IDX = "PERSON_DESCID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private Ignite crd;

    /** */
    private int duplicates;

    /** */
    private IgniteCache<Long, Person> cache;

    /** */
    @Parameterized.Parameter
    public int qryParallelism;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(3)
    public String node;

    /** */
    @Parameterized.Parameter(4)
    public int backups;

    /** */
    @Parameterized.Parameters(name = "qryPar={0} atomicity={1} mode={2} node={3} backups={4}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(
            new Object[] {1, TRANSACTIONAL, REPLICATED, "CRD", 0},
            new Object[] {1, TRANSACTIONAL, PARTITIONED, "CRD", 0},
            new Object[] {4, TRANSACTIONAL, PARTITIONED, "CRD", 0},

            new Object[] {1, ATOMIC, REPLICATED, "CRD", 0},
            new Object[] {1, ATOMIC, PARTITIONED, "CRD", 0},
            new Object[] {4, ATOMIC, PARTITIONED, "CRD", 0},

            new Object[] {1, TRANSACTIONAL, REPLICATED, "CLN", 0},
            new Object[] {1, TRANSACTIONAL, PARTITIONED, "CLN", 0},
            new Object[] {4, TRANSACTIONAL, PARTITIONED, "CLN", 0},

            new Object[] {1, ATOMIC, REPLICATED, "CLN", 0},
            new Object[] {1, ATOMIC, PARTITIONED, "CLN", 0},
            new Object[] {4, ATOMIC, PARTITIONED, "CLN", 0},

            new Object[] {1, TRANSACTIONAL, REPLICATED, "CRD", 2},
            new Object[] {1, TRANSACTIONAL, PARTITIONED, "CRD", 2},
            new Object[] {4, TRANSACTIONAL, PARTITIONED, "CRD", 2},

            new Object[] {1, ATOMIC, REPLICATED, "CRD", 2},
            new Object[] {1, ATOMIC, PARTITIONED, "CRD", 2},
            new Object[] {4, ATOMIC, PARTITIONED, "CRD", 2},

            new Object[] {1, TRANSACTIONAL, REPLICATED, "CLN", 2},
            new Object[] {1, TRANSACTIONAL, PARTITIONED, "CLN", 2},
            new Object[] {4, TRANSACTIONAL, PARTITIONED, "CLN", 2},

            new Object[] {1, ATOMIC, REPLICATED, "CLN", 2},
            new Object[] {1, ATOMIC, PARTITIONED, "CLN", 2},
            new Object[] {4, ATOMIC, PARTITIONED, "CLN", 2});
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrids(4);

        Ignite client = startClientGrid();

        if ("CRD".equals(node))
            cache = crd.cache(CACHE);
        else
            cache = client.cache(CACHE);
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
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setQueryParallelism(qryParallelism)
            .setBackups(backups);

        // TODO: remove after IGNITE-15671.
        if (atomicityMode == ATOMIC)
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    @Test
    public void testRangeQueries() {
        duplicates = 1;

        checkRangeQueries();
    }

    /** */
    @Test
    public void testRangeDescQueries() {
        duplicates = 1;

        checkRangeDescQueries();
    }

    /** */
    @Test
    public void testRangeQueriesWithDuplicatedData() {
        duplicates = 10;

        checkRangeQueries();
    }

    /** */
    @Test
    public void testRangeDescQueriesWithDuplicatedData() {
        duplicates = 10;

        checkRangeDescQueries();
    }

    /** */
    public void checkRangeQueries() {
        // Query empty cache.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", Integer.MAX_VALUE));

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Add data
        insertData();

        int pivot = new Random().nextInt(CNT);

        // Eq.
        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(eq("id", pivot));

        check(cache.query(qry), pivot, pivot + 1);

        // Lt.
        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lt("id", pivot));

        check(cache.query(qry), 0, pivot);

        // Lte.
        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(lte("id", pivot));

        check(cache.query(qry), 0, pivot + 1);

        // Gt.
        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(gt("id", pivot));

        check(cache.query(qry), pivot + 1, CNT);

        // Gte.
        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(gte("id", pivot));

        check(cache.query(qry), pivot, CNT);

        // Between.
        int lower = new Random().nextInt(CNT / 2);
        int upper = lower + CNT / 20;

        qry = new IndexQuery<Long, Person>(Person.class, IDX)
            .setCriteria(between("id", lower, upper));

        check(cache.query(qry), lower, upper + 1);
    }

    /** */
    public void checkRangeDescQueries() {
        // Query empty cache.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(lt("descId", Integer.MAX_VALUE));

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Add data
        insertData();

        int pivot = new Random().nextInt(CNT);

        // Eq.
        qry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(eq("descId", pivot));

        check(cache.query(qry), pivot, pivot + 1);

        // Lt, desc index.
        IndexQuery<Long, Person> descQry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(lt("descId", pivot));

        check(cache.query(descQry), 0, pivot);

        // Lte, desc index.
        descQry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(lte("descId", pivot));

        check(cache.query(descQry), 0, pivot + 1);

        // Gt, desc index.
        descQry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(gt("descId", pivot));

        check(cache.query(descQry), pivot + 1, CNT);

        // Gte, desc index.
        descQry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(gte("descId", pivot));

        check(cache.query(descQry), pivot, CNT);

        // Between, desc index.
        int lower = new Random().nextInt(CNT / 2);
        int upper = lower + CNT / 20;

        descQry = new IndexQuery<Long, Person>(Person.class, DESC_IDX)
            .setCriteria(between("descId", lower, upper));

        check(cache.query(descQry), lower, upper + 1);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right) {
        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertFalse(all.isEmpty());

        assertEquals((right - left) * duplicates, all.size());

        Set<Long> expKeys = LongStream
            .range((long) left * duplicates, (long) right * duplicates).boxed()
            .collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Long, Person> entry = all.get(i);

            assertTrue(expKeys.remove(entry.getKey()));

            int persId = entry.getKey().intValue() / duplicates;

            assertEquals(new Person(persId), all.get(i).getValue());
        }

        assertTrue(expKeys.isEmpty());
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Person> streamer = crd.dataStreamer(cache.getName())) {
            for (int persId = 0; persId < CNT; persId++) {
                // Create duplicates of data.
                for (int i = 0; i < duplicates; i++)
                    streamer.addData((long) persId * duplicates + i, new Person(persId));
            }
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(index = true)
        final int id;

        /** */
        @QuerySqlField(index = true, descending = true)
        final int descId;

        /** */
        @QuerySqlField
        final int nonIdxSqlFld;

        /** */
        final int nonSqlFld;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
            nonIdxSqlFld = id;
            nonSqlFld = id;
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

            return Objects.equals(id, person.id)
                && Objects.equals(descId, person.descId)
                && Objects.equals(nonIdxSqlFld, person.nonIdxSqlFld)
                && Objects.equals(nonSqlFld, person.nonSqlFld);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId, nonIdxSqlFld, nonSqlFld);
        }
    }
}
