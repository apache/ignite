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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.util.typedef.F;
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
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;
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
    @Parameterized.Parameter(5)
    public String idxName;

    /** Number of duplicates of indexed value. */
    @Parameterized.Parameter(6)
    public int duplicates;

    /** */
    @Parameterized.Parameters(name = "qryPar={0} atomicity={1} mode={2} node={3} backups={4} idxName={5} duplicates={6}")
    public static Collection<Object[]> testParams() {
        List<Object[]> params = new ArrayList<>();

        Stream.of("CRD", "CLN").forEach(node ->
            Stream.of(0, 2).forEach(backups ->
                Stream.of(1, 10).forEach(duplicates ->
                    Stream.of(IDX, DESC_IDX).forEach(idx -> {
                        params.add(new Object[] {1, TRANSACTIONAL, REPLICATED, node, backups, idx, duplicates});
                        params.add(new Object[] {1, TRANSACTIONAL, PARTITIONED, node, backups, idx, duplicates});
                        params.add(new Object[] {4, TRANSACTIONAL, PARTITIONED, node, backups, idx, duplicates});
                    })
                )
            )
        );

        return params;
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
    public void testRangeQueries() throws Exception {
        // Query empty cache.
        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, idxName);

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Add data
        insertData();

        qry = new IndexQuery<>(Person.class, idxName);

        check(qry, 0, CNT);

        // Range queries.
        String fld = idxName.equals(IDX) ? "id" : "descId";

        int pivot = new Random().nextInt(CNT);

        // Eq.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(eq(fld, pivot));

        check(qry, pivot, pivot + 1);

        // Lt.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(lt(fld, pivot));

        check(qry, 0, pivot);

        // Lte.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(lte(fld, pivot));

        check(qry, 0, pivot + 1);

        // Gt.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(gt(fld, pivot));

        check(qry, pivot + 1, CNT);

        // Gte.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(gte(fld, pivot));

        check(qry, pivot, CNT);

        // Between.
        int lower = new Random().nextInt(CNT / 2);
        int upper = lower + CNT / 20;

        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(between(fld, lower, upper));

        check(qry, lower, upper + 1);

        // In.
        qry = new IndexQuery<Long, Person>(Person.class, idxName)
            .setCriteria(in(fld, F.asList(pivot, pivot + 1)));

        check(qry, pivot, pivot + 2);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(Query<Cache.Entry<Long, Person>> qry, int left, int right) throws Exception {
        QueryCursor<Cache.Entry<Long, Person>> cursor = cache.query(qry);

        int expSize = (right - left) * duplicates;

        Set<Long> expKeys = new HashSet<>(expSize);
        List<Integer> expOrderedValues = new LinkedList<>();

        boolean desc = idxName.equals(DESC_IDX);

        int from = desc ? right - 1 : left;
        int to = desc ? left - 1 : right;
        IntUnaryOperator op = (i) -> desc ? i - 1 : i + 1;

        for (int i = from; i != to; i = op.applyAsInt(i)) {
            for (int j = 0; j < duplicates; j++) {
                expOrderedValues.add(i);
                expKeys.add((long)CNT * j + i);
            }
        }

        AtomicInteger actSize = new AtomicInteger();

        ((QueryCursorEx<Cache.Entry<Long, Person>>)cursor).getAll(entry -> {
            assertEquals(expOrderedValues.remove(0).intValue(), entry.getValue().id);

            assertTrue(expKeys.remove(entry.getKey()));

            int persId = entry.getKey().intValue() % CNT;

            assertEquals(new Person(persId), entry.getValue());

            actSize.incrementAndGet();
        });

        assertEquals(expSize, actSize.get());

        assertTrue(expKeys.isEmpty());
    }

    /** */
    private void insertData() {
        try (IgniteDataStreamer<Long, Person> streamer = crd.dataStreamer(cache.getName())) {
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

            Person person = (Person)o;

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
