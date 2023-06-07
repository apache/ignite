package org.apache.ignite.cache.query;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

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
                .setCacheMode(REPLICATED)
                .setQueryParallelism(1)
                .setBackups(0);

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
    public void testSetLimit() throws Exception {
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

        // Query empty cache.
        IndexQuery<Long, Person> qry = new IndexQuery<>(Person.class, IDX);

        assertTrue(crd.cache(CACHE).query(qry).getAll().isEmpty());

        // Add data
        insertData(duplicates);

        // All
        checkLimit(null, 0, CNT, duplicates);

        // Range queries.
        String fld = "id";

        int pivot = new Random().nextInt(CNT);

        // Eq.
        checkLimit(eq(fld, pivot), pivot, pivot + 1, duplicates);

        // Lt.
        checkLimit(lt(fld, pivot), 0, pivot, duplicates);

        // Lte.
        checkLimit(lte(fld, pivot), 0, pivot + 1, duplicates);

        // Gt.
        checkLimit(gt(fld, pivot), pivot + 1, CNT, duplicates);

        // Gte.
        checkLimit(gte(fld, pivot), pivot, CNT, duplicates);

        // Between.
        int lower = new Random().nextInt(CNT / 2);
        int upper = lower + CNT / 20;

        checkLimit(between(fld, lower, upper), lower, upper + 1, duplicates);

        // In.
        checkLimit(in(fld, F.asList(pivot, pivot + 1)), pivot, pivot + 2, duplicates);
    }

    /** */
    private void checkLimit(IndexQueryCriterion criterion, int left, int right, int duplicates) throws Exception {
        int rows = right - left;
        int limit = new Random().nextInt(rows + 1 - 1) + 1;

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
        if (criterion != null) {
            qry.setCriteria(criterion);
        }
        qry.setLimit(limit);

        QueryCursor<Cache.Entry<Long, Person>> cursor = crd.cache(CACHE).query(qry);

        int expSize = (right - left) * duplicates;
        if (limit > 0 && limit < expSize) {
            expSize = limit;
        }

        Set<Long> expKeys = new HashSet<>(expSize);
        List<Integer> expOrderedValues = new LinkedList<>();

        IntUnaryOperator op = (i) -> i + 1;

        loop: for (int i = left; i != right; i = op.applyAsInt(i)) {
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
