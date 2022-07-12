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
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
@RunWith(Parameterized.class)
public class RepeatedFieldIndexQueryTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String ID_IDX = "ID_IDX";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteCache<Integer, Person> cache;

    /** */
    @Parameterized.Parameter
    public String idxName;

    /** */
    @Parameterized.Parameter(1)
    public String fldName;

    /** */
    @Parameterized.Parameters(name = "idx={0} fldName={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] {ID_IDX, "id"},
            new Object[] {DESC_ID_IDX, "descId"}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);

        for (int i = 0; i < CNT; i++)
            cache.put(i, new Person(i));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>()
            .setName(CACHE)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg1);

        return cfg;
    }

    /** */
    @Test
    public void testRangeQueriesWithTwoCriteriaSingleField() {
        int lower = new Random().nextInt(CNT / 2);
        int upper = CNT / 2 + new Random().nextInt(CNT / 2 - 1);

        List<IndexQueryCriterion> criteria = criteria(fldName, lower, upper);

        List<T2<RangeIndexQueryCriterion, RangeIndexQueryCriterion>> checks = new ArrayList<>();

        for (int i = 0; i < criteria.size(); i++) {
            for (int j = 0; j < criteria.size(); j++) {
                checks.add(new T2<>(
                    (RangeIndexQueryCriterion)criteria.get(i),
                    (RangeIndexQueryCriterion)criteria.get(j)));
            }

        }

        checks.forEach(c -> checkTwoCriteria(c.get1(), c.get2()));
    }

    /** */
    @Test
    public void testMergeMultipleCriteriaForSingleField() {
        int lower = new Random().nextInt(CNT / 2);
        int upper = CNT / 2 + new Random().nextInt(CNT / 2 - 1);

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), gt(fldName, lower - 1), gt(fldName, lower - 2),
                lt(fldName, upper), lt(fldName, upper + 1), lt(fldName, upper + 2));

        check(null, cache.query(qry), lower + 1, upper);
    }

    /** */
    @Test
    public void testMultipleEqualsCriteria() {
        int lower = new Random().nextInt(CNT / 2);
        int upper = CNT / 2 + new Random().nextInt(CNT / 2 - 1);

        checkEqualsCriteria(lower, upper, 0, CNT);
        checkEqualsCriteria(lower, upper, CNT, 0);
        checkEqualsCriteria(upper, lower, 0, CNT);
        checkEqualsCriteria(upper, lower, CNT, 0);
    }

    /** */
    private void checkEqualsCriteria(int eq1, int eq2, int from, int to) {
        GridTestUtils.assertThrows(null, () -> {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
                .setCriteria(eq(fldName, eq1), eq(fldName, eq2), between(fldName, from, to));

            return cache.query(qry).getAll();
        }, CacheException.class, "Failed to merge criterion");
    }

    /** */
    @Test
    public void testCommonBoundary() {
        int boundary = new Random().nextInt(CNT / 2);

        checkEmptyForCommonBoundary(lt(fldName, boundary), gt(fldName, boundary));
        checkEmptyForCommonBoundary(lte(fldName, boundary), gt(fldName, boundary));
        checkEmptyForCommonBoundary(lt(fldName, boundary), gte(fldName, boundary));

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, boundary), gte(fldName, boundary));

        check(null, cache.query(qry), boundary, boundary + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, 0, boundary), between(fldName, boundary, CNT));

        check(null, cache.query(qry), boundary, boundary + 1);
    }

    /** */
    private void checkEmptyForCommonBoundary(IndexQueryCriterion c1, IndexQueryCriterion c2) {
        GridTestUtils.assertThrows(null, () -> {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
                .setCriteria(c1, c2);

            return cache.query(qry).getAll();
        }, CacheException.class, "Failed to merge criterion");
    }

    /** */
    @Test
    public void testCorrectMergeMultipleBoundaries() {
        Random rnd = new Random();

        List<Integer> boundaries = IntStream.range(0, 10)
            .boxed()
            .map(i -> rnd.nextInt(CNT))
            .collect(Collectors.toList());

        int min = boundaries.stream().min(Integer::compareTo).get();
        int max = boundaries.stream().max(Integer::compareTo).get();

        List<IndexQueryCriterion> ltCriteria = boundaries.stream()
            .map(b -> lt(fldName, b))
            .collect(Collectors.toList());

        List<IndexQueryCriterion> gtCriteria = boundaries.stream()
            .map(b -> gt(fldName, b))
            .collect(Collectors.toList());

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(ltCriteria);

        check(null, cache.query(qry), 0, min);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gtCriteria);

        check(null, cache.query(qry), max + 1, CNT);
    }

    /**
     * @param left  First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private <T> void check(String errMsg, QueryCursor<Cache.Entry<Integer, Person>> cursor, int left, int right) {
        List<Cache.Entry<Integer, Person>> all = cursor.getAll();

        assertEquals(errMsg, right - left, all.size());

        boolean desc = Objects.equals(idxName, DESC_ID_IDX);

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Integer, Person> entry = all.get(i);

            int exp = desc ? right - 1 - i : left + i;

            assertEquals(errMsg, exp, entry.getKey().intValue());
        }
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = ID_IDX, order = 0))
        final int id;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = DESC_ID_IDX, order = 0, descending = true))
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
        }
    }

    /** */
    private List<IndexQueryCriterion> criteria(String fld, int val1, int val2) {
        return F.asList(
            eq(fld, val1),
            lt(fld, val1),
            lte(fld, val1),
            gt(fld, val1),
            gte(fld, val1),
            between(fld, val1, val2));
    }

    /** */
    private void checkTwoCriteria(RangeIndexQueryCriterion c1, RangeIndexQueryCriterion c2) {
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(c1, c2);

        Range expRange = mergeRange(
            new Range(c1.lower() == null ? 0 : (int)c1.lower(), c1.upper() == null ? CNT : (int)c1.upper(),
                c1.lowerIncl(), c1.upperIncl()),

            new Range(c2.lower() == null ? 0 : (int)c2.lower(), c2.upper() == null ? CNT : (int)c2.upper(),
                c2.lowerIncl(), c2.upperIncl()));

        int lower = expRange.lower();
        int upper = expRange.upper();

        String errMsg = "Fail crit pair: " + c1 + ", " + c2 + ". Lower=" + lower + ", upper=" + upper;

        if (!expRange.valid()) {
            GridTestUtils.assertThrows(null, () -> cache.query(qry).getAll(),
                CacheException.class, "Failed to merge criterion",
                "Not thrown for " + c1 + " " + c2);
        }
        else
            check(errMsg, cache.query(qry), lower, upper);
    }

    /** */
    private Range mergeRange(Range range1, Range range2) {
        int left = range1.left;
        boolean leftIncl = range1.leftIncl;

        if (range2.left > left) {
            left = range2.left;
            leftIncl = range2.leftIncl;
        }
        else if (range2.left == left)
            leftIncl = leftIncl && range2.leftIncl;

        int right = range1.right;
        boolean rightIncl = range1.rightIncl;

        if (range2.right < right) {
            right = range2.right;
            rightIncl = range2.rightIncl;
        }
        else if (range2.right == right)
            rightIncl = rightIncl && range2.rightIncl;

        return new Range(left, right, leftIncl, rightIncl);
    }

    /** */
    private static class Range {
        /** */
        final int left;

        /** */
        final int right;

        /** */
        final boolean leftIncl;

        /** */
        final boolean rightIncl;

        /** */
        Range(int left, int right, boolean leftIncl, boolean rightIncl) {
            this.left = left;
            this.right = right;
            this.leftIncl = leftIncl;
            this.rightIncl = rightIncl;
        }

        /** */
        int lower() {
            return leftIncl ? left : left + 1;
        }

        /** */
        int upper() {
            return rightIncl ? right == CNT ? CNT : right + 1 : right;
        }

        /** */
        boolean valid() {
            return left < right || (left == right && leftIncl && rightIncl);
        }
    }
}
