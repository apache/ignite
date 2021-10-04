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
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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

        List<T2<RangeIndexQueryCriterion, RangeIndexQueryCriterion>> cc = new ArrayList<>();

        Stream.of(new T2<>(lower, upper), new T2<>(upper, lower)).forEach(pair -> {
            for (int i = 0; i < 6; i++) {
                for (int j = 0; j < 6; j++) {
                    RangeIndexQueryCriterion c1 = (RangeIndexQueryCriterion)criterion(i, fldName, pair.get1(), pair.get2());
                    RangeIndexQueryCriterion c2 = (RangeIndexQueryCriterion)criterion(j, fldName, pair.get2(), pair.get1());

                    cc.add(new T2<>(c1, c2));
                }
            }
        });

        cc.forEach(c -> checkTwoCriteria(c.get1(), c.get2()));
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
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, eq1), eq(fldName, eq2), between(fldName, from, to));

        assertTrue(qry.getCriteria().toString(), cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testCommonBoundary() {
        int upper, lower = upper = new Random().nextInt(CNT / 2);

        checkEmptyForCommonBoundary(lt(fldName, lower), gt(fldName, upper));
        checkEmptyForCommonBoundary(lte(fldName, lower), gt(fldName, upper));
        checkEmptyForCommonBoundary(lt(fldName, lower), gte(fldName, upper));

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), gte(fldName, upper));

        check(null, cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, 0, lower), between(fldName, upper, CNT));

        check(null, cache.query(qry), lower, upper + 1);
    }

    /** */
    private void checkEmptyForCommonBoundary(IndexQueryCriterion c1, IndexQueryCriterion c2) {
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(c1, c2);

        assertTrue(qry.getCriteria().toString(), cache.query(qry).getAll().isEmpty());
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

        Set<Integer> expKeys = IntStream.range(left, right).boxed().collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Integer, Person> entry = all.get(i);

            assertTrue(errMsg, expKeys.remove(entry.getKey()));
        }

        assertTrue(errMsg, expKeys.isEmpty());
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
    private IndexQueryCriterion criterion(int idx, String fld, int val1, int val2) {
        switch (idx) {
            case 0: return eq(fld, val1);
            case 1: return lt(fld, val1);
            case 2: return lte(fld, val1);
            case 3: return gt(fld, val1);
            case 4: return gte(fld, val1);
            case 5: return between(fld, val1, val2);
            default:
                throw new RuntimeException("Unknown criteria operation");
        }
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

        if (lower >= upper)
            assertTrue(errMsg, cache.query(qry).getAll().isEmpty());
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
        } else if (range2.left == left)
            leftIncl = leftIncl && range2.leftIncl;

        int right = range1.right;
        boolean rightIncl = range1.rightIncl;

        if (range2.right < right) {
            right = range2.right;
            rightIncl = range2.rightIncl;
        } else if (range2.right == right)
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
    }
}
