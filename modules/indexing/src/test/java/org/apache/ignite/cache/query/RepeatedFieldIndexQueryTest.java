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
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
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

        // Lt.
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), lt(fldName, lower));

        check(cache.query(qry), 0, lower);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), lte(fldName, lower));

        check(cache.query(qry), 0, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), eq(fldName, lower));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), gt(fldName, lower));

        check(cache.query(qry), lower + 1, upper);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), gte(fldName, lower));

        check(cache.query(qry), lower, upper);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, upper), between(fldName, lower, upper));

        check(cache.query(qry), lower, upper);

        // Lt, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), lt(fldName, upper));

        check(cache.query(qry), 0, lower);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), lte(fldName, upper));

        check(cache.query(qry), 0, lower);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), eq(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), gte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), between(fldName, lower, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Lte.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), lt(fldName, lower));

        check(cache.query(qry), 0, lower);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), lte(fldName, lower));

        check(cache.query(qry), 0, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), eq(fldName, lower));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), gt(fldName, lower));

        check(cache.query(qry), lower + 1, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), gte(fldName, lower));

        check(cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, upper), between(fldName, lower, upper));

        check(cache.query(qry), lower, upper + 1);

        // Lte, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), lt(fldName, upper));

        check(cache.query(qry), 0, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), lte(fldName, upper));

        check(cache.query(qry), 0, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), eq(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), gte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), between(fldName, lower, upper));

        check(cache.query(qry), lower, lower + 1);

        // Gt.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), lt(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), lte(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), eq(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), gt(fldName, lower));

        check(cache.query(qry), upper + 1, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), gte(fldName, lower));

        check(cache.query(qry), upper + 1, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, upper), between(fldName, lower, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        // Gt, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), lt(fldName, upper));

        check(cache.query(qry), lower + 1, upper);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), lte(fldName, upper));

        check(cache.query(qry), lower + 1, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), eq(fldName, upper));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), gt(fldName, upper));

        check(cache.query(qry), upper + 1, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), gte(fldName, upper));

        check(cache.query(qry), upper, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gt(fldName, lower), between(fldName, lower, upper));

        check(cache.query(qry), lower + 1, upper + 1);

        // Gte.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), lt(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), lte(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), eq(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), gt(fldName, lower));

        check(cache.query(qry), upper, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), gte(fldName, lower));

        check(cache.query(qry), upper, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, upper), between(fldName, lower, upper));

        check(cache.query(qry), upper, upper + 1);

        // Gte, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), lt(fldName, upper));

        check(cache.query(qry), lower, upper);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), lte(fldName, upper));

        check(cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), eq(fldName, upper));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), gt(fldName, upper));

        check(cache.query(qry), upper + 1, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), gte(fldName, upper));

        check(cache.query(qry), upper, CNT);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gte(fldName, lower), between(fldName, lower, upper));

        check(cache.query(qry), lower, upper + 1);

        // Eq.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), lt(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), lte(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), eq(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), gt(fldName, lower));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), gte(fldName, lower));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), between(fldName, lower, upper));

        check(cache.query(qry), upper, upper + 1);

        // Eq, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), lt(fldName, upper));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), lte(fldName, upper));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), eq(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), gte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), between(fldName, lower, upper));

        check(cache.query(qry), lower, lower + 1);

        // Between.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), lt(fldName, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), lte(fldName, lower));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), eq(fldName, lower));

        check(cache.query(qry), lower, lower + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), gt(fldName, lower));

        check(cache.query(qry), lower + 1, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), gte(fldName, lower));

        check(cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), between(fldName, lower, upper));

        check(cache.query(qry), lower, upper + 1);

        // Between, reverse order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), lt(fldName, upper));

        check(cache.query(qry), lower, upper);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), lte(fldName, upper));

        check(cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), eq(fldName, upper));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), gte(fldName, upper));

        check(cache.query(qry), upper, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, lower, upper), between(fldName, lower, upper));

        check(cache.query(qry), lower, upper + 1);

        // Between, wrong order.
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), lt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());
        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), lte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), eq(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), gte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), between(fldName, lower, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, upper, lower), between(fldName, upper, lower));

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testMultipleEqualsCriteria() {
        int lower = new Random().nextInt(CNT / 2);
        int upper = CNT / 2 + new Random().nextInt(CNT / 2 - 1);

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, upper), eq(fldName, lower), between(fldName, 0, CNT));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), eq(fldName, upper), between(fldName, 0, CNT));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), eq(fldName, upper), between(fldName, CNT, 0));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(eq(fldName, lower), eq(fldName, upper), between(fldName, CNT, 0));

        assertTrue(cache.query(qry).getAll().isEmpty());
    }

    /** */
    @Test
    public void testCommonBoundary() {
        int upper, lower = upper = new Random().nextInt(CNT / 2);

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), gt(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lt(fldName, lower), gte(fldName, upper));

        assertTrue(cache.query(qry).getAll().isEmpty());

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(lte(fldName, lower), gte(fldName, upper));

        check(cache.query(qry), lower, upper + 1);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(between(fldName, 0, lower), between(fldName, upper, CNT));

        check(cache.query(qry), lower, upper + 1);
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

        check(cache.query(qry), 0, min);

        qry = new IndexQuery<Integer, Person>(Person.class, idxName)
            .setCriteria(gtCriteria);

        check(cache.query(qry), max + 1, CNT);
    }

    /**
     * @param left  First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private <T> void check(QueryCursor<Cache.Entry<Integer, Person>> cursor, int left, int right) {
        List<Cache.Entry<Integer, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        Set<Integer> expKeys = IntStream.range(left, right).boxed().collect(Collectors.toSet());

        for (int i = 0; i < all.size(); i++) {
            Cache.Entry<Integer, Person> entry = all.get(i);

            assertTrue(expKeys.remove(entry.getKey()));
        }

        assertTrue(expKeys.isEmpty());
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
}
