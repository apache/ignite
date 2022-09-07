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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.between;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gte;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.in;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lte;

/** */
public class IndexQueryInCriterionTest extends GridCommonAbstractTest {
    /** */
    private static final String IDX = "IDX";

    /** */
    private static final Map<Integer, Person> data = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryEntity qe = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .addQueryField("age", Integer.class.getName(), null)
            .setIndexes(Collections.singleton(
                new QueryIndex()
                    .setName(IDX)
                    .setFields(new LinkedHashMap<>(F.asMap("age", asc())))
            ));

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>()
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singleton(qe));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    protected boolean asc() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        try (IgniteDataStreamer<Integer, Person> stream = grid(0).dataStreamer("CACHE")) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++) {
                Person p = new Person(rnd.nextInt(100));

                stream.addData(i, p);

                data.put(i, p);
            }

            stream.addData(20_000, new Person(null));

            data.put(20_000, new Person(null));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSingleValueInCriterion() {
        for (int i = -1; i <= 100; i++) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", Collections.singleton(i)));

            final int age = i;

            assertExpect(qry, (k, p) -> p.age == age);
        }
    }

    /** */
    @Test
    public void testTwoValuesInCriterion() {
        for (int i = -1; i <= 100; i++) {
            for (int j = -1; j <= 100; j++) {
                IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                    .setCriteria(in("age", F.asList(i, j)));

                final int ageFirst = i;
                final int ageSecond = j;

                assertExpect(i + " " + j, qry, (k, p) -> p.age == ageFirst || p.age == ageSecond);
            }
        }
    }

    /** */
    @Test
    public void testMultipleInCriterion() {
        List<Integer> arr = new ArrayList<>();

        for (int i = -1; i <= 100; i++)
            arr.add(i);

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(in("age", arr));

        assertExpect(qry, (k, p) -> true);
    }

    /** */
    @Test
    public void testSingleInWithSingleRangeCriterion() {
        List<IndexQueryCriterion> criteria = F.asList(
            lt("age", 50),
            lte("age", 10),
            eq("age", 10),
            gte("age", 10),
            gt("age", 0));

        for (IndexQueryCriterion c: criteria) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", Collections.singleton(10)), c);

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c, in("age", Collections.singleton(10)));

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10);
        }
    }

    /** */
    @Test
    public void testSingleInWithMultipleRangeCriterion() {
        List<IndexQueryCriterion> criteria = F.asList(
            lt("age", 50),
            lte("age", 10),
            eq("age", 10),
            gte("age", 10),
            gt("age", 0));

        List<IndexQueryCriterion[]> rangeCritPair = new ArrayList<>();

        for (IndexQueryCriterion i: criteria) {
            for (IndexQueryCriterion j: criteria) {
                IndexQueryCriterion[] c = new IndexQueryCriterion[2];

                c[0] = i;
                c[1] = j;

                rangeCritPair.add(c);
            }
        }

        for (IndexQueryCriterion[] c: rangeCritPair) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", Collections.singleton(10)), c[0], c[1]);

            assertExpect(qry, (k, p) -> p.age == 10);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c[0], in("age", Collections.singleton(10)), c[1]);

            assertExpect(qry, (k, p) -> p.age == 10);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c[0], in("age", Collections.singleton(10)), c[0], c[1]);

            assertExpect(qry, (k, p) -> p.age == 10);
        }
    }

    /** */
    @Test
    public void testMultipleInWithSingleRangeCriterion() {
        List<IndexQueryCriterion> criteria = F.asList(
            lt("age", 50),
            lte("age", 20),
            between("age", 10, 20),
            gte("age", 10),
            gt("age", 0));

        for (IndexQueryCriterion c: criteria) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", F.asList(10, 20)), c);

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10 || p.age == 20);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c, in("age", F.asList(10, 20)));

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10 || p.age == 20);
        }
    }

    /** */
    @Test
    public void testMultipleInWithSingleRangeCriterionCrossing() {
        List<IndexQueryCriterion> firstOnlyCriteria = F.asList(
            lte("age", 10),
            lte("age", 15),
            lt("age", 15),
            lt("age", 20),
            between("age", 5, 15),
            between("age", 10, 15),
            between("age", 5, 10),
            eq("age", 10));

        for (IndexQueryCriterion c: firstOnlyCriteria) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", F.asList(10, 20)), c);

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c, in("age", F.asList(10, 20)));

            assertExpect(c.toString(), qry, (k, p) -> p.age == 10);
        }

        if (true)
            return;

        List<IndexQueryCriterion> secondOnlyCriteria = F.asList(
            gte("age", 15),
            gte("age", 20),
            gt("age", 10),
            gt("age", 15),
            between("age", 15, 25),
            between("age", 20, 30),
            between("age", 15, 20),
            eq("age", 20));

        for (IndexQueryCriterion c: secondOnlyCriteria) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", F.asList(10, 20)), c);

            assertExpect(c.toString(), qry, (k, p) -> p.age == 20);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c, in("age", F.asList(10, 20)));

            assertExpect(c.toString(), qry, (k, p) -> p.age == 20);
        }
    }

    /** */
    @Test
    public void testExplcitiInCriterionWithNullVal() {
        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(in("age", Collections.singleton(null)));

        assertExpect(qry, (k, p) -> p.age == null, true);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(in("age", F.asList(null, 10)));

        assertExpect(qry, (k, p) -> p.age == null || p.age == 10, true);
    }

    /** */
    @Test
    public void testDuplicatedInCriterionFailure() {
        List<IndexQueryCriterion[]> criteria = F.asList(
            new IndexQueryCriterion[] {
                in("age", Collections.singleton(10)),
                in("age", Collections.singleton(20))
            },
            new IndexQueryCriterion[] {
                in("age", Collections.singleton(10)),
                in("age", Collections.singleton(10))
            },
            new IndexQueryCriterion[] {
                in("age", Collections.singleton(10)),
                lt("age", 100),
                in("age", Collections.singleton(20))
            },
            new IndexQueryCriterion[] {
                in("age", Collections.singleton(10)),
                lt("age", 100),
                in("age", Collections.singleton(10))
            },
            new IndexQueryCriterion[] {
                lt("age", 100),
                in("age", Collections.singleton(10)),
                in("age", Collections.singleton(20))
            },
            new IndexQueryCriterion[] {
                lt("age", 100),
                in("age", Collections.singleton(10)),
                in("age", Collections.singleton(10))
            });

        for (IndexQueryCriterion[] c: criteria) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c);

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry).getAll(),
                IgniteCheckedException.class,
                "Multiple IN criteria for same field arent't supported."
            );
        }
    }

    /** */
    @Test
    public void testSingleInCriteriaAndRangeCriteriaNoCross() {
        List<IndexQueryCriterion> criteria = F.asList(
            lt("age", 10),
            lt("age", 5),
            lte("age", 5),
            eq("age", 5),
            eq("age", 20),
            between("age", 0, 5),
            between("age", 15, 20),
            gt("age", 10),
            gt("age", 15),
            gte("age", 20)
        );

        for (IndexQueryCriterion c: criteria) {
            IndexQuery<Integer, Person> qry1 = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", Collections.singleton(10)), c);

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry1).getAll(),
                IgniteCheckedException.class,
                "Failed to merge criterion"
            );

            IndexQuery<Integer, Person> qry2 = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c, in("age", Collections.singleton(10)));

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry2).getAll(),
                IgniteCheckedException.class,
                "Failed to merge criterion"
            );
        }
    }

    /** */
    @Test
    public void testMultipleInCriteriaAndRangeCriteriaNoCross() {
        List<IndexQueryCriterion[]> criteria = F.asList(
            new IndexQueryCriterion[] { lt("age", 15), lt("age", 5) },
            new IndexQueryCriterion[] { lt("age", 10), lte("age", 10) },
            new IndexQueryCriterion[] { lt("age", 10), gt("age", 5) },
            new IndexQueryCriterion[] { lt("age", 10), gte("age", 5) },
            new IndexQueryCriterion[] { lt("age", 10), between("age", 5, 15) },
            new IndexQueryCriterion[] { lt("age", 10), gte("age", 10) },
            new IndexQueryCriterion[] { lte("age", 10), gt("age", 10) },
            new IndexQueryCriterion[] { lt("age", 10), gt("age", 10) }
        );

        for (IndexQueryCriterion[] c: criteria) {
            IndexQuery<Integer, Person> qry1 = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("age", Collections.singleton(10)), c[0], c[1]);

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry1).getAll(),
                IgniteCheckedException.class,
                "Failed to merge"
            );

            IndexQuery<Integer, Person> qry2 = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c[0], in("age", Collections.singleton(10)), c[1]);

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry2).getAll(),
                IgniteCheckedException.class,
                "Failed to merge"
            );

            IndexQuery<Integer, Person> qry3 = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(c[0], c[1], in("age", Collections.singleton(10)));

            GridTestUtils.assertThrows(
                log,
                () -> grid(0).cache("CACHE").query(qry3).getAll(),
                IgniteCheckedException.class,
                "Failed to merge"
            );
        }
    }

    /** */
    @Test
    public void testOnlySecondFieldCriterionFailed() {
        IndexQuery<Integer, Person> idxQry = new IndexQuery<Integer, Person>(Person.class, IDX)
            .setCriteria(in("_KEY", Collections.singleton(0)));

        GridTestUtils.assertThrows(
            log,
            () -> grid(0).cache("CACHE").query(idxQry).getAll(),
            IgniteCheckedException.class,
            "Index doesn't match criteria"
        );
    }

    /** */
    @Test
    public void testSingleInCriterionOnSecondField() {
        for (int i = -1; i <= 100; i++) {
            final int key = i;

            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(gte("age", 0), in("_KEY", Collections.singleton(i)));

            assertExpect(String.valueOf(key), qry, (k, p) -> k == key);

            qry = new IndexQuery<Integer, Person>(Person.class)
                .setCriteria(in("_KEY", Collections.singleton(i)), gte("age", 0));

            assertExpect(String.valueOf(key), qry, (k, p) -> k == key);
        }
    }

    /** */
    @Test
    public void testMultipleInCriterionOnSecondField() {
        for (int i = -1; i < 10; i++) {
            for (int j = -1; j < 10; j++) {
                final int firstKey = i;
                final int secondKey = j;

                IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                    .setCriteria(gte("age", 0), in("_KEY", F.asList(i, j)));

                assertExpect(qry, (k, p) -> k == firstKey || k == secondKey);

                qry = new IndexQuery<Integer, Person>(Person.class)
                    .setCriteria(in("_KEY", F.asList(i, j)), gte("age", 0));

                assertExpect(qry, (k, p) -> k == firstKey || k == secondKey);
            }
        }
    }

    /** */
    @Test
    public void testMultipleInWithRangeCrossCriterionOnSecondField() {
        Random rnd = new Random();

        int keyFirst = 0;
        int keySecond = 0;

        int ageFirst = 0;
        int ageSecond = 0;

        while (ageFirst >= ageSecond) {
            keyFirst = rnd.nextInt(10_000);
            keySecond = rnd.nextInt(10_000);

            ageFirst = data.get(keyFirst).age;
            ageSecond = data.get(keySecond).age;
        }

        final int firstKey = keyFirst;
        final int secondKey = keySecond;

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(lt("age", ageSecond), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(lte("age", ageSecond), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey || k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(lte("age", ageFirst), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(gt("age", ageFirst), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(gte("age", ageFirst), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey || k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(gte("age", ageSecond), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(between("age", ageFirst, ageSecond), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey || k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(between("age", ageFirst + 1, ageSecond), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == secondKey);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setCriteria(between("age", ageFirst, ageSecond - 1), in("_KEY", F.asList(keyFirst, keySecond)));

        assertExpect(qry, (k, p) -> k == firstKey);
    }

    /** */
    private void assertExpect(IndexQuery<Integer, Person> qry, IgniteBiPredicate<Integer, Person> expectFunc) {
        assertExpect(qry, expectFunc, false);
    }

    /** */
    private void assertExpect(String msg, IndexQuery<Integer, Person> qry, IgniteBiPredicate<Integer, Person> expectFunc) {
        assertExpect(msg, qry, expectFunc, false);
    }

    /** */
    private void assertExpect(IndexQuery<Integer, Person> qry, IgniteBiPredicate<Integer, Person> expectFunc, boolean checkNull) {
        assertExpect("", qry, expectFunc, checkNull);
    }

    /** */
    private void assertExpect(
        String msg,
        IndexQuery<Integer, Person> qry,
        IgniteBiPredicate<Integer, Person> expectFunc,
        boolean checkNull
    ) {
        Map<Integer, Person> expect = new HashMap<>();

        data.forEach((k, v) -> {
            if ((checkNull || v.age != null) && expectFunc.apply(k, v))
                expect.put(k, v);
        });

        int expSize = expect.size();

        grid(0).cache("CACHE").query(qry).getAll().forEach(e ->
            assertTrue(
                msg + " : " + e.toString() + " expSize=" + expSize + " currSize=" + expect.size(),
                expect.remove(e.getKey(), e.getValue()))
        );

        assertTrue(expect.toString(), expect.isEmpty());
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        final Integer age;

        /** */
        Person(Integer age) {
            this.age = age;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return Objects.equals(age, ((Person)o).age);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return age;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
