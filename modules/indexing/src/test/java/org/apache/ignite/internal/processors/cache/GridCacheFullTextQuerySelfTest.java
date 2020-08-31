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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * FullTest queries left test.
 */
public class GridCacheFullTextQuerySelfTest extends GridCommonAbstractTest {
    /** Cache size. */
    private static final int MAX_ITEM_COUNT = 100;

    /** Cache name */
    private static final String PERSON_CACHE = "Person";

    /** Limitation to query response size */
    private static final int QUERY_LIMIT = 5;

    /** Concurrent threads number */
    private static final int N_THREADS = 20;

    /**
     * Container for expected values and all available entries
     */
    private static final class TestPair {
        /** */
        public final Set<Integer> expected;

        /** */
        public final List<Cache.Entry<Integer, ?>> all = new ArrayList<>();

        /**
         * Constructor
         *
         * @param exp expected values set.
         * */
        public TestPair(Set<Integer> exp) {
            this.expected = new HashSet<>(exp);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes();

        cfg.setConnectorConfiguration(null);

        CacheConfiguration<Integer, Person> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(PERSON_CACHE)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(0)
            .setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryWithField() throws Exception {
        checkTextQuery("name:1*", 0, false, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryWithFieldLimited() throws Exception {
        checkTextQuery("name:1*", QUERY_LIMIT, false, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testLocalTextQueryWithKeepBinary() throws Exception {
        checkTextQuery(true, true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testLocalTextQuery() throws Exception {
        checkTextQuery(true, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testLocalTextQueryLimited() throws Exception {
        checkTextQuery(null, QUERY_LIMIT, true, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryWithKeepBinary() throws Exception {
        checkTextQuery(false, true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryWithKeepBinaryLimited() throws Exception {
        checkTextQuery(null, QUERY_LIMIT, false, true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQuery() throws Exception {
        checkTextQuery(false, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryLimited() throws Exception {
        checkTextQuery(null, QUERY_LIMIT, false, false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testTextQueryLimitedConcurrent() throws Exception {
        final IgniteEx ignite = grid(0);

        final String clause = "1*";

        // 1. Populate cache with data, calculating expected count in parallel.
        Set<Integer> exp = populateCache(ignite, false, MAX_ITEM_COUNT, (IgnitePredicate<Integer>)x -> String.valueOf(x).startsWith("1"));

        GridTestUtils.runMultiThreaded(textQueryTask(ignite, clause, exp), N_THREADS, "text-query-test");

        clearCache(ignite);
    }

    /**
     * Creates Runnable for TextQuery
     *
     * @param ignite Ignite insance.
     * @param clause Query clause.
     * @param exp Expected results for validation.
     * @return TextQuery and validation wrapped into Runnable functional interface.
     */
    @NotNull private Runnable textQueryTask(IgniteEx ignite, String clause, Set<Integer> exp) {
        return () -> {
            try {
                TextQuery qry = new TextQuery<>(Person.class, clause);
                validateQueryResults(ignite, qry, exp, false);
            }
            catch (Exception e) {
                fail(e.getMessage());
            }
        };
    }

    /**
     * @param loc local query flag.
     * @param keepBinary keep binary flag.
     */
    private void checkTextQuery(boolean loc, boolean keepBinary) throws Exception {
        checkTextQuery(null, 0, loc, keepBinary);
    }

    /**
     * @param clause Query clause.
     * @param limit limits response size
     * @param loc local query flag.
     * @param keepBinary keep binary flag.
     */
    private void checkTextQuery(String clause, int limit, boolean loc, boolean keepBinary) throws Exception {
        final IgniteEx ignite = grid(0);

        if (F.isEmpty(clause))
            clause = "1*";

        // 1. Populate cache with data, calculating expected count in parallel.
        Set<Integer> exp = populateCache(ignite, loc, MAX_ITEM_COUNT, new IgnitePredicate<Integer>() {
            @Override public boolean apply(Integer x) {
                return String.valueOf(x).startsWith("1");
            }
        });

        // 2. Validate results.
        TextQuery qry = new TextQuery<>(Person.class, clause).setLocal(loc).setLimit(limit);

        validateQueryResults(ignite, qry, exp, keepBinary);

        clearCache(ignite);
    }

    /**
     * Clear cache with check.
     */
    private static void clearCache(IgniteEx ignite) {
        IgniteCache<Integer, Person> cache = ignite.cache(PERSON_CACHE);

        cache.clear();

        List all = cache.query(new TextQuery<>(Person.class, "1*")).getAll();

        assertTrue(all.isEmpty());
    }

    /**
     * Fill cache.
     *
     * @throws IgniteCheckedException if failed.
     */
    private static Set<Integer> populateCache(IgniteEx ignite, boolean loc, int cnt,
        IgnitePredicate<Integer> expectedEntryFilter) throws IgniteCheckedException {
        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        assertNotNull(cache);

        Random rand = new Random();

        HashSet<Integer> exp = new HashSet<>();

        Affinity<Integer> aff = cache.affinity();

        ClusterNode localNode = cache.context().localNode();

        for (int i = 0; i < cnt; i++) {
            int val = rand.nextInt(cnt);

            cache.put(val, new Person(String.valueOf(val), val));

            if (expectedEntryFilter.apply(val) && (!loc || aff.isPrimary(localNode, val)))
                exp.add(val);
        }

        return exp;
    }

    /**
     * Check query results.
     *
     * @throws IgniteCheckedException if failed.
     */
    private static void validateQueryResults(IgniteEx ignite, TextQuery qry, Set<Integer> exp,
        boolean keepBinary) throws IgniteCheckedException {
        IgniteCache<Integer, Person> cache = ignite.cache(PERSON_CACHE);

        if (keepBinary) {
            IgniteCache<Integer, BinaryObject> cache0 = cache.withKeepBinary();

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache0.query(qry)) {

                TestPair testPair = processExpectedWithBinary(exp, cursor);

                assertResult(ignite, qry, testPair);
            }

            try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache0.query(qry)) {

                TestPair testPair = processExpectedWithBinary(exp, cursor);

                assertResult(ignite, qry, testPair);
            }
        }
        else {
            try (QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(qry)) {

                TestPair testPair = processExpected(exp, cursor);

                assertResult(ignite, qry, testPair);

            }

            try (QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(qry)) {

                TestPair testPair = processExpected(exp, cursor);

                assertResult(ignite, qry, testPair);
            }
        }
    }

    /**
     * Checks query for missed keys or if limit is set - for limitation correctness.
     *
     * @param ignite Ignite context.
     * @param qry Initial text query.
     * @param testPair pair containing expected and all entries.
     * @throws IgniteCheckedException if key check failed.
     */
    private static void assertResult(IgniteEx ignite, TextQuery qry,
        TestPair testPair) throws IgniteCheckedException {
        if (qry.getLimit() > 0)
            assertTrue(testPair.all.size() <= QUERY_LIMIT);
        else
            checkForMissedKeys(ignite, testPair.expected, testPair.all);
    }

    /**
     * Checks cursor with binary entries for correct keys and values.
     * Removes valid entries from expected list copy.
     *
     * @param cursor Query cursor with response
     * @param exp List of expected values.
     * @return Altered expected values list.
     */
    @NotNull private static GridCacheFullTextQuerySelfTest.TestPair processExpectedWithBinary(Set<Integer> exp,
        QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor) {
        TestPair testPair = new TestPair(exp);

        for (Cache.Entry<Integer, BinaryObject> entry : cursor.getAll()) {
            testPair.all.add(entry);

            assertEquals(entry.getKey().toString(), entry.getValue().field("name"));

            assertEquals(entry.getKey(), entry.getValue().field("age"));

            testPair.expected.remove(entry.getKey());
        }
        return testPair;
    }

    /**
     * Checks cursor entries for correct keys and values.
     * Removes valid entries from expected list copy.
     *
     * @param cursor Query cursor with response
     * @param exp List of expected values.
     * @return Altered expected values list.
     */
    @NotNull private static GridCacheFullTextQuerySelfTest.TestPair processExpected(Set<Integer> exp,
        QueryCursor<Cache.Entry<Integer, Person>> cursor) {
        TestPair testPair = new TestPair(exp);

        for (Cache.Entry<Integer, Person> entry : cursor.getAll()) {
            testPair.all.add(entry);

            assertEquals(entry.getKey().toString(), entry.getValue().name);

            assertEquals(entry.getKey().intValue(), entry.getValue().age);

            testPair.expected.remove(entry.getKey());
        }
        return testPair;
    }

    /**
     * Check if there is missed keys.
     *
     * @throws IgniteCheckedException if failed.
     */
    private static void checkForMissedKeys(IgniteEx ignite, Collection<Integer> exp,
        List<Cache.Entry<Integer, ?>> all) throws IgniteCheckedException {
        if (exp.size() == 0)
            return;

        IgniteInternalCache<Integer, Person> cache = ignite.cachex(PERSON_CACHE);

        assertNotNull(cache);

        StringBuilder sb = new StringBuilder();

        Affinity<Integer> aff = cache.affinity();

        for (Integer key : exp) {
            Integer part = aff.partition(key);

            sb.append(
                String.format("Query did not return expected key '%d' (exists: %s), partition '%d', partition nodes: ",
                    key, cache.get(key) != null, part));

            Collection<ClusterNode> partNodes = aff.mapPartitionToPrimaryAndBackups(part);

            for (ClusterNode node : partNodes)
                sb.append(node).append("  ");

            sb.append(";\n");
        }

        sb.append("Returned keys: ");

        for (Cache.Entry e : all)
            sb.append(e.getKey()).append(" ");

        sb.append(";\n");

        fail(sb.toString());
    }

    /**
     * Test model class.
     */
    public static class Person implements Serializable {
        /** */
        @QueryTextField
        String name;

        /** */
        @QuerySqlField(index = true)
        int age;

        /** */
        @QuerySqlField final Date birthday;

        /**
         * Constructor
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age % 2000;

            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.YEAR, -age);

            birthday = cal.getTime();
        }
    }
}
