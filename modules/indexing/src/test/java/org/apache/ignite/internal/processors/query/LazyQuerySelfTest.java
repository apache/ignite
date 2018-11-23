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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for lazy query execution.
 */
public class LazyQuerySelfTest extends GridCommonAbstractTest {
    /** Keys ocunt. */
    private static final int KEY_CNT = 200;

    /** Base query argument. */
    private static final int BASE_QRY_ARG = 50;

    /** Size for small pages. */
    private static final int PAGE_SIZE_SMALL = 12;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test local query execution.
     *
     * @throws Exception If failed.
     */
    public void testSingleNode() throws Exception {
        checkSingleNode(1);
    }

    /**
     * Test local query execution.
     *
     * @throws Exception If failed.
     */
    public void testSingleNodeWithParallelism() throws Exception {
        checkSingleNode(4);
    }

    /**
     * Test query execution with multiple topology nodes.
     *
     * @throws Exception If failed.
     */
    public void testMultipleNodes() throws Exception {
        checkMultipleNodes(1);
    }

    /**
     * Test query execution with multiple topology nodes with query parallelism.
     *
     * @throws Exception If failed.
     */
    public void testMultipleNodesWithParallelism() throws Exception {
        checkMultipleNodes(4);
    }

    /**
     * Check local query execution.
     *
     * @param parallelism Query parallelism.
     * @throws Exception If failed.
     */
    public void checkSingleNode(int parallelism) throws Exception {
        Ignite srv = startGrid();

        srv.createCache(cacheConfiguration(parallelism));

        populateBaseQueryData(srv);

        checkBaseOperations(srv);
    }

    /**
     * Check query execution with multiple topology nodes.
     *
     * @param parallelism Query parallelism.
     * @throws Exception If failed.
     */
    public void checkMultipleNodes(int parallelism) throws Exception {
        Ignite srv1 = startGrid(1);
        Ignite srv2 = startGrid(2);

        Ignite cli;

        try {
            Ignition.setClientMode(true);

            cli = startGrid(3);
        }
        finally {
            Ignition.setClientMode(false);
        }

        cli.createCache(cacheConfiguration(parallelism));

        populateBaseQueryData(cli);

        checkBaseOperations(srv1);
        checkBaseOperations(srv2);
        checkBaseOperations(cli);

        // Test originating node leave.
        FieldsQueryCursor<List<?>> cursor = execute(cli, baseQuery().setPageSize(PAGE_SIZE_SMALL));

        Iterator<List<?>> iter = cursor.iterator();

        for (int i = 0; i < 30; i++)
            iter.next();

        stopGrid(3);

        assertNoWorkers();

        // Test server node leave with active worker.
        cursor = execute(srv1, baseQuery().setPageSize(PAGE_SIZE_SMALL));

        try {
            iter = cursor.iterator();

            for (int i = 0; i < 30; i++)
                iter.next();

            stopGrid(2);
        }
        finally {
            cursor.close();
        }

        assertNoWorkers();
    }

    /**
     * Check base operations.
     *
     * @param node Node.
     * @throws Exception If failed.
     */
    private void checkBaseOperations(Ignite node) throws Exception {
        // Get full data.
        List<List<?>> rows = execute(node, baseQuery()).getAll();

        assertBaseQueryResults(rows);
        assertNoWorkers();

        // Get data in several pages.
        rows = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL)).getAll();

        assertBaseQueryResults(rows);
        assertNoWorkers();

        // Test full iteration.
        rows = new ArrayList<>();

        FieldsQueryCursor<List<?>> cursor = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL));

        for (List<?> row : cursor)
            rows.add(row);

        assertBaseQueryResults(rows);
        assertNoWorkers();

        // Test partial iteration with cursor close.
        try (FieldsQueryCursor<List<?>> partialCursor = execute(node, baseQuery().setPageSize(PAGE_SIZE_SMALL))) {
            Iterator<List<?>> iter = partialCursor.iterator();

            for (int i = 0; i < 30; i++)
                iter.next();
        }

        assertNoWorkers();

        // Test execution of multiple queries at a time.
        List<Iterator<List<?>>> iters = new ArrayList<>();

        for (int i = 0; i < 200; i++)
            iters.add(execute(node, randomizedQuery().setPageSize(PAGE_SIZE_SMALL)).iterator());

        while (!iters.isEmpty()) {
            Iterator<Iterator<List<?>>> iterIter = iters.iterator();

            while (iterIter.hasNext()) {
                Iterator<List<?>> iter = iterIter.next();

                int i = 0;

                while (iter.hasNext() && i < 20) {
                    iter.next();

                    i++;
                }

                if (!iter.hasNext())
                    iterIter.remove();
            }
        }

        assertNoWorkers();
    }

    /**
     * Populate base query data.
     *
     * @param node Node.
     */
    private static void populateBaseQueryData(Ignite node) {
        IgniteCache<Long, Person> cache = cache(node);

        for (long i = 0; i < KEY_CNT; i++)
            cache.put(i, new Person(i));
    }

    /**
     * @return Query with randomized argument.
     */
    private static SqlFieldsQuery randomizedQuery() {
        return query(ThreadLocalRandom.current().nextInt(KEY_CNT / 2));
    }

    /**
     * @return Base query.
     */
    private static SqlFieldsQuery baseQuery() {
        return query(BASE_QRY_ARG);
    }

    /**
     * @param parallelism Query parallelism.
     * @return Default cache configuration.
     */
    private static CacheConfiguration<Long, Person> cacheConfiguration(int parallelism) {
        return new CacheConfiguration<Long, Person>().setName(CACHE_NAME).setIndexedTypes(Long.class, Person.class)
            .setQueryParallelism(parallelism);
    }

    /**
     * Default query.
     *
     * @param arg Argument.
     * @return Query.
     */
    private static SqlFieldsQuery query(long arg) {
        return new SqlFieldsQuery("SELECT id, name FROM Person WHERE id >= ?").setArgs(arg);
    }

    /**
     * Assert base query results.
     *
     * @param rows Result rows.
     */
    private static void assertBaseQueryResults(List<List<?>> rows) {
        assertEquals(KEY_CNT - BASE_QRY_ARG, rows.size());

        for (List<?> row : rows) {
            Long id = (Long)row.get(0);
            String name = (String)row.get(1);

            assertTrue(id >= BASE_QRY_ARG);
            assertEquals(nameForId(id), name);
        }
    }

    /**
     * Get cache for node.
     *
     * @param node Node.
     * @return Cache.
     */
    private static IgniteCache<Long, Person> cache(Ignite node) {
        return node.cache(CACHE_NAME);
    }

    /**
     * Execute query on the given cache.
     *
     * @param node Node.
     * @param qry Query.
     * @return Cursor.
     */
    private static FieldsQueryCursor<List<?>> execute(Ignite node, SqlFieldsQuery qry) {
        return cache(node).query(qry.setLazy(true));
    }

    /**
     * Make sure that are no active lazy workers.
     *
     * @throws Exception If failed.
     */
    private static void assertNoWorkers() throws Exception {
        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite node : Ignition.allGrids()) {
                    IgniteH2Indexing idx = (IgniteH2Indexing) ((IgniteKernal)node).context().query().getIndexing();

                    if (idx.mapQueryExecutor().registeredLazyWorkers() != 0)
                        return false;
                }

                return MapQueryLazyWorker.activeCount() == 0;
            }
        }, 1000L);
    }

    /**
     * Get name for ID.
     *
     * @param id ID.
     * @return Name.
     */
    private static String nameForId(long id) {
        return "name-" + id;
    }

    /**
     * Person class.
     */
    private static class Person {
        /** ID. */
        @QuerySqlField(index = true)
        private long id;

        /** Name. */
        @QuerySqlField
        private String name;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public Person(long id) {
            this.id = id;
            this.name = nameForId(id);
        }

        /**
         * @return ID.
         */
        public long id() {
            return id;
        }

        /**
         * @return Name.
         */
        public String name() {
            return name;
        }
    }
}
