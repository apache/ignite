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

package org.apache.ignite.sqltests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test base for test for sql features.
 */
public class BaseSqlTest extends GridCommonAbstractTest {
    /** Size of data in the test table. */
    private final static long EMP_CNT = 1000;

    /** Name of client node. */
    private static final String CLIENT_NODE_NAME = "clientNode";

    /** Name of the test table cache. */
    private static final String EMP_CACHE_NAME = "SQL_PUBLIC_EMPLOYEE";

    /** Client node instance. */
    private static IgniteEx client;

    /** Node name of second server. */
    private final String SRV2_NAME = "server2";

    /** Node name of first server. */
    private final String SRV1_NAME = "server1";

    protected static final String[] ALL_FIELDS = new String[] {"ID", "DEPID", "FIRSTNAME", "LASTNAME", "AGE"};

    @InjectTestSuite.Parameter
    private Configuration cfg = new Configuration();

    /**
     * Makes configuration for client node.
     */
    private IgniteConfiguration clientConfiguration() throws Exception {
        IgniteConfiguration clCfg = getConfiguration(CLIENT_NODE_NAME);

        clCfg.setClientMode(true);

        return optimize(clCfg);
    }

    /**
     * Creates common "Employee" table and fills it with data.
     *
     * @param withStr With clause for created table, such as "template=partitioned"
     */
    protected final void fillCommonData(String withStr) {
        execute("CREATE TABLE Employee (" +
            "id LONG PRIMARY KEY, " +
            "depId LONG, " +
            "firstName VARCHAR, " +
            "lastName VARCHAR, " +
            "age INT) " +
            (F.isEmpty(withStr) ? "" : " WITH \"" + withStr + '"') +
            ";");

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Employee VALUES (?, ?, ?, ?, ?)");

        final long depId = 42;

        Random rnd = new Random();

        for (long id = 0; id < EMP_CNT; id++) {
            String firstName = UUID.randomUUID().toString();
            String lastName = UUID.randomUUID().toString();
            Integer age = rnd.nextInt(50) + 18;

            execute(qry.setArgs(id, depId, firstName, lastName, age));
        }
    }

    /**
     * Sets up data. Override in children to add/change behaviour.
     */
    protected void fillData() {
        fillCommonData(""); // default.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV1_NAME, getConfiguration(SRV1_NAME), null);
        startGrid(SRV2_NAME, getConfiguration(SRV2_NAME), null);

        client = (IgniteEx)startGrid(CLIENT_NODE_NAME, clientConfiguration(), null);

        fillData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Assert that all returned results exactly matches values from cache.
     * Intended to verify "SELECT *" results.
     *
     * @param res result of "SELECT *..."
     * @param cache cache to verify values.
     */
    private void assertResultEqualToBinaryObjects(Result res, IgniteCache cache) {
        int idRowIdx = res.columnNames().indexOf("ID");

        assert idRowIdx >= 0 : "Column with name \"ID\" have not been found in column names " + res.columnNames();

        IgniteCache<Long, BinaryObject> binCache = cache.withKeepBinary();

        for (List<?> rowData : res.values()) {
            Map<String, Object> row = zipToMap(res.columnNames(), rowData);

            Long id = (Long)row.get("ID");

            BinaryObject cached = binCache.get(id);

            assertNotNull("Cache does not contain entry with id " + id, cached);

            assertEqualToBinaryObj(cached, row);
        }
    }

    protected Result checkedSelectAll(String selectQry, Ignite node, IgniteCache<?, ?> cache) {
        Result res = executeFrom(selectQry, node);

        assertResultEqualToBinaryObjects(res, cache);

        return res;

    }

    /**
     * Result of sql query. Contains metadata and all values in memory.
     */
    static class Result {

        /** Names of columns. */
        private List<String> colNames;
        /** Table */
        private List<List<?>> vals;

        /** */
        public Result(List<String> colNames, List<List<?>> vals) {
            this.colNames = colNames;
            this.vals = vals;
        }

        /**
         * @return metadata - name of columns.
         */
        public List<String> columnNames() {
            return colNames;
        }

        /**
         * @return table, the actual data.
         */
        public List<List<?>> values() {
            return vals;
        }

        /**
         * Creates result from cursor.
         *
         * @param cursor cursor to use to read column names and data.
         * @return Result that contains data and metadata, fetched from cursor.
         */
        public static Result fromCursor(FieldsQueryCursor<List<?>> cursor) {
            List<String> cols = readColNames(cursor);
            List<List<?>> vals = cursor.getAll();
            return new Result(cols, vals);
        }

    }

    /**
     * @param colons metadata: names of colons.
     * @param values actual values retrived from sql.
     * @return map colon name -> sql value.
     */
    private static Map<String, Object> zipToMap(List<String> colons, List<?> values) {
        assert colons.size() == values.size() : "incorrect row sizes of colons and values differ!";

        Iterator<String> colIt = colons.iterator();
        Iterator<?> valIt = values.iterator();

        Map<String, Object> res = new HashMap<>();

        while (colIt.hasNext())
            res.put(colIt.next(), valIt.next());

        assertTrue("Colon names contain duplicates.", res.size() == colons.size());

        return res;
    }

    /**
     * Assert that results are sorted by comparator.
     *
     * @param vals values to check.
     * @param cmp comparator to use.
     * @param <T> any type.
     */
    protected <T> void assertSortedBy(List<T> vals, Comparator<T> cmp) {
        Iterator<T> it = vals.iterator();
        if (!it.hasNext())
            return;

        T last = it.next();
        while (it.hasNext()) {
            T cur = it.next();
            if (cmp.compare(last, cur) > 0)
                throw new AssertionError("List is not sorted, element '" + last + "' is greater than '" +
                    cur + "'. List: " + vals);
        }
    }

    /**
     * Assert that result returned by sql is equal to binary object.
     * Useful with "SELECT *" when all the fields are returned.
     *
     * @param fromCache
     * @param fromSql
     */
    private static void assertEqualToBinaryObj(BinaryObject fromCache, Map<String, Object> fromSql) {
        Collection<String> binValCols = fromCache.type().fieldNames();

        ArrayList<String> allBinCols = new ArrayList<>(binValCols);
        allBinCols.add("ID");

        assertEquals("Returned sql columns count is not equal to binary object's one.",
            allBinCols.size(), fromSql.size());

        assertTrue("Column names are not the same in binary object and sql result.",
            allBinCols.containsAll(fromSql.keySet()));

        for (String colName : binValCols)
            assertEquals("Value for column " + colName + " in cache and in sql result differ.",
                fromCache.field(colName), fromSql.get(colName));
    }

    /**
     * Read colon names from cursor.
     *
     * @param cursor source of metadata.
     * @return List containing colon names.
     */
    private static List<String> readColNames(FieldsQueryCursor<?> cursor) {
        ArrayList<String> colNames = new ArrayList<>();

        for (int i = 0; i < cursor.getColumnsCount(); i++)
            colNames.add(cursor.getFieldName(i));

        return Collections.unmodifiableList(colNames);
    }

    /**
     * Shortcut for {@link #executeFrom(SqlFieldsQuery, Ignite)}, that has String argument.
     */
    protected Result executeFrom(String qry, Ignite node) {
        return executeFrom(new SqlFieldsQuery(qry), node);
    }

    /**
     * Shortcut for {@link #execute(SqlFieldsQuery)}.
     *
     * @param qry query string.
     * @return number of changed rows.
     */
    protected Result execute(String qry) {
        return executeFrom(new SqlFieldsQuery(qry), client);
    }

    /**
     * Performs query from client node.
     *
     * @param qry query.
     * @return number of changed rows.
     */
    protected Result execute(SqlFieldsQuery qry) {
        return executeFrom(qry, client);
    }

    /**
     * Execute query from node.
     *
     * @param qry query.
     * @param node node to use to perform query.
     * @return Result of query.
     */
    protected final Result executeFrom(SqlFieldsQuery qry, Ignite node) {
        FieldsQueryCursor<List<?>> cursor = ((IgniteEx)node).context().query().querySqlFields(qry, false);

        return Result.fromCursor(cursor);
    }

    /**
     * Shortcut for {@link #assertContainsEq(Collection, Collection)} without message.
     */
    protected void assertContainsEq(Collection actual, Collection expected) {
        assertContainsEq(null , actual, expected);
    }

    /**
     * Assert that collections contain the equal elements ({@link Object#equals(Object)}), ignoring the order.
     *
     * @param msg message to add if assert fails.
     * @param actual collection.
     * @param expected collection.
     */
    protected void assertContainsEq(String msg, Collection actual, Collection expected) {
        if (!F.isEmpty(msg))
            msg += " ";

        if (actual.size() != expected.size())
            throw new AssertionError(msg + "Collections contain different number of elements:" +
                " [actual=" + actual + ", expected=" + expected + "].");

        if (!actual.containsAll(expected))
            throw new AssertionError(msg + "Collections differ:" +
                " [actual=" + actual + ", expected=" + expected + "].");
    }

    /**
     * Performs scan query with fields projection.
     *
     * @param node node to use as entry point for query.
     * @param filter filter for rows.
     * @param fields to use in result (projection).
     */
    protected static List<List<Object>> select(
        Ignite node,
        @Nullable IgniteBiPredicate<Long, BinaryObject> filter,
        String... fields) {
        IgniteClosure<Cache.Entry<Long, BinaryObject>, List<Object>> transformer = e -> {
            List<Object> res = new ArrayList<>();

            BinaryObject val = e.getValue();

            for (String field : fields) {
                if (val.hasField(field))
                    res.add(val.field(field));
                else if (field.equals("ID"))
                    res.add(e.getKey());
                else
                    throw new AssertionError("Field with name " + field +
                        " not found in binary object " + val);
            }

            return res;
        };

        QueryCursor<List<Object>> cursor= node.cache(EMP_CACHE_NAME).withKeepBinary()
            .query(new ScanQuery<>(filter), transformer);

        return cursor.getAll();
    }

    /**
     * Make collection to be distinct - put all in Set.
     *
     * @param src collection.
     * @return Set with elements from {@code src} collection.
     */
    public static Set<Object> distinct(Collection<?> src) {
        return new HashSet<>(src);
    }

    protected void testAllNodes(Consumer<Ignite> consumer) {
        for (Ignite node : Ignition.allGrids()) {
            log.info("Testing on node " + node.name() + '.');

            consumer.accept(node);

            log.info("Testing on node " + node.name() + " is done.");
        }
    }

    public void testBasicSelect() {
        testAllNodes(node -> {
            Result emps = checkedSelectAll("SELECT * FROM Employee", node, node.cache(EMP_CACHE_NAME));

            assertEquals("Unexpected size of employees", EMP_CNT, emps.values().size());
        });
    }

    public void testSelectBetween() {
        testAllNodes(node -> {
            Result emps = checkedSelectAll("SELECT * FROM Employee e WHERE e.id BETWEEN 101 and 200", node, node.cache(EMP_CACHE_NAME));

            assertEquals("Fetched number of employees is incorrect", 100, emps.values().size());

            IgniteBiPredicate<Long, BinaryObject> between = (key, val) -> 101 <= key && key <= 200;

            String[] fields = emps.columnNames().toArray(new String[0]);

            List<List<Object>> expected = select(node, between, fields);

            assertContainsEq(emps.values(), expected);
        });
    }

    public void testSelectFields() {
        testAllNodes(node -> {
            Result res = executeFrom("SELECT firstName, id, age FROM Employee;", node);

            String[] fields = {"FIRSTNAME", "ID", "AGE"};

            assertEquals("Returned column names are incorrect.", res.columnNames(), Arrays.asList(fields));

            List<List<Object>> expected = select(node, null, fields);

            assertContainsEq(res.values(), expected);
        });
    }

    public void testEmptyBetween() {
        testAllNodes(node -> {
            Result emps = executeFrom("SELECT * FROM Employee e WHERE e.id BETWEEN 200 AND 101", node);

            assertTrue("SQL should have returned empty result set, but it have returned: " + emps, emps.values().isEmpty());
        });
    }

    public void testSelectOrderByLastName() {
        testAllNodes(node -> {
            Result result = checkedSelectAll("SELECT * FROM Employee e ORDER BY e.lastName", node, node.cache(EMP_CACHE_NAME));

            int lastNameIdx = result.columnNames().indexOf("LASTNAME");

            Comparator<List<?>> asc = Comparator.comparing((List<?> row) -> (String)row.get(lastNameIdx));
            assertSortedBy(result.values(), asc);
        });
    }

    public void testBasicDistinct() {
        testAllNodes(node -> {
            Result ages = executeFrom("SELECT DISTINCT age FROM Employee", node);

            Set<Object> expected = distinct(select(node, null, "age"));

            assertContainsEq("Values in cache differ from values returned from sql.", ages.values(), expected);
        });
    }

    public void testDistinctWithWhere() {
        testAllNodes(node -> {
            Result ages = executeFrom("SELECT DISTINCT age FROM Employee WHERE id < 100", node);

            Set<Object> expAges = distinct(select(node, (key, val) -> key < 100, "age"));

            assertContainsEq(ages.values(), expAges);
        });
    }

    public void testCfg() {
        // false by default, injected as true
        assertTrue(cfg.persistenceEnabled());
    }
}
