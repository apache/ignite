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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test base for test for sql features.
 */
public class BaseSqlTest extends GridCommonAbstractTest {
    /** Number of all employees. */
    public final static long EMP_CNT = 1000L;

    /** Number of all departments. */
    public final static long DEP_CNT = 50L;

    /** Number of all addresses. */
    public final static long ADDR_CNT = 500L;

    /** Number of employees that aren't associated with any department. */
    public final static long FREE_EMP_CNT = 50;

    /** Number of departments that don't have employees and addresses. */
    public final static long FREE_DEP_CNT = 5;

    /** Number of conferences that don't have participants. */
    public final static long FREE_ADDR_CNT = 30;

    /** Number of possible age values (width of ages values range). */
    public final static int AGES_CNT = 50;

    /** Name of client node. */
    public static final String CLIENT_NODE_NAME = "clientNode";

    /** Name of the Employee table cache. */
    public static final String EMP_CACHE_NAME = "SQL_PUBLIC_EMPLOYEE";

    /** Name of the Department table cache. */
    public static final String DEP_CACHE_NAME = "SQL_PUBLIC_DEPARTMENT";

    /** Name of the Address table cache. */
    public static final String ADDR_CACHE_NAME = "SQL_PUBLIC_ADDRESS";

    /** Client node instance. */
    protected static IgniteEx client;

    /** Node name of second server. */
    public final String SRV2_NAME = "server2";

    /** Node name of first server. */
    public final String SRV1_NAME = "server1";

    public static final String[] ALL_EMP_FIELDS = new String[] {"ID", "DEPID", "DEPIDNOIDX", "FIRSTNAME", "LASTNAME", "AGE", "SALARY"};

    /** Flag that forces to do explain query in log before performing actual query. */
    public static boolean explain = false;

    /** Configuration that is injected by Suite */
    @InjectTestSuite.Parameter
    private Configuration cfg = new Configuration();

    /** Random for generator. */
    private Random rnd = new Random();

    /**
     * Makes configuration for client node.
     */
    private IgniteConfiguration clientConfiguration() throws Exception {
        IgniteConfiguration clCfg = getConfiguration(CLIENT_NODE_NAME);

        clCfg.setClientMode(true);

        return optimize(clCfg);
    }

    /**
     * Fills tables with data.
     */
    protected void fillCommonData() {
        SqlFieldsQuery insDep = new SqlFieldsQuery("INSERT INTO Department VALUES (?, ?, ?)");

        SqlFieldsQuery insEmp = new SqlFieldsQuery("INSERT INTO Employee VALUES (?, ?, ?, ?, ?, ?, ?)");

        SqlFieldsQuery insConf = new SqlFieldsQuery("INSERT INTO Address VALUES (?, ?, ?, ?)");

        for (long id = 0; id < DEP_CNT; id++) {
            String name = UUID.randomUUID().toString();

            execute(insDep.setArgs(id, id, name));
        }

        for (long id = 0; id < EMP_CNT; id++) {
            Long depId = (long)rnd.nextInt((int)(DEP_CNT - FREE_DEP_CNT));

            if (id < FREE_EMP_CNT)
                depId = null;

            String firstName = UUID.randomUUID().toString();
            String lastName = UUID.randomUUID().toString();

            Integer age = rnd.nextInt(AGES_CNT) + 18;
            Integer salary = rnd.nextInt(50) + 50;

            execute(insEmp.setArgs(id, depId, depId, firstName, lastName, age, salary));
        }

        for (long addrId = 0; addrId < ADDR_CNT; addrId++) {
            Long depId = (long)rnd.nextInt((int)(DEP_CNT - FREE_DEP_CNT));

            if (addrId < FREE_ADDR_CNT)
                depId = null;

            String address = UUID.randomUUID().toString();

            execute(insConf.setArgs(addrId, depId, depId, address));
        }
    }

    /**
     * Creates common tables.
     *
     * @param commonParams Common parameters for the with clause (of CREATE TABLE), such as "template=partitioned".
     */
    protected final void createTables(String commonParams) {
        createEmployeeTable(commonParams);

        createDepartmentTable(commonParams);

        createAddressTable(commonParams);
    }

    protected void createAddressTable(String commonParams) {
        execute("CREATE TABLE Address (" +
            "id LONG PRIMARY KEY, " +
            "depId LONG, " +
            "depIdNoidx LONG, " +
            "address VARCHAR" +
            ")" +
            (F.isEmpty(commonParams) ? "" : " WITH \"" + commonParams + "\"") +
            ";");

        execute("CREATE INDEX depIndex ON Address (depId)");
    }

    protected void createDepartmentTable(String commonParams) {
        execute("CREATE TABLE Department (" +
            "id LONG PRIMARY KEY," +
            "idNoidx LONG, " +
            "name VARCHAR" +
            ") " +
            (F.isEmpty(commonParams) ? "" : " WITH \"" + commonParams + "\"") +
            ";");
    }

    protected void createEmployeeTable(String commonParams) {
        execute("CREATE TABLE Employee (" +
            "id LONG, " +
            "depId LONG, " +
            "depIdNoidx LONG," +
            "firstName VARCHAR, " +
            "lastName VARCHAR, " +
            "age INT, " +
            "salary INT, " +
            "PRIMARY KEY (id, depId)" +
            ") " +
            "WITH \"affinity_key=depId" + (F.isEmpty(commonParams) ? "" : ", " + commonParams) + "\"" +
            ";");

        execute("CREATE INDEX AgeIndex ON Employee (age)");
    }

    /**
     * Sets up data. Override in children to add/change behaviour.
     */
    protected void setupData() {
        createTables(""); // default.

        fillCommonData();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV1_NAME, getConfiguration(SRV1_NAME), null);
        startGrid(SRV2_NAME, getConfiguration(SRV2_NAME), null);

        client = (IgniteEx)startGrid(CLIENT_NODE_NAME, clientConfiguration(), null);

        boolean locExp = explain;
        explain = false;

        setupData();

        explain = locExp;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
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
        if (explain) {
            try {
                SqlFieldsQuery explainQry = new SqlFieldsQuery(qry).setSql("EXPLAIN " + qry.getSql());

                List<List<?>> res = ((IgniteEx)node).context().query().querySqlFields(explainQry, false).getAll();

                String explanation = (String) res.get(0).get(0);

                log.debug("Node: " + node.name() + ": Execution plan for query "  + qry + ":\n" + explanation);
            } catch (Exception exc) {
                log.error("Ignoring exception gotten explaining query : " + qry, exc);
            }
        }

        FieldsQueryCursor<List<?>> cursor = ((IgniteEx)node).context().query().querySqlFields(qry, false);

        return Result.fromCursor(cursor);
    }

    /**
     * Shortcut for {@link #assertContainsEq(Collection, Collection)} without message.
     */
    protected void assertContainsEq(Collection actual, Collection expected) {
        assertContainsEq(null, actual, expected);
    }

    /**
     * Assert that collections contain the equal elements ({@link Object#equals(Object)}), ignoring the order.
     *
     * @param msg message to add if assert fails.
     * @param actual collection.
     * @param expected collection.
     */
    protected void assertContainsEq(String msg, Collection<?> actual, Collection<?> expected) {
        if (F.isEmpty(msg))
            msg = "Assertion failed.";

        if (actual.size() != expected.size())
            throw new AssertionError(msg + " Collections contain different number of elements:" +
                " [actual=" + actual + ", expected=" + expected + "].\n" +
                "[uniqActual=]" + removeFromCopy(actual, expected) +
                ", uniqExpected=" + removeFromCopy(expected, actual) + "]");

        if (!actual.containsAll(expected))
            throw new AssertionError(msg + " Collections differ:" +
                " [actual=" + actual + ", expected=" + expected + "].\n" +
                "[uniqActual=]" + removeFromCopy(actual, expected) +
                ", uniqExpected=" + removeFromCopy(expected, actual) + "]");
    }

    /**
     * Subtracts from the copy of one collection another collection.
     * Number of "from" collection duplicates that will be removed, is equal to number of
     * duplicates in "toRemove" collection.
     *
     * @param from Collection which copy is left argument of subtraction.
     * @param toRemove Right argument of subtraction.
     */
    private static Collection removeFromCopy(Collection<?> from, Collection<?> toRemove) {
        List<?> fromCp = new ArrayList<>(from);

        for (Object e : toRemove)
            fromCp.remove(e);

        return fromCp;
    }

    /**
     * Performs scan query with fields projection.
     *
     * @param cache cache to query.
     * @param filter filter for rows.
     * @param fields to use in result (projection).
     */
    protected static <K, V> List<List<Object>> select(
        IgniteCache<K, V> cache,
        @Nullable IgnitePredicate<Map<String, Object>> filter,
        String... fields) {

        IgniteClosure<Map<String, Object>, List<Object>> fieldsExtractor = row -> {
            List<Object> res = new ArrayList<>();

            for (String field : fields) {
                String normField = field.toUpperCase();

                if (!row.containsKey(normField)) {
                    throw new RuntimeException("Field with name " + normField +
                        " not found in the table. Avaliable fields: " + row.keySet());
                }

                Object val = row.get(normField);

                res.add(val);
            }
            return res;
        };

        return select(cache, filter, fieldsExtractor);
    }

    /**
     * Performs scan query with custom transformer (mapper).
     *
     * @param cache cache to query.
     * @param filter filter for rows.
     * @param transformer result mapper.
     */
    protected static <K, V, R> List<R> select(
        IgniteCache<K, V> cache,
        @Nullable IgnitePredicate<Map<String, Object>> filter,
        IgniteClosure<Map<String, Object>, R> transformer) {

        Collection<QueryEntity> entities = cache.getConfiguration(CacheConfiguration.class).getQueryEntities();

        assert entities.size() == 1 : "Cache should contain exactly one table";

        final QueryEntity meta = entities.iterator().next();

        IgniteClosure<Cache.Entry<K, V>, R> transformerAdapter = entry -> {
            Map<String, Object> row = entryToMap(meta, entry.getKey(), entry.getValue());

            return transformer.apply(row);
        };

        IgniteBiPredicate<K, V> filterAdapter = (filter == null) ? null :
            (key, val) -> filter.apply(entryToMap(meta, key, val));

        QueryCursor<R> cursor = cache.withKeepBinary()
            .query(new ScanQuery<>(filterAdapter), transformerAdapter);

        return cursor.getAll();
    }

    /**
     * Transforms cache entry to map (column name -> value).
     *
     * @param meta Meta information (QueryEntity) about table.
     * @param key Key of the cache entry.
     * @param val Value of the cache entry.
     */
    private static Map<String, Object> entryToMap(QueryEntity meta, Object key, Object val) {
        Map<String, Object> row = new LinkedHashMap<>();

        // Look up for the field in the key
        if (key instanceof BinaryObject) {
            BinaryObject compositeKey = (BinaryObject)key;

            for (String field : compositeKey.type().fieldNames())
                row.put(field, compositeKey.field(field));
        }
        else
            row.put(meta.getKeyFieldName(), key);

        // And in the value.
        if (val instanceof BinaryObject) {
            BinaryObject compositeVal = (BinaryObject)val;

            for (String field : compositeVal.type().fieldNames())
                row.put(field, compositeVal.field(field));
        }
        else
            row.put(meta.getValueFieldName(), val);

        return row;
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

    /**
     * Applies specified closure to each cluster node.
     */
    protected void testAllNodes(Consumer<Ignite> consumer) {
        for (Ignite node : Ignition.allGrids()) {
            log.info("Testing on node " + node.name() + '.');

            consumer.accept(node);

            log.info("Testing on node " + node.name() + " is done.");
        }
    }

    public void testBasicSelect() {
        testAllNodes(node -> {
            Result emps = executeFrom("SELECT * FROM Employee", node);

            assertContainsEq("SELECT * returned unexpected column names.", emps.columnNames(), Arrays.asList(ALL_EMP_FIELDS));

            List<List<Object>> expEmps = select(node.cache(EMP_CACHE_NAME), null, emps.columnNames().toArray(new String[0]));

            assertContainsEq(emps.values(), expEmps);
        });
    }

    public void testSelectFields() {
        testAllNodes(node -> {
            Result res = executeFrom("SELECT firstName, id, age FROM Employee;", node);

            String[] fields = {"FIRSTNAME", "ID", "AGE"};

            assertEquals("Returned column names are incorrect.", res.columnNames(), Arrays.asList(fields));

            List<List<Object>> expected = select(node.cache(EMP_CACHE_NAME), null, fields);

            assertContainsEq(res.values(), expected);
        });
    }

    public void testSelectBetween() {
        testAllNodes(node -> {
            Result emps = executeFrom("SELECT * FROM Employee e WHERE e.id BETWEEN 101 and 200", node);

            assertEquals("Fetched number of employees is incorrect", 100, emps.values().size());

            String[] fields = emps.columnNames().toArray(new String[0]);

            assertContainsEq("SELECT * returned unexpected column names.", emps.columnNames(), Arrays.asList(ALL_EMP_FIELDS));

            IgnitePredicate<Map<String, Object>> between = row -> {
                long id = (Long)row.get("ID");
                return 101 <= id && id <= 200;
            };

            List<List<Object>> expected = select(node.cache(EMP_CACHE_NAME), between, fields);

            assertContainsEq(emps.values(), expected);
        });
    }

    public void testEmptyBetween() {
        testAllNodes(node -> {
            Result emps = executeFrom("SELECT * FROM Employee e WHERE e.id BETWEEN 200 AND 101", node);

            assertTrue("SQL should have returned empty result set, but it have returned: " + emps, emps.values().isEmpty());
        });
    }

    public void testSelectInStatic() {
        testAllNodes(node -> {
            Result actual = executeFrom("SELECT age FROM Employee WHERE id IN (1, 256, 42)", node);

            List<List<Object>> expected = select(node.cache(EMP_CACHE_NAME),
                row -> Arrays.asList(1L, 256L, 42L).contains(row.get("ID")),
                "AGE");

            assertContainsEq(actual.values(), expected);
        });
    }

    public void testSelectInSubquery() {
        testAllNodes(node -> {
            Result actual = executeFrom("SELECT lastName FROM Employee WHERE id in (SELECT id FROM Employee WHERE age < 30)", node);

            List<List<Object>> expected = select(node.cache(EMP_CACHE_NAME), row -> (Integer)row.get("AGE") < 30, "lastName");

            assertContainsEq(actual.values(), expected);
        });
    }

    public void testBasicOrderByLastName() {
        testAllNodes(node -> {
            Result result = executeFrom("SELECT * FROM Employee e ORDER BY e.lastName", node);

            List<List<Object>> exp = select(node.cache(EMP_CACHE_NAME), null, result.columnNames().toArray(new String[0]));

            assertContainsEq(result.values(), exp);

            int lastNameIdx = result.columnNames().indexOf("LASTNAME");

            Comparator<List<?>> asc = Comparator.comparing((List<?> row) -> (String)row.get(lastNameIdx));
            assertSortedBy(result.values(), asc);
        });
    }

    public void testBasicDistinct() {
        testAllNodes(node -> {
            Result ages = executeFrom("SELECT DISTINCT age FROM Employee", node);

            Set<Object> expected = distinct(select(node.cache(EMP_CACHE_NAME), null, "age"));

            assertContainsEq("Values in cache differ from values returned from sql.", ages.values(), expected);
        });
    }

    public void testDistinctWithWhere() {
        testAllNodes(node -> {
            Result ages = executeFrom("SELECT DISTINCT age FROM Employee WHERE id < 100", node);

            Set<Object> expAges = distinct(select(node.cache(EMP_CACHE_NAME), row -> (Long)row.get("ID") < 100, "age"));

            assertContainsEq(ages.values(), expAges);
        });
    }

    public void testWhereGreater() {
        testAllNodes(node -> {
            Result idxActual = executeFrom("SELECT firstName FROM Employee WHERE age > 30", node);
            Result noidxActual = executeFrom("SELECT firstName FROM Employee WHERE salary > 75", node);

            IgniteCache<Object, Object> cache = node.cache(EMP_CACHE_NAME);

            List<List<Object>> idxExp = select(cache, row -> (Integer)row.get("AGE") > 30, "firstName");
            List<List<Object>> noidxExp = select(cache, row -> (Integer)row.get("SALARY") > 75, "firstName");

            assertContainsEq(idxActual.values(), idxExp);
            assertContainsEq(noidxActual.values(), noidxExp);
        });
    }

    public void testWhereLess() {
        testAllNodes(node -> {
            Result idxActual = executeFrom("SELECT firstName FROM Employee WHERE age < 30", node);
            Result noidxActual = executeFrom("SELECT firstName FROM Employee WHERE salary < 75", node);

            IgniteCache<Object, Object> cache = node.cache(EMP_CACHE_NAME);

            List<List<Object>> idxExp = select(cache, row -> (Integer)row.get("AGE") < 30, "firstName");
            List<List<Object>> noidxExp = select(cache, row -> (Integer)row.get("SALARY") < 75, "firstName");

            assertContainsEq(idxActual.values(), idxExp);
            assertContainsEq(noidxActual.values(), noidxExp);
        });
    }

    public void testWhereEq() {
        testAllNodes(node -> {
            Result idxActual = executeFrom("SELECT firstName FROM Employee WHERE age = 30", node);
            Result noidxActual = executeFrom("SELECT firstName FROM Employee WHERE salary = 75", node);

            IgniteCache<Object, Object> cache = node.cache(EMP_CACHE_NAME);

            List<List<Object>> idxExp = select(cache, row -> (Integer)row.get("AGE") == 30, "firstName");
            List<List<Object>> noidxExp = select(cache, row -> (Integer)row.get("SALARY") == 75, "firstName");

            assertContainsEq(idxActual.values(), idxExp);
            assertContainsEq(noidxActual.values(), noidxExp);
        });
    }

    public void testGroupByIdxField() {
        testAllNodes(node -> {
            // Need to filter out only part of records (each one is a count of employees
            // of particular age) in HAVING clause.
            final int avgAge = (int)(EMP_CNT / AGES_CNT);

            Result result = executeFrom("SELECT age, COUNT(*) FROM Employee GROUP BY age HAVING COUNT(*) > " + avgAge, node);

            List<List<Object>> all = select(node.cache(EMP_CACHE_NAME), null, "age");

            Map<Integer, Long> cntGroups = new HashMap<>();

            for (List<Object> entry : all) {
                Integer age = (Integer)entry.get(0);

                long cnt = cntGroups.getOrDefault(age, 0L);

                cntGroups.put(age, cnt + 1L);
            }

            List<List<Object>> expected = cntGroups.entrySet().stream()
                .filter(ent -> ent.getValue() > avgAge)
                .map(ent -> Arrays.<Object>asList(ent.getKey(), ent.getValue()))
                .collect(Collectors.toList());

            assertContainsEq(result.values(), expected);
        });
    }

    public void testGroupByNoIdxField() {
        testAllNodes(node -> {
            // Need to filter out only part of records (each one is a count of employees
            // associated with particular department id) in HAVING clause.
            final int avgDep = (int)((EMP_CNT - FREE_EMP_CNT) / (DEP_CNT - FREE_DEP_CNT));

            assert FREE_EMP_CNT > avgDep : "Test constants error: group with depId = null should be in result.";

            Result result = executeFrom(
                "SELECT depId, COUNT(*) " +
                    "FROM Employee " +
                    "GROUP BY depIdNoidx " +
                    "HAVING COUNT(*) > " + avgDep, node);

            List<List<Object>> all = select(node.cache(EMP_CACHE_NAME), null, "depId");

            Map<Long, Long> cntGroups = new HashMap<>();

            for (List<Object> entry : all) {
                Long depId = (Long)entry.get(0);

                long cnt = cntGroups.getOrDefault(depId, 0L);

                cntGroups.put(depId, cnt + 1L);
            }

            List<List<Object>> expected = cntGroups.entrySet().stream()
                .filter(ent -> ent.getValue() > avgDep)
                .map(ent -> Arrays.<Object>asList(ent.getKey(), ent.getValue()))
                .collect(Collectors.toList());

            assertContainsEq(result.values(), expected);
        });
    }

    /**
     * Performs generic join operation of two tables.
     * If either outerLeft or outerRight is true, empty map will be passed to transformer argument for rows that
     * don't have matches in the other table.
     *
     * @param left Cache of left table.
     * @param right Cache of the right table.
     * @param filter Filter, corresponds to ON sql clause.
     * @param transformer Transformer (mapper) to make sql projection (select fields for example).
     * @param outerLeft Preserve every row from the left table even if there is no matches in the right table.
     * @param outerRight Same as outerLeft for right table.
     */
    public static <R> List<R> doCommonJoin(
        IgniteCache<?, ?> left,
        IgniteCache<?, ?> right,
        IgniteBiPredicate<Map<String, Object>, Map<String, Object>> filter,
        IgniteBiClosure<Map<String, Object>, Map<String, Object>, R> transformer,
        boolean outerLeft,
        boolean outerRight) {

        List<Map<String, Object>> leftTab = select(left, null, x -> x);
        List<Map<String, Object>> rightTab = select(right, null, x -> x);

        final Map<String, Object> nullRow = Collections.emptyMap();

        List<R> join = new ArrayList<>();

        Set<Map<String, Object>> notFoundRight = Collections.newSetFromMap(new IdentityHashMap<>());

        notFoundRight.addAll(rightTab);

        for (Map<String, Object> lRow : leftTab) {
            boolean foundLeft = false;

            for (Map<String, Object> rRow : rightTab) {
                if (filter.apply(lRow, rRow)) {
                    foundLeft = true;
                    notFoundRight.remove(rRow);

                    join.add(transformer.apply(lRow, rRow));
                }
            }

            if (!foundLeft && outerLeft)
                join.add(transformer.apply(lRow, nullRow));

        }

        if (outerRight) {
            for (Map<String, Object> rRow : notFoundRight)
                join.add(transformer.apply(nullRow, rRow));
        }

        return join;
    }

    protected static <R> List<R> doRightJoin(
        IgniteCache<?, ?> left,
        IgniteCache<?, ?> right,
        IgniteBiPredicate<Map<String, Object>, Map<String, Object>> filter,
        IgniteBiClosure<Map<String, Object>, Map<String, Object>, R> transformer) {

        return doCommonJoin(left, right, filter, transformer, false, true);
    }

    protected static <R> List<R> doLeftJoin(
        IgniteCache<?, ?> left,
        IgniteCache<?, ?> right,
        IgniteBiPredicate<Map<String, Object>, Map<String, Object>> filter,
        IgniteBiClosure<Map<String, Object>, Map<String, Object>, R> transformer) {

        return doCommonJoin(left, right, filter, transformer, true, false);
    }

    protected static <R> List<R> doInnerJoin(
        IgniteCache<?, ?> left,
        IgniteCache<?, ?> right,
        IgniteBiPredicate<Map<String, Object>, Map<String, Object>> filter,
        IgniteBiClosure<Map<String, Object>, Map<String, Object>, R> transformer) {

        return doCommonJoin(left, right, filter, transformer, false, false);
    }

    protected static <R> List<R> doOuterJoin(
        IgniteCache<?, ?> left,
        IgniteCache<?, ?> right,
        IgniteBiPredicate<Map<String, Object>, Map<String, Object>> filter,
        IgniteBiClosure<Map<String, Object>, Map<String, Object>, R> transformer) {

        return doCommonJoin(left, right, filter, transformer, true, true);
    }

    /**
     * Assert that exception about incorrect index in distributed join query is thrown.
     *
     * @param joinCmd command that performs slq join operation.
     */
    protected void assertDistJoinHasIncorrectIndex(Callable<?> joinCmd) {
        GridTestUtils.assertThrows(log,
            joinCmd,
            CacheException.class,
            "Failed to prepare distributed join query: join condition does not use index");
    }

    public void testInnerJoin1() {
        testAllNodes(node -> {
            String qryTpl = "SELECT e.id as EmpId, e.firstName as EmpName, d.id as DepId, d.name as DepName " +
                "FROM Employee e INNER JOIN Department d " +
                "ON e.%s = d.%s";
            Result actIdxOnOn = executeFrom(String.format(qryTpl, "depId", "id"), node);
            Result actIdxOnOff = executeFrom(String.format(qryTpl, "depId", "idNoidx"), node);
            Result actIdxOffOn = executeFrom(String.format(qryTpl, "depIdNoidx", "id"), node);
            Result actIdxOffOff = executeFrom(String.format(qryTpl, "depIdNoidx", "idNoidx"), node);

            List<List<Object>> expected = doInnerJoin(node.cache(EMP_CACHE_NAME), node.cache(DEP_CACHE_NAME),
                (emp, dep) -> sqlEq(emp.get("DEPID"), dep.get("ID")),
                (emp, dep) -> Arrays.asList(emp.get("ID"), emp.get("FIRSTNAME"), dep.get("ID"), dep.get("NAME")));

            assertContainsEq("Join on idx = idx is incorrect.", actIdxOnOn.values(), expected);
            assertContainsEq("Join on idx = noidx is incorrect.", actIdxOnOff.values(), expected);
            assertContainsEq("Join on noidx = idx is incorrect.", actIdxOffOn.values(), expected);
            assertContainsEq("Join on noidx = noidx is incorrect.", actIdxOffOff.values(), expected);
        });
    }

    public void testInnerJoin2() {
        testAllNodes(node -> {
            String qryTpl = "SELECT e.id as EmpId, e.firstName as EmpName, d.id as DepId, d.name as DepName " +
                "FROM Employee e INNER JOIN Department d " +
                "ON e.%s = d.%s";
            Result actIdxOnOn = executeFrom(String.format(qryTpl, "depId", "id"), node);
            Result actIdxOnOff = executeFrom(String.format(qryTpl, "depId", "idNoidx"), node);
            Result actIdxOffOn = executeFrom(String.format(qryTpl, "depIdNoidx", "id"), node);
            Result actIdxOffOff = executeFrom(String.format(qryTpl, "depIdNoidx", "idNoidx"), node);

            List<List<Object>> expected = doInnerJoin(node.cache(EMP_CACHE_NAME), node.cache(DEP_CACHE_NAME),
                (emp, dep) -> sqlEq(emp.get("DEPID"), dep.get("ID")),
                (emp, dep) -> Arrays.asList(emp.get("ID"), emp.get("FIRSTNAME"), dep.get("ID"), dep.get("NAME")));

            assertContainsEq("Join on idx = idx is incorrect.", actIdxOnOn.values(), expected);
            assertContainsEq("Join on idx = noidx is incorrect.", actIdxOnOff.values(), expected);
            assertContainsEq("Join on noidx = idx is incorrect.", actIdxOffOn.values(), expected);
            assertContainsEq("Join on noidx = noidx is incorrect.", actIdxOffOff.values(), expected);
        });
    }

    public void testLeftJoin() {
        testAllNodes(node -> {
            String qryTpl = "SELECT e.id as EmpId, e.firstName as EmpName, d.id as DepId, d.name as DepName " +
                "FROM Employee e LEFT JOIN Department d " +
                "ON e.%s = d.%s";
            Result actIdxOnOn = executeFrom(String.format(qryTpl, "depId", "id"), node);
            Result actIdxOnOff = executeFrom(String.format(qryTpl, "depId", "idNoidx"), node);
            Result actIdxOffOn = executeFrom(String.format(qryTpl, "depIdNoidx", "id"), node);
            Result actIdxOffOff = executeFrom(String.format(qryTpl, "depIdNoidx", "idNoidx"), node);

            List<List<Object>> expected = doLeftJoin(node.cache(EMP_CACHE_NAME), node.cache(DEP_CACHE_NAME),
                (emp, dep) -> sqlEq(emp.get("DEPID"), dep.get("ID")),
                (emp, dep) -> Arrays.asList(emp.get("ID"), emp.get("FIRSTNAME"), dep.get("ID"), dep.get("NAME")));

            assertContainsEq("Join on idx = idx is incorrect.", actIdxOnOn.values(), expected);
            assertContainsEq("Join on idx = noidx is incorrect.", actIdxOnOff.values(), expected);
            assertContainsEq("Join on noidx = idx is incorrect.", actIdxOffOn.values(), expected);
            assertContainsEq("Join on noidx = noidx is incorrect.", actIdxOffOff.values(), expected);
        });
    }

    public void testRightJoin() {
        testAllNodes(node -> {
            String qryTpl = "SELECT e.id as EmpId, e.firstName as EmpName, d.id as DepId, d.name as DepName " +
                "FROM Employee e RIGHT JOIN Department d " +
                "ON e.%s = d.%s";
            Result actIdxOnOn = executeFrom(String.format(qryTpl, "depId", "id"), node);
            Result actIdxOnOff = executeFrom(String.format(qryTpl, "depId", "idNoidx"), node);
            Result actIdxOffOn = executeFrom(String.format(qryTpl, "depIdNoidx", "id"), node);
            Result actIdxOffOff = executeFrom(String.format(qryTpl, "depIdNoidx", "idNoidx"), node);

            List<List<Object>> expected = doRightJoin(node.cache(EMP_CACHE_NAME), node.cache(DEP_CACHE_NAME),
                (emp, dep) -> sqlEq(emp.get("DEPID"), dep.get("ID")),
                (emp, dep) -> Arrays.asList(emp.get("ID"), emp.get("FIRSTNAME"), dep.get("ID"), dep.get("NAME")));

            assertContainsEq("Join on idx = idx is incorrect.", actIdxOnOn.values(), expected);
            assertContainsEq("Join on idx = noidx is incorrect.", actIdxOnOff.values(), expected);
            assertContainsEq("Join on noidx = idx is incorrect.", actIdxOffOn.values(), expected);
            assertContainsEq("Join on noidx = noidx is incorrect.", actIdxOffOff.values(), expected);
        });
    }

    public void testNegativeFullOuterJoin() {
        testAllNodes(node -> {
            String fullOuterJoinQry = "SELECT e.id as EmpId, e.firstName as EmpName, d.id as DepId, d.name as DepName " +
                "FROM Employee e FULL OUTER JOIN Department d " +
                "ON e.depId = d.id";

            GridTestUtils.assertThrows(log, () -> executeFrom(fullOuterJoinQry, node),
                IgniteSQLException.class, "Failed to parse query.");

            String fullOuterJoinSubquery = "SELECT EmpId from (" + fullOuterJoinQry + ")";

            GridTestUtils.assertThrows(log, () -> executeFrom(fullOuterJoinSubquery, node),
                IgniteSQLException.class, "Failed to parse query.");
        });
    }

    public void testNegativeFullOuterDistJoin() {
        testAllNodes(node -> {
            String qry = "SELECT d.id, d.name, a.address " +
                "FROM Department d FULL OUTER JOIN Address a " +
                "ON d.idNoidx = a.depIdNoidx";

            GridTestUtils.assertThrows(log,
                () -> executeFrom(new SqlFieldsQuery(qry).setDistributedJoins(true), node),
                IgniteSQLException.class, "Failed to parse query.");
        });
    }

    /**
     * Creates new SqlFieldsQuery with enabled distributed joins and predictable join order.
     *
     * @param qry Sql join query.
     */
    protected static SqlFieldsQuery prepareDistJoin(String qry) {
        return new SqlFieldsQuery(qry).setDistributedJoins(true).setEnforceJoinOrder(true);
    }

    /**
     * Returns true if arguments are equal in terms of sql: if both arguments are not null and content is equal.
     * Note that null is not equal to null.
     */
    public static boolean sqlEq(Object a, Object b) {
        if (a == null || b == null)
            return false;

        return a.equals(b);
    }

    public void testCfg() {
        // false by default, injected as true
        assertTrue(cfg.persistenceEnabled());
    }

    /**
     * Sets explain flag. If flag is set, execute methods will perform explain query before actual query execution.
     * Query plan is logged.
     *
     * @param shouldExplain explain flag value.
     */
    public static void setExplain(boolean shouldExplain) {
        explain = shouldExplain;
    }
}
