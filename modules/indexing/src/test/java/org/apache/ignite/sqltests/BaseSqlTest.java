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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.hamcrest.core.IsEqual.equalTo;

public class BaseSqlTest extends GridCommonAbstractTest {
    private final static long EMP_CNT = 1000;

    private static final String CLIENT_NODE_NAME = "clientNode";

    private static IgniteCache empCache;

    /** Client node instance. */
    private static IgniteEx client;

    /**
     * Hook to change nodes configurations in children.
     *
     * @param cfg - ignite configuration to configure.
     * @return - configured ignite configuration.
     */
    protected IgniteConfiguration configureIgnite(IgniteConfiguration cfg) {
        // No-op. Override to add behaviour.
        return cfg;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName,
        IgniteTestResources rsrcs) throws Exception {
        return configureIgnite(super.getConfiguration(igniteInstanceName, rsrcs));
    }

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
        executeUpdate("CREATE TABLE Employee (" +
            "id LONG PRIMARY KEY, " +
            "depId LONG, " +
            "firstName VARCHAR, " +
            "lastName VARCHAR) " +
            (F.isEmpty(withStr) ? "" : " WITH \"" + withStr + '"') +
            ";");

        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO Employee VALUES (?, ?, ?, ?)");

        final long depId = 42;

        for (int i = 0; i < EMP_CNT; i++) {
            String firstName = UUID.randomUUID().toString();
            String lastName = UUID.randomUUID().toString();

            execute(qry.setArgs(i, depId, firstName, lastName));
        }
    }

    /**
     * Sets up data. Override in children to add/change behaviour.
     */
    protected void fillData(){
        fillCommonData(""); // default.
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(2);

        client = (IgniteEx) startGrid(CLIENT_NODE_NAME, configureIgnite(clientConfiguration()), null);

        fillData();

        empCache = client.cache("SQL_PUBLIC_EMPLOYEE");
    }



    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    public void testBasicSelect() {

        Result emps = checkedSelect("SELECT * FROM Employee", empCache);

        assertEquals("Unexpected size of employees", EMP_CNT, emps.values().size());
    }

    public void testSelectBetween() {
        Result emps = checkedSelect("SELECT * FROM Employee e WHERE e.id BETWEEN 101 and 200", empCache);

        assertEquals("Fetched number of employees is incorrect", 100, emps.values().size());
    }

    public void testEmptyBetween() {
        Result emps = execute("SELECT * FROM Employee e WHERE e.id BETWEEN 200 AND 101");
        assertTrue("SQL sould return empty result set, but returned: " + emps, emps.values().isEmpty());
    }

    public void testSelectOrderByLastName () {
        Result result = checkedSelect("SELECT * FROM Employee e ORDER BY e.lastName", empCache);

        int lastNameIdx = result.columnNames().indexOf("LASTNAME");

        Comparator<List<?>> asc = Comparator.comparing((List<?> row) -> (String)row.get(lastNameIdx));
        assertSortedBy(result.values(), asc);
    }

//    public void testBasicJoin() {
//        execute("SELECT * FROM Employee e JOIN Department d on e.depId = d.id");
//    }

    protected Result checkedSelect(String selectQry, IgniteCache<?, ?> cache) {
        Result res = execute(selectQry);

        assertValuesAreInCache(res, cache);

        return res;

    }

    static class Result {
        private List<String> colNames;
        private List<List<?>> vals;

        public Result(List<String> colNames, List<List<?>> vals) {
            this.colNames = colNames;
            this.vals = vals;
        }

        public List<String> columnNames() {
            return colNames;
        }

        public List<List<?>> values() {
            return vals;
        }

        public static Result fromCursor(FieldsQueryCursor<List<?>> cursor){
            List<String> cols = readColNames(cursor);
            List<List<?>> vals = cursor.getAll();
            return new Result(cols, vals);
        }
    }

    protected <T> void assertSortedBy(List<T> seq, Comparator<T> cmp) {
        Iterator<T> it = seq.iterator();
        if (!it.hasNext())
            return;

        T last = it.next();
        while(it.hasNext()){
            T cur = it.next();
            if (cmp.compare(last, cur) > 0)
                throw new AssertionError("List is not sorted, element '" + last + "' is greater than '" +
                    cur + "'. List: " + seq);
        }
    }

    protected void assertValuesAreInCache(Result res,  IgniteCache<?, ?> cache) {
        int idRowIdx = res.columnNames().indexOf("ID");

        assert idRowIdx >= 0 : "Column with name \"id\" have not been found in column names " + res.columnNames();

        IgniteCache<Long, BinaryObject> binCache = cache.withKeepBinary();

        for (List<?> row : res.values()) {

            Long id = (Long) row.get(idRowIdx);

            BinaryObject cached = binCache.get(id);

            assertNotNull("Cache does not contain entry with id " + id, cached);

            checkMatches(cached, row, res.columnNames());
        }
    }

    private static void checkMatches(BinaryObject fromCache, List<?> fromSql, List<String> colNames) {
        Collection<String> binValCols = fromCache.type().fieldNames();

        ArrayList<String> allBinCols = new ArrayList<>(binValCols);
        allBinCols.add("ID");

        // todo: more informative assert(s).
        assertTrue("Colon names are not equal",
            allBinCols.containsAll(colNames) && colNames.containsAll(allBinCols));

        List<?> valsFromCache = colNames.stream()
            .filter(name -> !name.equals("ID"))
            .map(name -> fromCache.field(name))
            .collect(Collectors.toList());

        List<Object> valsFromSql = new ArrayList<>();

        for (int i = 0; i < colNames.size(); i++) {
            String name = colNames.get(i);

            if (name.equals("ID"))
                continue;

            valsFromSql.add(fromSql.get(i));
        }

        Assert.assertThat("Binary object content does not match values returned by sql query", valsFromSql,
            equalTo(valsFromCache));
    }

    private static List<String> readColNames(FieldsQueryCursor<?> cursor) {
        ArrayList<String> colNames = new ArrayList<>();

        for (int i = 0; i < cursor.getColumnsCount(); i++)
            colNames.add(cursor.getFieldName(i));

        return Collections.unmodifiableList(colNames);
    }

    protected Result execute(String qry) {
        return execute(new SqlFieldsQuery(qry));
    }

    protected Long executeUpdate(String updateQry) {
        return (Long) execute(new SqlFieldsQuery(updateQry)).values().get(0).get(0);
    }

    protected final Result execute(SqlFieldsQuery updateQry) {
        FieldsQueryCursor<List<?>> cursor = client.context().query().querySqlFields(updateQry, false);

        return Result.fromCursor(cursor);
    }
}
