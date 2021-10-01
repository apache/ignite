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

package org.apache.ignite.internal.calcite;

import java.util.Arrays;
import java.util.List;

import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITMixedQueriesTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void initTestData() {
        Table emp1 = createTable("EMP1");
        Table emp2 = createTable("EMP2");

        int idx = 0;
        insertData(emp1, new String[] {"ID", "NAME", "SALARY"}, new Object[][] {
            {idx++, "Igor", 10d},
            {idx++, "Igor", 11d},
            {idx++, "Igor", 12d},
            {idx++, "Igor1", 13d},
            {idx++, "Igor1", 13d},
            {idx++, "Igor1", 13d},
            {idx, "Roman", 14d}
        });

        idx = 0;
        insertData(emp2, new String[] {"ID", "NAME", "SALARY"}, new Object[][] {
            {idx++, "Roman", 10d},
            {idx++, "Roman", 11d},
            {idx++, "Roman", 12d},
            {idx++, "Roman", 13d},
            {idx++, "Igor1", 13d},
            {idx, "Igor1", 13d}
        });

        /*
        select * from emp1;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Igor  |   10  |
        |  2 | Igor  |   11  |
        |  3 | Igor  |   12  |
        |  4 | Igor1 |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        |  7 | Roman |   14  |
        +----+-------+-------+

        select * from emp2;
        +----+-------+-------+
        | ID | NAME  | SALARY|
        +----+-------+-------+
        |  1 | Roman |   10  |
        |  2 | Roman |   11  |
        |  3 | Roman |   12  |
        |  4 | Roman |   13  |
        |  5 | Igor1 |   13  |
        |  6 | Igor1 |   13  |
        +----+-------+-------+
         */
    }

    /** Tests varchar min\max aggregates. */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15107")
    @Test
    public void testVarCharMinMax() {
        sql("CREATE TABLE TEST(val VARCHAR primary key, val1 integer);");
        sql("INSERT INTO test VALUES ('б', 1), ('бб', 2), ('щ', 3), ('щщ', 4), ('Б', 4), ('ББ', 4), ('Я', 4);");
        List<List<?>> rows = sql("SELECT MAX(val), MIN(val) FROM TEST");

        assertEquals(1, rows.size());
        assertEquals(Arrays.asList("щщ", "Б"), first(rows));
    }

    /** */
    @Test
    public void testOrderingByColumnOutsideSelectList() {
        assertQuery("select salary from emp2 order by id desc")
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .returns(12d)
            .returns(11d)
            .returns(10d)
            .check();

        assertQuery("select name, sum(salary) from emp2 group by name order by count(salary)")
            .returns("Roman", 46d)
            .returns("Igor1", 26d)
            .check();
    }

    /** */
    @Test
    public void testEqConditionWithDistinctSubquery() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 WHERE salary = (SELECT DISTINCT(salary) FROM emp2 WHERE name='Igor1')");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testEqConditionWithAggregateSubqueryMax() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 WHERE salary = (SELECT MAX(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testEqConditionWithAggregateSubqueryMin() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 WHERE salary = (SELECT MIN(salary) FROM emp2 WHERE name='Roman')");

        assertEquals(1, rows.size());
    }

    /** */
    @Test
    public void testInConditionWithSubquery() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 WHERE name IN (SELECT name FROM emp2)");

        assertEquals(4, rows.size());
    }

    /** */
    @Test
    public void testDistinctQueryWithInConditionWithSubquery() {
        List<List<?>> rows = sql(
            "SELECT distinct(name) FROM emp1 o WHERE name IN (" +
                "   SELECT name" +
                "   FROM emp2)");

        assertEquals(2, rows.size());
    }

    /** */
    @Test
    public void testNotInConditionWithSubquery() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 WHERE name NOT IN (SELECT name FROM emp2)");

        assertEquals(3, rows.size());
    }

    /** */
    @Test
    public void testExistsConditionWithSubquery() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 o WHERE EXISTS (" +
                "   SELECT 1" +
                "   FROM emp2 a" +
                "   WHERE o.name = a.name)");

        assertEquals(4, rows.size());
    }

    /** */
    @Test
    public void testNotExistsConditionWithSubquery() {
        List<List<?>> rows = sql(
            "SELECT name FROM emp1 o WHERE NOT EXISTS (" +
                "   SELECT 1" +
                "   FROM emp2 a" +
                "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql(
            "SELECT name FROM emp1 o WHERE NOT EXISTS (" +
                "   SELECT name" +
                "   FROM emp2 a" +
                "   WHERE o.name = a.name)");

        assertEquals(3, rows.size());

        rows = sql(
            "SELECT distinct(name) FROM emp1 o WHERE NOT EXISTS (" +
                "   SELECT name" +
                "   FROM emp2 a" +
                "   WHERE o.name = a.name)");

        assertEquals(1, rows.size());
    }

    /** */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15107")
    @Test
    public void testSequentialInserts() {
        sql("CREATE TABLE t(x INTEGER)");

        for (int i = 0; i < 10_000; i++)
            sql("INSERT INTO t VALUES (?)", i);

        assertEquals(10_000L, sql("SELECT count(*) FROM t").get(0).get(0));
    }

    /**
     * Verifies that table modification events are passed to a calcite schema modification listener.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15107")
    @Test
    public void testIgniteSchemaAwaresAlterTableCommand() {
        String selectAllQry = "select * from test_tbl";

        sql("drop table if exists test_tbl");
        sql("create table test_tbl(id int primary key, val varchar)");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        sql("alter table test_tbl add column new_col int");

        assertQuery(selectAllQry).columnNames("ID", "NEW_COL", "VAL").check();

        // column with such name already exists
        assertThrows(Exception.class, () -> sql("alter table test_tbl add column new_col int"));

        assertQuery(selectAllQry).columnNames("ID", "NEW_COL", "VAL").check();

        sql("alter table test_tbl add column if not exists new_col int");

        assertQuery(selectAllQry).columnNames("ID", "NEW_COL", "VAL").check();

        sql("alter table test_tbl drop column new_col");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        // column with such name is not exists
        assertThrows(Exception.class, () -> sql("alter table test_tbl drop column new_col"));

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();

        sql("alter table test_tbl drop column if exists new_col");

        assertQuery(selectAllQry).columnNames("ID", "VAL").check();
    }

//    /**
//     * Verifies infix cast operator.
//     */
//    @Test
//    public void testInfixTypeCast() {
//        sql("drop table if exists test_tbl");
//        sql("create table test_tbl(id int primary key, val varchar)");
//
//        // Await for PME, see details here: https://issues.apache.org/jira/browse/IGNITE-14974
//        awaitPartitionMapExchange();
//
//        QueryEngine engineSrv = Commons.lookupComponent(grid(1).context(), QueryEngine.class);
//
//        FieldsQueryCursor<List<?>> cur = engineSrv.query(null, "PUBLIC",
//            "select id, id::tinyint as tid, id::smallint as sid, id::varchar as vid from test_tbl").get(0);
//
//        assertThat(cur, CoreMatchers.instanceOf(QueryCursorEx.class));
//
//        QueryCursorEx<?> qCur = (QueryCursorEx<?>)cur;
//
//        assertThat(qCur.fieldsMeta().get(0).fieldTypeName(), equalTo(Integer.class.getName()));
//        assertThat(qCur.fieldsMeta().get(1).fieldTypeName(), equalTo(Byte.class.getName()));
//        assertThat(qCur.fieldsMeta().get(2).fieldTypeName(), equalTo(Short.class.getName()));
//        assertThat(qCur.fieldsMeta().get(3).fieldTypeName(), equalTo(String.class.getName()));
//    }

    /** Quantified predicates test. */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-13159")
    @Test
    public void quantifiedCompTest() throws InterruptedException {
        assertQuery("select salary from emp2 where salary > SOME (10, 11) ORDER BY salary")
            .returns(11d)
            .returns(12d)
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .check();

        assertQuery("select salary from emp2 where salary < SOME (12, 12) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();

        assertQuery("select salary from emp2 where salary < ANY (11, 12) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();

        assertQuery("select salary from emp2 where salary > ANY (12, 13) ORDER BY salary")
            .returns(13d)
            .returns(13d)
            .returns(13d)
            .check();

        assertQuery("select salary from emp2 where salary <> ALL (12, 13) ORDER BY salary")
            .returns(10d)
            .returns(11d)
            .check();
    }

    private Table createTable(String tableName) {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", tableName)
            .columns(
                SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
                SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
                SchemaBuilders.column("SALARY", ColumnType.DOUBLE).asNullable().build()
            )
            .withPrimaryKey("ID")
            .build();

        return CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(2)
                .changePartitions(10)
        );
    }
}
