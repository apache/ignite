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

import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.Streams;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for set op (EXCEPT, INTERSECT).
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITSetOpTest extends AbstractBasicIntegrationTest {
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
    }

    /** */
    @Test
    public void testExcept() {
        List<List<?>> rows = sql("SELECT name FROM emp1 EXCEPT SELECT name FROM emp2");

        assertEquals(1, rows.size());
        assertEquals("Igor", rows.get(0).get(0));
    }

    /** */
    @Test
    public void testExceptFromEmpty() {
        List<List<?>> rows = sql("SELECT name FROM emp1 WHERE salary < 0 EXCEPT SELECT name FROM emp2");

        assertEquals(0, rows.size());
    }

    /** */
    @Test
    public void testExceptSeveralColumns() {
        List<List<?>> rows = sql("SELECT name, salary FROM emp1 EXCEPT SELECT name, salary FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, countIf(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testExceptAll() {
        List<List<?>> rows = sql("SELECT name FROM emp1 EXCEPT ALL SELECT name FROM emp2");

        assertEquals(4, rows.size());
        assertEquals(3, countIf(rows, r -> r.get(0).equals("Igor")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testExceptNested() {
        List<List<?>> rows =
            sql("SELECT name FROM emp1 EXCEPT (SELECT name FROM emp1 EXCEPT SELECT name FROM emp2)");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    @Disabled
    public void testSetOpBigBatch() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "BIG_TABLE1")
            .columns(
                SchemaBuilders.column("KEY", ColumnType.INT32).asNonNull().build(),
                SchemaBuilders.column("VAL", ColumnType.INT32).asNullable().build()
            )
            .withPrimaryKey("KEY")
            .build();

        Table tbl1 = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(2)
                .changePartitions(10)
        );

        TableDefinition schTbl2 = SchemaBuilders.tableBuilder("PUBLIC", "BIG_TABLE2")
            .columns(
                SchemaBuilders.column("KEY", ColumnType.INT32).asNonNull().build(),
                SchemaBuilders.column("VAL", ColumnType.INT32).asNullable().build()
            )
            .withPrimaryKey("KEY")
            .build();

        Table tbl2 = CLUSTER_NODES.get(0).tables().createTable(schTbl2.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl2, tblCh)
                .changeReplicas(2)
                .changePartitions(10)
        );

        int key = 0;

        RecordView<Tuple> recordView1 = tbl1.recordView();
        RecordView<Tuple> recordView2 = tbl2.recordView();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < ((i == 0) ? 1 : (1 << (i * 4 - 1))); j++) {
                // Cache1 keys count: 1 of "0", 8 of "1", 128 of "2", 2048 of "3", 32768 of "4".
                recordView1.insert(Tuple.create().set("KEY", key++).set("VAL", i));

                // Cache2 keys count: 1 of "5", 128 of "3", 32768 of "1".
                if ((i & 1) == 0)
                    recordView2.insert(Tuple.create().set("KEY", key++).set("VAL", 5 - i));
            }
        }

        // Check 2 partitioned caches.
        List<List<?>> rows = sql("SELECT val FROM BIG_TABLE1 EXCEPT SELECT val FROM BIG_TABLE2");

        assertEquals(3, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(0)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(2)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(4)));

        rows = sql("SELECT val FROM BIG_TABLE1 EXCEPT ALL SELECT val FROM BIG_TABLE2");

        assertEquals(34817, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(0)));
        assertEquals(128, countIf(rows, r -> r.get(0).equals(2)));
        assertEquals(1920, countIf(rows, r -> r.get(0).equals(3)));
        assertEquals(32768, countIf(rows, r -> r.get(0).equals(4)));

        rows = sql("SELECT val FROM BIG_TABLE1 INTERSECT SELECT val FROM BIG_TABLE2");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals(1)));
        assertEquals(1, countIf(rows, r -> r.get(0).equals(3)));

        rows = sql("SELECT val FROM BIG_TABLE1 INTERSECT ALL SELECT val FROM BIG_TABLE2");

        assertEquals(136, rows.size());
        assertEquals(8, countIf(rows, r -> r.get(0).equals(1)));
        assertEquals(128, countIf(rows, r -> r.get(0).equals(3)));
    }

    /** */
    @Test
    public void testIntersect() {
        List<List<?>> rows = sql("SELECT name FROM emp1 INTERSECT SELECT name FROM emp2");

        assertEquals(2, rows.size());
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testIntersectAll() {
        List<List<?>> rows = sql("SELECT name FROM emp1 INTERSECT ALL SELECT name FROM emp2");

        assertEquals(3, rows.size());
        assertEquals(2, countIf(rows, r -> r.get(0).equals("Igor1")));
        assertEquals(1, countIf(rows, r -> r.get(0).equals("Roman")));
    }

    /** */
    @Test
    public void testIntersectEmpty() {
        List<List<?>> rows = sql("SELECT name FROM emp1 WHERE salary < 0 INTERSECT SELECT name FROM emp2");

        assertEquals(0, rows.size());
    }

    /** */
    @Test
    public void testIntersectSeveralColumns() {
        List<List<?>> rows = sql("SELECT name, salary FROM emp1 INTERSECT ALL SELECT name, salary FROM emp2");
        
        assertEquals(2, rows.size());
        assertEquals(2, countIf(rows, r -> r.get(0).equals("Igor1")));
    }

    /** */
    @Test
    public void testUnionAll() {
        List<List<?>> rows = sql("SELECT name, salary FROM emp1 " +
            "UNION ALL " +
            "SELECT name, salary FROM emp2 " +
            "UNION ALL " +
            "SELECT name, salary FROM emp1 WHERE salary > 13 ");

        assertEquals(14, rows.size());
    }

    /** */
    @Test
    public void testUnion() {
        List<List<?>> rows = sql("SELECT name, salary FROM emp1 " +
            "UNION " +
            "SELECT name, salary FROM emp2 " +
            "UNION " +
            "SELECT name, salary FROM emp1 WHERE salary > 13 ");

        assertEquals(9, rows.size());
    }

    /** */
    @Test
    public void testUnionWithDistinct() {
        List<List<?>> rows = sql(
            "SELECT distinct(name) FROM emp1 UNION SELECT name from emp2");

        assertEquals(3, rows.size());
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

    private <T> long countIf(Iterable<T> it, Predicate<T> pred) {
        return Streams.stream(it).filter(pred).count();
    }
}
