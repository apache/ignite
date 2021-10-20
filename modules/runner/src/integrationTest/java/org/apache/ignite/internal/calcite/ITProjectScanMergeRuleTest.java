/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.calcite;

import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.calcite.util.QueryChecker.containsAnyProject;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsOneProject;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsProject;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsTableScan;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests projection rule {@code org.apache.ignite.internal.processors.query.calcite.rule.logical.ProjectScanMergeRule}
 * This rule have a deal with only useful columns and.
 * For example for tables: T1(f12, f12, f13) and T2(f21, f22, f23)
 * sql execution: SELECT t1.f11, t2.f21 FROM T1 t1 INNER JOIN T2 t2 on t1.f11 = t2.f22"
 * need to eleminate all unused coluns and take into account only: f11, f21 and f22 cols.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITProjectScanMergeRuleTest extends AbstractBasicIntegrationTest {
    /** */
    public static final String IDX_CAT_ID = "IDX_CAT_ID";

    /** {@inheritDoc} */
    @Override protected void initTestData() {
        TableDefinition schTbl1 = SchemaBuilders.tableBuilder("PUBLIC", "PRODUCTS").columns(
            SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("CATEGORY", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("CAT_ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("SUBCATEGORY", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("SUBCAT_ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build()
        )
            .withPrimaryKey("ID")
            .withIndex(
                SchemaBuilders.sortedIndex(IDX_CAT_ID)
                    .addIndexColumn("CAT_ID").done()
                    .build()
            )
            .build();

        Table tbl = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        insertData(tbl, new String[] {"ID", "CATEGORY", "CAT_ID", "SUBCATEGORY", "SUBCAT_ID", "NAME"}, new Object[][] {
            {1, "prod1", 1, "cat1", 11, "noname1"},
            {2, "prod2", 2, "cat1", 11, "noname2"},
            {3, "prod3", 3, "cat1", 12, "noname3"},
            {4, "prod4", 4, "cat1", 13, "noname4"},
        });
    }

    /**
     * Tests that the projects exist only for simple expressions without any predicates.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-14925")
    @Test
    public void testProjects() {
        assertQuery("SELECT NAME FROM products d;")
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .matches(containsOneProject("PUBLIC", "PRODUCTS", 4))
            .returns("noname1")
            .returns("noname2")
            .returns("noname3")
            .returns("noname4")
            .check();

        assertQuery("SELECT SUBCAT_ID, NAME FROM products d;")
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .matches(containsOneProject("PUBLIC", "PRODUCTS", 2, 4))
            .returns(11, "noname1")
            .returns(11, "noname2")
            .returns(12, "noname3")
            .returns(13, "noname4")
            .check();

        assertQuery("SELECT NAME FROM products d WHERE CAT_ID > 1;")
            .matches(containsIndexScan("PUBLIC", "PRODUCTS"))
            .matches(containsProject("PUBLIC", "PRODUCTS", 1, 4))
            .returns("noname2")
            .returns("noname3")
            .returns("noname4")
            .check();
    }

    /**
     * Tests projects with nested requests.
     */
    @Test
    public void testNestedProjects() {
        assertQuery("SELECT NAME FROM products WHERE CAT_ID IN (SELECT CAT_ID FROM products WHERE CAT_ID > 1) and ID > 2;")
            .matches(containsAnyProject("PUBLIC", "PRODUCTS"))
            .returns("noname3")
            .returns("noname4")
            .check();

        assertQuery("SELECT NAME FROM products WHERE CAT_ID IN (SELECT DISTINCT CAT_ID FROM products WHERE CAT_ID > 1)")
            .matches(containsAnyProject("PUBLIC", "PRODUCTS"))
            .returns("noname2")
            .returns("noname3")
            .returns("noname4")
            .check();

        assertQuery("SELECT NAME FROM products WHERE CAT_ID IN (SELECT DISTINCT CAT_ID FROM products WHERE SUBCAT_ID > 11)")
            .matches(containsAnyProject("PUBLIC", "PRODUCTS"))
            .returns("noname3")
            .returns("noname4")
            .check();

        assertQuery("SELECT NAME FROM products WHERE CAT_ID = (SELECT CAT_ID FROM products WHERE SUBCAT_ID = 13)")
            .matches(containsAnyProject("PUBLIC", "PRODUCTS"))
            .returns("noname4")
            .check();

        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT NAME FROM products WHERE CAT_ID = (SELECT CAT_ID FROM products WHERE SUBCAT_ID = 11)").check()
        );

        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT NAME FROM products WHERE CAT_ID = (SELECT 2 UNION ALL SELECT 1)").check()
        );

        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT NAME FROM products WHERE CAT_ID = (SELECT null UNION ALL SELECT 1)").check()
        );
    }
}
