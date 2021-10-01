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

import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.calcite.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsUnion;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test OR -> UnionAll rewrite rule.
 *
 * Example:
 * SELECT * FROM products
 * WHERE category = 'Photo' OR subcategory ='Camera Media';
 *
 * A query above will be rewritten to next (or equivalient similar query)
 *
 * SELECT * FROM products
 *      WHERE category = 'Photo'
 * UNION ALL
 * SELECT * FROM products
 *      WHERE subcategory ='Camera Media' AND LNNVL(category, 'Photo');
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14925")
public class ITOrToUnionRuleTest extends AbstractBasicIntegrationTest {
    /** */
    public static final String IDX_SUBCAT_ID = "IDX_SUBCAT_ID";

    /** */
    public static final String IDX_SUBCATEGORY = "IDX_SUBCATEGORY";

    /** */
    public static final String IDX_CATEGORY = "IDX_CATEGORY";

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
            .withIndex(SchemaBuilders.sortedIndex(IDX_CATEGORY).addIndexColumn("CATEGORY").done().build())
            .withIndex(SchemaBuilders.sortedIndex(IDX_CAT_ID).addIndexColumn("CAT_ID").done().build())
            .withIndex(SchemaBuilders.sortedIndex(IDX_SUBCATEGORY).addIndexColumn("SUBCATEGORY").done().build())
            .withIndex(SchemaBuilders.sortedIndex(IDX_SUBCAT_ID).addIndexColumn("SUBCAT_ID").done().build())
            .build();

        Table tbl = CLUSTER_NODES.get(0).tables().createTable(schTbl1.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(schTbl1, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        insertData(tbl, new String[] {"ID", "CATEGORY", "CAT_ID", "SUBCATEGORY", "SUBCAT_ID", "NAME"}, new Object[][] {
            {1, "Photo", 1, "Camera Media", 11, "Media 1"},
            {2, "Photo", 1, "Camera Media", 11, "Media 2"},
            {3, "Photo", 1, "Camera Lens", 12, "Lens 1"},
            {4, "Photo", 1, "Other", 12, "Charger 1"},
            {5, "Video", 2, "Camera Media", 21, "Media 3"},
            {6, "Video", 2, "Camera Lens", 22, "Lens 3"},
            {7, "Video", 1, null, 0, "Canon"},
            {8, null, 0, "Camera Lens", 11, "Zeiss"},
            {9, null, 0, null, 0, null},
            {10, null, 0, null, 30, null},
            {11, null, 0, null, 30, null},
            {12, null, 0, null, 31, null},
            {13, null, 0, null, 31, null},
            {14, null, 0, null, 32, null},
            {15, null, 0, null, 33, null},
            {16, null, 0, null, 34, null},
            {17, null, 0, null, 35, null},
            {18, null, 0, null, 36, null},
            {19, null, 0, null, 37, null},
            {20, null, 0, null, 38, null},
            {21, null, 0, null, 39, null},
            {22, null, 0, null, 40, null},
            {23, null, 0, null, 41, null},
        });
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualityOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Video' " +
            "OR subcategory ='Camera Lens'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-13710")
    public void testNonDistinctOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE subcategory = 'Camera Lens' " +
            "OR subcategory = 'Other'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"))
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(8, null, 0, "Camera Lens", 11, "Zeiss")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is applied for mixed conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE category = 'Photo' " +
            "OR (subcat_id > 12 AND subcat_id < 22)")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID"))
            .returns(1, "Photo", 1, "Camera Media", 11, "Media 1")
            .returns(2, "Photo", 1, "Camera Media", 11, "Media 2")
            .returns(3, "Photo", 1, "Camera Lens", 12, "Lens 1")
            .returns(4, "Photo", 1, "Other", 12, "Charger 1")
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied for range conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE cat_id > 1 " +
            "OR subcat_id < 10")
            .matches(not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .returns(9, null, 0, null, 0, null)
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if (at least) one of column is not indexed.
     */
    @Test
    public void testNonIndexedOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE name = 'Canon' " +
            "OR category = 'Video'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"))
            .returns(5, "Video", 2, "Camera Media", 21, "Media 3")
            .returns(6, "Video", 2, "Camera Lens", 22, "Lens 3")
            .returns(7, "Video", 1, null, 0, "Canon")
            .check();
    }

    /**
     * Check 'OR -> UNION' rule is not applied if all columns are not indexed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAllNonIndexedOrToUnionAllRewrite() {
        assertQuery("SELECT * " +
            "FROM products " +
            "WHERE name = 'Canon' " +
            "OR name = 'Sony'")
            .matches(not(containsUnion(true)))
            .matches(containsTableScan("PUBLIC", "PRODUCTS"))
            .returns(7, "Video", 1, null, 0, "Canon")
            .check();
    }
}
