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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.calcite.util.QueryChecker.containsAnyProject;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsAnyScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsTableScan;
import static org.apache.ignite.internal.calcite.util.QueryChecker.containsUnion;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.Matchers.not;

/**
 * Basic index tests.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-14925")
public class ITSecondaryIndexTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String PK_IDX = "PK_IDX";

    /** */
    private static final String DEPID_IDX = "DEPID_IDX";

    /** */
    private static final String NAME_CITY_IDX = "NAME_CITY_IDX";

    /** */
    private static final String NAME_DEPID_CITY_IDX = "NAME_DEPID_CITY_IDX";

    /** */
    @BeforeAll
    static void initTestData() {
        {
            TableDefinition schema = SchemaBuilders.tableBuilder("PUBLIC", "DEVELOPER")
                .columns(
                    SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
                    SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
                    SchemaBuilders.column("DEPID", ColumnType.INT32).asNullable().build(),
                    SchemaBuilders.column("CITY", ColumnType.string()).asNullable().build(),
                    SchemaBuilders.column("AGE", ColumnType.INT32).asNullable().build()
                )
                .withPrimaryKey("ID")
                .withIndex(
                    SchemaBuilders.sortedIndex(DEPID_IDX).addIndexColumn("DEPID").done().build()
                )
                .withIndex(
                    SchemaBuilders.sortedIndex(NAME_CITY_IDX)
                        .addIndexColumn("DEPID").desc().done()
                        .addIndexColumn("CITY").desc().done()
                        .build()
                )
                .withIndex(
                    SchemaBuilders.sortedIndex(NAME_DEPID_CITY_IDX)
                        .addIndexColumn("NAME").done()
                        .addIndexColumn("DEPID").desc().done()
                        .addIndexColumn("CITY").desc().done()
                        .build()
                )
                .build();

            Table dev = CLUSTER_NODES.get(0).tables().createTable(schema.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schema, tblCh)
                    .changeReplicas(2)
                    .changePartitions(10)
            );

            insertData(dev, new String[] {"ID", "NAME", "DEPID", "CITY", "AGE"}, new Object[][] {
                {1, "Mozart", 3, "Vienna", 33},
                {2, "Beethoven", 2, "Vienna", 44},
                {3, "Bach", 1, "Leipzig", 55},
                {4, "Strauss", 2, "Munich", 66},
                {5, "Vagner", 4, "Leipzig", 70},
                {6, "Chaikovsky", 5, "Votkinsk", 53},
                {7, "Verdy", 6, "Rankola", 88},
                {8, "Stravinsky", 7, "Spt", 89},
                {9, "Rahmaninov", 8, "Starorussky ud", 70},
                {10, "Shubert", 9, "Vienna", 31},
                {11, "Glinka", 10, "Smolenskaya gb", 53},
                {12, "Einaudi", 11, "", -1},
                {13, "Glass", 12, "", -1},
                {14, "Rihter", 13, "", -1},
                {15, "Marradi", 14, "", -1},
                {16, "Zimmer", 15, "", -1},
                {17, "Hasaishi", 16, "", -1},
                {18, "Arnalds", 17, "", -1},
                {19, "Yiruma", 18, "", -1},
                {20, "O'Halloran", 19, "", -1},
                {21, "Cacciapaglia", 20, "", -1},
                {22, "Prokofiev", 21, "", -1},
                {23, "Musorgskii", 22, "", -1}
            });
        }

        {
            TableDefinition schema = SchemaBuilders.tableBuilder("PUBLIC", "UNWRAP_PK")
                .columns(
                    SchemaBuilders.column("F1", ColumnType.string()).asNullable().build(),
                    SchemaBuilders.column("F2", ColumnType.INT64).asNullable().build(),
                    SchemaBuilders.column("F3", ColumnType.INT64).asNullable().build(),
                    SchemaBuilders.column("F4", ColumnType.INT64).asNullable().build()
                )
                .withPrimaryKey(
                    SchemaBuilders.primaryKey().withColumns("F2", "F1").build()
                )
                .withIndex(
                    SchemaBuilders.sortedIndex(PK_IDX)
                        .addIndexColumn("F2").done()
                        .addIndexColumn("F1").done()
                        .build()
                )
                .build();

            Table dev = CLUSTER_NODES.get(0).tables().createTable(schema.canonicalName(), tblCh ->
                SchemaConfigurationConverter.convert(schema, tblCh)
                    .changeReplicas(2)
                    .changePartitions(10)
            );

            insertData(dev, new String[] {"F1", "F2", "F3", "F4"}, new Object[][] {
                {"Petr", 1L, 2L, 3L},
                {"Ivan", 2L, 2L, 4L},
                {"Ivan1", 21L, 2L, 4L},
                {"Ivan2", 22L, 2L, 4L},
                {"Ivan3", 23L, 2L, 4L},
                {"Ivan4", 24L, 2L, 4L},
                {"Ivan5", 25L, 2L, 4L},
            });
        }
    }

    /** */
    @Test
    public void testEqualsFilterWithUnwrpKey() {
        assertQuery("SELECT F1 FROM UNWRAP_PK WHERE F2=2")
            .matches(containsIndexScan("PUBLIC", "UNWRAP_PK", PK_IDX))
            .returns("Ivan")
            .check();
    }

    /** */
    @Test
    public void testIndexLoopJoin() {
        assertQuery("" +
            "SELECT /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter') */ d1.name, d2.name " +
            "FROM Developer d1, Developer d2 WHERE d1.id = d2.id")
            .matches(containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
            .returns("Bach", "Bach")
            .returns("Beethoven", "Beethoven")
            .returns("Mozart", "Mozart")
            .returns("Strauss", "Strauss")
            .returns("Vagner", "Vagner")
            .returns("Chaikovsky", "Chaikovsky")
            .returns("Verdy", "Verdy")
            .returns("Stravinsky", "Stravinsky")
            .returns("Rahmaninov", "Rahmaninov")
            .returns("Shubert", "Shubert")
            .returns("Glinka", "Glinka")
            .returns("Arnalds", "Arnalds")
            .returns("Glass", "Glass")
            .returns("O'Halloran", "O'Halloran")
            .returns("Prokofiev", "Prokofiev")
            .returns("Yiruma", "Yiruma")
            .returns("Cacciapaglia", "Cacciapaglia")
            .returns("Einaudi", "Einaudi")
            .returns("Hasaishi", "Hasaishi")
            .returns("Marradi", "Marradi")
            .returns("Musorgskii", "Musorgskii")
            .returns("Rihter", "Rihter")
            .returns("Zimmer", "Zimmer")
            .check();
    }

    // ===== No filter =====

    /** */
    @Test
    public void testNoFilter() {
        assertQuery("SELECT * FROM Developer")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    // ===== id filter =====

    /** */
    @Test
    public void testKeyEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>? and id<?")
            .withParams(3, 12)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX))
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id>=3 and id<12")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testKeyLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testKeyLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE id<=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", PK_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    // ===== indexed field filter =====

    /** */
    @Test
    public void testIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId=2")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>21")
            .withParams(3)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId>=21")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<?")
            .withParams(3)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    /** */
    @Test
    public void testIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE depId<=?")
            .withParams(2)
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .check();
    }

    // ===== non-indexed field filter =====

    /** */
    @Test
    public void testNonIndexedFieldEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age=?")
            .withParams(44)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>?")
            .withParams(50)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldGreaterThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age>=?")
            .withParams(34)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<?")
            .withParams(56)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    /** */
    @Test
    public void testNonIndexedFieldLessThanOrEqualsFilter() {
        assertQuery("SELECT * FROM Developer WHERE age<=?")
            .withParams(55)
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Test
    public void testComplexIndexCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition2() {
        assertQuery("SELECT * FROM Developer WHERE depId=? AND name=?")
            .withParams(3, "Mozart")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition4() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Leipzig'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition5() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition6() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition7() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition8() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=2 AND age>20")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition9() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId>=2 AND city>='Vienna'")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX, DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition10() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND city>='Vienna'")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX, NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition11() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition12() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition13() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND depId>=3 AND city='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition14() {
        assertQuery("SELECT * FROM Developer WHERE name>='Mozart' AND depId=3 AND city>='Vienna'")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition15() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Vienna'")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testComplexIndexCondition16() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND (city='Vienna' AND depId=3)")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testEmptyResult() {
        assertQuery("SELECT * FROM Developer WHERE age=33 AND city='Leipzig'")
            .matches(containsTableScan("PUBLIC", "DEVELOPER"))
            .check();
    }

    /** */
    @Test
    public void testOrCondition1() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' OR age=55")
            .matches(containsUnion(true))
            .matches(anyOf(
                containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX),
                containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            )
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-13710")
    public void testOrCondition2() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (depId=1 OR depId=3)")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-13710")
    public void testOrCondition3() {
        assertQuery("SELECT * FROM Developer WHERE name='Mozart' AND (age > 22 AND (depId=1 OR depId=3))")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .check();
    }

    /** */
    @Test
    public void testOrCondition4() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR (name='Mozart' AND depId=3)")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_DEPID_CITY_IDX))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .check();
    }

    /** */
    @Test
    public void testOrCondition5() {
        assertQuery("SELECT * FROM Developer WHERE depId=1 OR name='Mozart'")
            .matches(containsUnion(true))
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .check();
    }

    // ===== various complex conditions =====

    /** */
    @Test
    public void testOrderByKey() {
        assertQuery("SELECT * FROM Developer WHERE id<=4 ORDER BY id")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER"))
            .matches(not(containsSubPlan("IgniteSort")))
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByDepId() {
        assertQuery("SELECT * FROM Developer ORDER BY depId")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", DEPID_IDX))
            .matches(not(containsSubPlan("IgniteSort")))
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)

            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(14, "Rihter", 13, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)

            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityAsc() {
        assertQuery("SELECT * FROM Developer ORDER BY name, city")
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .matches(containsAnyScan("PUBLIC", "DEVELOPER"))
            .matches(containsSubPlan("IgniteSort"))
            .returns(18, "Arnalds", 17, "", -1)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(23, "Musorgskii", 22, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(14, "Rihter", 13, "", -1)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNameCityDesc() {
        assertQuery("SELECT ID, NAME, DEPID, CITY, AGE FROM Developer ORDER BY name DESC, city DESC")
            .matches(containsIndexScan("PUBLIC", "DEVELOPER", NAME_CITY_IDX))
            .matches(not(containsSubPlan("IgniteSort")))
            .returns(16, "Zimmer", 15, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(14, "Rihter", 13, "", -1)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(15, "Marradi", 14, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(13, "Glass", 12, "", -1)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(18, "Arnalds", 17, "", -1)
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testOrderByNoIndexedColumn() {
        assertQuery("SELECT * FROM Developer ORDER BY age DESC")
            .matches(containsAnyProject("PUBLIC", "DEVELOPER"))
            .matches(containsSubPlan("IgniteSort"))
            .returns(8, "Stravinsky", 7, "Spt", 89)
            .returns(7, "Verdy", 6, "Rankola", 88)
            .returns(9, "Rahmaninov", 8, "Starorussky ud", 70)
            .returns(5, "Vagner", 4, "Leipzig", 70)
            .returns(4, "Strauss", 2, "Munich", 66)
            .returns(3, "Bach", 1, "Leipzig", 55)
            .returns(6, "Chaikovsky", 5, "Votkinsk", 53)
            .returns(11, "Glinka", 10, "Smolenskaya gb", 53)
            .returns(2, "Beethoven", 2, "Vienna", 44)
            .returns(1, "Mozart", 3, "Vienna", 33)
            .returns(10, "Shubert", 9, "Vienna", 31)
            .returns(14, "Rihter", 13, "", -1)
            .returns(13, "Glass", 12, "", -1)
            .returns(12, "Einaudi", 11, "", -1)
            .returns(20, "O'Halloran", 19, "", -1)
            .returns(23, "Musorgskii", 22, "", -1)
            .returns(19, "Yiruma", 18, "", -1)
            .returns(21, "Cacciapaglia", 20, "", -1)
            .returns(22, "Prokofiev", 21, "", -1)
            .returns(16, "Zimmer", 15, "", -1)
            .returns(18, "Arnalds", 17, "", -1)
            .returns(17, "Hasaishi", 16, "", -1)
            .returns(15, "Marradi", 14, "", -1)
            .ordered()
            .check();
    }

    /**
     * Test verifies that ranges would be serialized and desirialized without any errors.
     */
    @Test
    public void testSelectWithRanges() {
        String sql = "select depId from Developer " +
            "where depId in (1,2,3,5,6,7,9,10,13,14,15,18,19,20,21,22,23,24,25,26,27,28,30,31,32,33) " +
            "   or depId between 7 and 8 order by depId limit 5";

        assertQuery(sql)
            .returns(1)
            .returns(2)
            .returns(2)
            .returns(3)
            .returns(5)
            .check();
    }
}
