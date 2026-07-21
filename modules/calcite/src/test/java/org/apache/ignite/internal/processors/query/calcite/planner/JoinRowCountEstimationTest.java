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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.math.BigDecimal;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdSelectivity.COMPARISON_SELECTIVITY;
import static org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdSelectivity.EQUALS_SELECTIVITY;
import static org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdSelectivity.IS_NOT_NULL_SELECTIVITY;

/**
 * Tests to check row count estimation for join relation.
 */
public class JoinRowCountEstimationTest extends AbstractPlannerTest {
    /** */
    private static final int CATALOG_SALES_SIZE = 1_441_548;

    /** */
    private static final int CATALOG_RETURNS_SIZE = 144_067;

    /** */
    private static final int DATE_DIM_SIZE = 73_049;

    /** */
    private static final String SELECT = "SELECT /*+ DISABLE_RULE(" +
        "'JoinCommuteRule', " +
        "'MergeJoinConverter', " +
        "'NestedLoopJoinConverter', " +
        "'CorrelateToNestedLoopRule'" +
        ") */ * ";

    /** */
    private IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        TestTable tbl1 = createTable(
            "CATALOG_SALES",
            CATALOG_SALES_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(1, 2), CU.cacheId("default"), 0),
            "ID", Integer.class,
            "CS_ITEM_SK", Integer.class,
            "CS_ORDER_NUMBER", Integer.class,
            "CS_PROMO_SK", Integer.class
        );

        TestTable tbl2 = createTable(
            "CATALOG_RETURNS",
            CATALOG_RETURNS_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(1, 2), CU.cacheId("default"), 0),
            "ID", Integer.class,
            "CR_ITEM_SK", Integer.class,
            "CR_ORDER_NUMBER", Integer.class,
            "CR_RETURNED_DATE_SK", Integer.class,
            "CR_RETURN_QUANTITY", Integer.class
        );

        TestTable tbl3 = createTable(
            "DATE_DIM",
            DATE_DIM_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(0), CU.cacheId("default"), 0),
            "D_DATE_SK", Integer.class,
            "D_MOY", Integer.class
        );

        publicSchema = createSchema(tbl1, tbl2, tbl3);
    }

    /** */
    @Test
    public void joinSameTableSimpleAffMergeJoin() throws Exception {
        assertPlan(SELECT
            + "  FROM catalog_sales"
            + "      ,catalog_returns"
            + "  WHERE cs_item_sk = cr_item_sk"
            + "    AND cs_order_number = cr_order_number",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)));

        // It needs to return like: CATALOG_RETURNS_SIZE * IS_NOT_NULL_SELECTIVITY, but it will be done at future
        // Need to adopt: IGNITE-23969
        assertPlan(SELECT
            + "  FROM catalog_sales"
            + "      ,catalog_returns"
            + "  WHERE cs_item_sk = cr_item_sk"
            + "    AND cs_order_number = cr_order_number"
            + "    AND cs_promo_sk IS NOT NULL",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)));
    }

    /** */
    @Test
    public void joinByPrimaryKeysLeft() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));

        assertPlan(SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));

        assertPlan(SELECT
                + "  FROM catalog_sales LEFT JOIN catalog_returns"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));
    }

    /** */
    @Test
    public void joinByPrimaryKeysRight() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));

        assertPlan(SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));

        assertPlan(SELECT
                + "  FROM catalog_returns RIGHT JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_SALES_SIZE)));
    }

    /** */
    @Test
    public void joinByPrimaryKeysFull() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales ON"
                + "  cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual((int)((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                * (1.0 - (EQUALS_SELECTIVITY * EQUALS_SELECTIVITY))))));

        assertPlan(SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cs_promo_sk IS NOT NULL",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual((int)((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                * (1.0 - (EQUALS_SELECTIVITY * EQUALS_SELECTIVITY * IS_NOT_NULL_SELECTIVITY))))));

        assertPlan(SELECT
                + "  FROM catalog_returns FULL OUTER JOIN catalog_sales"
                + "  ON cs_item_sk = cr_item_sk"
                + "    AND cs_order_number = cr_order_number"
                + "    AND cr_order_number > 1",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual((int)((CATALOG_SALES_SIZE + CATALOG_RETURNS_SIZE)
                * (1.0 - (EQUALS_SELECTIVITY * EQUALS_SELECTIVITY * COMPARISON_SELECTIVITY))))));
    }

    /** */
    @Test
    public void joinByForeignKey() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_returns"
                + "      ,date_dim"
                + "  WHERE cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual(CATALOG_RETURNS_SIZE)));

        // It needs to return like: CATALOG_RETURNS_SIZE * COMPARISON_SELECTIVITY, but it will be done at future
        // Need to adopt: IGNITE-23969
        assertPlan(SELECT
                + "  FROM date_dim"
                + "      ,catalog_returns"
                + "  WHERE cr_returned_date_sk = d_date_sk"
                + "    AND d_moy > 6",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));
    }

    /** */
    @Test
    public void joinByForeignKeyLeft() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_returns LEFT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));

        assertPlan(SELECT
                + "  FROM date_dim LEFT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));

        assertPlan(SELECT
                + "  FROM catalog_returns LEFT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));
    }

    /** */
    @Test
    public void joinByForeignKeyRight() throws Exception {
        assertPlan(SELECT
                + "  FROM catalog_returns RIGHT JOIN date_dim ON"
                + "  cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));

        assertPlan(SELECT
                + "  FROM date_dim RIGHT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));

        assertPlan(SELECT
                + "  FROM date_dim RIGHT JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6",
            publicSchema,
            nodeRowCount("IgniteHashJoin", CoreMatchers.is(CATALOG_RETURNS_SIZE)));
    }

    /** */
    @Test
    public void joinByForeignKeyFull() throws Exception {
        assertPlan(SELECT
                + "  FROM date_dim FULL OUTER JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual((int)((DATE_DIM_SIZE + CATALOG_RETURNS_SIZE)
                * (1.0 - (EQUALS_SELECTIVITY))))));

        assertPlan(SELECT
                + "  FROM date_dim FULL OUTER JOIN catalog_returns ON"
                + "  cr_returned_date_sk = d_date_sk"
                + "     AND cr_return_quantity > 6",
            publicSchema,
            nodeRowCount("IgniteHashJoin", approximatelyEqual((int)((DATE_DIM_SIZE + CATALOG_RETURNS_SIZE)
                * (1.0 - (EQUALS_SELECTIVITY * COMPARISON_SELECTIVITY))))));
    }

    /**
     * Matcher which ensures that node matching the pattern has row count matching provided matcher.
     *
     * @param nodePattern A pattern describing a node of interest.
     * @param rowCountMatcher Matcher for the estimated row count.
     * @return Matcher.
     */
    static <T extends RelNode> Predicate<RelNode> nodeRowCount(String nodePattern, Matcher<Integer> rowCountMatcher) {
        Pattern pattern = Pattern.compile(".*" + nodePattern
            + "\\(.*?rowcount = (?<rowcount>\\d+).*");

        return node -> {
            String plan = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);

            String sanitized = plan.replace("\n", "");
            java.util.regex.Matcher matcher = pattern.matcher(sanitized);

            if (!matcher.matches())
                return false;

            String rowCntStr = matcher.group("rowcount");

            return rowCountMatcher.matches(new BigDecimal(rowCntStr).intValue());
        };
    }

    /** Approximate equality check. */
    static Matcher<Integer> approximatelyEqual(double expected) {
        /** */
        return new BaseMatcher<>() {
            @Override public boolean matches(Object o) {
                if (!(o instanceof Integer)) {
                    return false;
                }

                double val = ((Integer)o).doubleValue();
                // IgniteMdRowCount.EQUI_COEFF with a bit overhead
                return Math.abs(1 - (val / expected)) < 0.21;
            }

            @Override public void describeTo(Description description) {
                description.appendText("equals to ").appendValue(expected).appendText("± %");
            }
        };
    }
}
