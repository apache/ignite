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

package org.apache.ignite.internal.processors.query.calcite;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Predicate;
import javax.management.DynamicMBean;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.CommandMBean;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.ExposeIndexRule;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.not;

/** */
public class CalciteQueryProcessorPropertiesTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        changeDistributedProperty(DistributedCalciteConfiguration.DISABLED_RULES_PROPERTY_NAME, " ",
            pVal -> F.compareArrays(pVal, new String[0]) == 0);
    }

    /** */
    @Test
    public void testDisableRuleDistributedPropertyCommand() throws Exception {
        String propName = DistributedCalciteConfiguration.DISABLED_RULES_PROPERTY_NAME;

        for (Ignite ig : G.allGrids()) {
            DistributedChangeableProperty<String[]> prop = distributedProperty(ig, propName);

            assertNotNull(prop);
            assertNotNull(prop.get());
            assertTrue(prop.get().length == 0);
        }

        // Check simple value.
        changeDistributedProperty(propName, "ExposeIndexRule",
            pVal -> F.compareArrays(new String[] {"ExposeIndexRule"}, pVal) == 0);

        // Check setting with spaces.
        changeDistributedProperty(propName, ",, ExposeIndexRule   , ,\t, CorrelatedNestedLoopJoinRule  ",
            pVal -> F.compareArrays(new String[] {"ExposeIndexRule", "CorrelatedNestedLoopJoinRule"}, pVal) == 0);
    }

    /**
     * Tests the ability to disable planning rules globally.
     *
     * @see DistributedCalciteConfiguration#disabledRules()}.
     */
    @Test
    public void testDisableRuleDistributedProperty() throws Exception {
        try {
            sql("create table test_tbl1 (c1 int, c2 int, c3 int)");
            sql("create index idx12 on test_tbl1(c2)");
            sql("create index idx13 on test_tbl1(c3)");

            assertQuery(client, "select * from test_tbl1 order by c2")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12"))
                .matches(not(containsSubPlan("IgniteSort")))
                .check();
            assertQuery(client, "select * from test_tbl1 order by c3")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX13"))
                .matches(not(containsSubPlan("IgniteSort")))
                .check();

            String propName = DistributedCalciteConfiguration.DISABLED_RULES_PROPERTY_NAME;

            changeDistributedProperty(propName, ExposeIndexRule.INSTANCE.toString(),
                pVal -> F.compareArrays(pVal, new String[] {ExposeIndexRule.INSTANCE.toString()}) == 0);

            // Check that there are no index scans now.
            assertQuery(client, "select * from test_tbl1 order by c2")
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12")))
                .matches(containsSubPlan("IgniteSort"))
                .check();
            assertQuery(client, "select * from test_tbl1 order by c3")
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX13")))
                .matches(containsSubPlan("IgniteSort"))
                .check();

            changeDistributedProperty(propName, " ", pVal -> ((String[])pVal).length == 0);

            // Check that the index are available again.
            assertQuery(client, "select * from test_tbl1 order by c2")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12"))
                .matches(not(containsSubPlan("IgniteSort")))
                .check();
            assertQuery(client, "select * from test_tbl1 order by c3")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX13"))
                .matches(not(containsSubPlan("IgniteSort")))
                .check();
        }
        finally {
            sql("drop table if exists test_tbl1");
        }
    }

    /** */
    @Test
    public void testDisabledRulesByHintAndGrlobalProperty() throws Exception {
        String propName = DistributedCalciteConfiguration.DISABLED_RULES_PROPERTY_NAME;

        try {
            sql("create table test_tbl1 (c1 int, c2 int, c3 int)");
            sql("create index idx12 on test_tbl1(c2)");

            sql("create table test_tbl2 (c1 int, c2 int, c3 int)");
            sql("create index idx22 on test_tbl2(c3)");

            // Ensure the index is enabled by default.
            assertQuery(client, "select * from test_tbl1 order by c2")
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12"))
                .matches(not(containsSubPlan("IgniteSort")))
                .check();

            // Ensure the indices are active with MergeJoin by default.
            assertQuery(client, "select /*+ MERGE_JOIN */ t1.c1,t2.c3 from test_tbl1 t1, test_tbl2 t2 where t1.c2=t2.c3")
                .matches(containsSubPlan("IgniteMergeJoin"))
                .matches(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12"))
                .matches(containsIndexScan("PUBLIC", "TEST_TBL2", "IDX22"))
                .check();

            // The index scan rule is now disabled by the global property.
            changeDistributedProperty(propName, ExposeIndexRule.INSTANCE.toString(),
                pVal -> F.compareArrays(pVal, new String[] {ExposeIndexRule.INSTANCE.toString()}) == 0);

            assertQuery(client, "select * from test_tbl1 order by c2")
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12")))
                .matches(containsSubPlan("IgniteSort"))
                .check();

            // Test that global property overrides hint.
            assertQuery(client, "select /*+ FORCE_INDEX(IDX12) */ * from test_tbl1 order by c2")
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12")))
                .matches(containsSubPlan("IgniteSort"))
                .check();

            // MergeJoin is on, but the indices are disabled by the global rule.
            assertQuery(client, "select /*+ MERGE_JOIN */ t1.c1,t2.c3 from test_tbl1 t1, test_tbl2 t2 where t1.c2=t2.c3")
                .matches(containsSubPlan("IgniteMergeJoin"))
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL1", "IDX12")))
                .matches(not(containsIndexScan("PUBLIC", "TEST_TBL2", "IDX22")))
                .check();

            String[] disabledRules = new String[] {"MergeJoinConverter"};

            changeDistributedProperty(propName, "MergeJoinConverter", pVal -> F.compareArrays(pVal, disabledRules) == 0);

            // Ensure that hint and global property are able to work together. The MergeJoin enforcing hint disables
            // other join types. But it won't run because its rule is disabled by the global property. So, the planner is
            // unable to build plan.
            GridTestUtils.assertThrows(
                null,
                () -> sql("select /*+ MERGE_JOIN */ t1.c1,t2.c3 from test_tbl1 t1, test_tbl2 t2 where t1.c2=t2.c3"),
                IgniteSQLException.class,
                "Failed to plan query"
            );
        }
        finally {
            sql("drop table if exists test_tbl1");
            sql("drop table if exists test_tbl2");
        }
    }

    /** */
    private static <T extends Serializable> DistributedChangeableProperty<T> distributedProperty(Ignite ig, String propName) {
        return ((IgniteEx)ig).context().distributedConfiguration().property(propName);
    }

    /** */
    private <T extends Serializable> void changeDistributedProperty(
        String propName,
        Object val,
        @Nullable Predicate<T> newValChacker
    ) throws Exception {
        DynamicMBean mbean = getMxBean(
            client.context().igniteInstanceName(),
            "management",
            Collections.singletonList("Property"),
            "Set",
            DynamicMBean.class
        );

        Object[] beanCallParams = new Object[2];
        String[] callParamsType = new String[2];

        beanCallParams[0] = propName;
        callParamsType[0] = String.class.getName();

        beanCallParams[1] = val;
        callParamsType[1] = val == null ? Void.class.getName() : val.getClass().getName();

        mbean.invoke(CommandMBean.INVOKE, beanCallParams, callParamsType);

        if (newValChacker == null)
            return;

        assertTrue(waitForCondition(
            () -> {
                for (Ignite ig : G.allGrids()) {
                    DistributedChangeableProperty<T> prop0 = distributedProperty(ig, propName);

                    if (!newValChacker.test(prop0.get()))
                        return false;
                }

                return true;
            },
            getTestTimeout()
        ));
    }
}
