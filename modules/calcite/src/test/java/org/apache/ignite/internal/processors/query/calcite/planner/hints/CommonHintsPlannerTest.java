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

package org.apache.ignite.internal.processors.query.calcite.planner.hints;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Common test for SQL hints.
 */
public class CommonHintsPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        // Sets CalciteQueryEngines's logger for the hints warnings.
        try {
            Field f = U.findField(CalciteQueryProcessor.class, "HINTS_LOG_SUPPLIER");

            f.setAccessible(true);

            ((AtomicReference<IgniteLogger>)f.get(null)).set(lsnrLog);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Unble to setup test.", e);
        }

        tbl = createTable("TBL", 100, IgniteDistributions.random(), "ID", Integer.class, "VAL",
            Integer.class).addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0).addIndex("IDX", 1);

        schema = createSchema(tbl);
    }

    /**
     * Tests hint 'DISABLE_RULE' works for whole query despite it is not set for the root node.
     */
    @Test
    public void testDisableRuleInHeader() throws Exception {
        assertPlan("SELECT /*+ DISABLE_RULE('ExposeIndexRule') */ VAL FROM TBL UNION ALL " +
            "SELECT VAL FROM TBL", schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());

        assertPlan("SELECT VAL FROM TBL where val=1 UNION ALL " +
                "SELECT /*+ DISABLE_RULE('ExposeIndexRule') */ VAL FROM TBL", schema,
            nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());
    }

    /** */
    @Test
    public void testWrongParamsDisableRule() throws Exception {
        LogListener lsnr = LogListener.matches("Hint 'DISABLE_RULE' must have at least one option").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ DISABLE_RULE */ VAL FROM TBL", schema);

        assertTrue(lsnr.check());

        lsnrLog.registerListener(lsnr);

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Hint 'DISABLE_RULE' can't have any key-value option").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ DISABLE_RULE(a='b') */ VAL FROM TBL", schema);

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testWrongParamsExpandDistinct() throws Exception {
        LogListener lsnr = LogListener.matches("Hint 'EXPAND_DISTINCT_AGG' can't have any option").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ EXPAND_DISTINCT_AGG(OPTION) */ VAL FROM TBL", schema);

        assertTrue(lsnr.check());

        lsnrLog.registerListener(lsnr);

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Hint 'EXPAND_DISTINCT_AGG' can't have any option").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ EXPAND_DISTINCT_AGG(a='b') */ VAL FROM TBL", schema);

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testWrongHint() throws Exception {
        LogListener lsnr =
            LogListener.matches("Hint: UNEXISTING_HINT should be registered in the HintStrategyTable").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ UNEXISTING_HINT */ VAL FROM TBL", schema);

        assertTrue(lsnr.check());
    }
}
