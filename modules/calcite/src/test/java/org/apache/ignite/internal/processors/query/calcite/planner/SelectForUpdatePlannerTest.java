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

import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**
 * Tests propagation of {@code SELECT ... FOR UPDATE} marker to {@link IgniteTableScan}.
 */
public class SelectForUpdatePlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        publicSchema = createSchema(createTable("TBL", IgniteDistributions.single(), "A", INTEGER, "B", INTEGER));
    }

    /** */
    @Test
    public void testForUpdateFlagPropagatesToTableScan() throws Exception {
        IgniteRel rel = physicalPlan("SELECT a FROM TBL FOR UPDATE", publicSchema);

        IgniteTableScan scan = findFirstNode(rel, byClass(IgniteTableScan.class));

        assertNotNull(scan);
        assertTrue(scan.getHints().stream().anyMatch(h -> HintDefinition.FOR_UPDATE.name().equals(h.hintName)));
    }

    /** */
    @Test
    public void testScanWithoutForUpdateDoesNotHaveFlag() throws Exception {
        IgniteRel rel = physicalPlan("SELECT a FROM TBL", publicSchema);

        IgniteTableScan scan = findFirstNode(rel, byClass(IgniteTableScan.class));

        assertNotNull(scan);
        assertFalse(scan.getHints().stream().anyMatch(h -> HintDefinition.FOR_UPDATE.name().equals(h.hintName)));
    }
}
