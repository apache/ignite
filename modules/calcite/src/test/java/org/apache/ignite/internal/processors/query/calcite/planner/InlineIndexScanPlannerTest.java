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

import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Planner test for index inline scan.
 */
public class InlineIndexScanPlannerTest extends AbstractPlannerTest {
    /** */
    @Test
    public void testInlinScan() throws Exception {
        TestTable tbl = createTable("TBL", 100, IgniteDistributions.single(),
            "I0", UUID.class,
            "I1", Integer.class,
            "I2", Long.class,
            "I3", String.class,
            "I4", Timestamp.class,
            "I5", Byte.class,
            "I6", Object.class
        );

        tbl.addIndex("IDX1", 0, 2, 4);
        tbl.addIndex("IDX2", 2, 0);
        tbl.addIndex("IDX3", 5, 3);
        tbl.addIndex("IDX4", 5, 6);

        IgniteSchema publicSchema = createSchema(tbl);

        // Index IDX1 the only possible index with inlined I4 key.
        assertPlan("SELECT I4 FROM TBL", publicSchema, isIndexScan("TBL", "IDX1")
            .and(AbstractIndexScan::isInlineScan));

        // Index IDX2 has less keys count than IDX1.
        assertPlan("SELECT I2 FROM TBL", publicSchema, isIndexScan("TBL", "IDX2")
            .and(AbstractIndexScan::isInlineScan));

        // But if we have filter on the first key than IDX1 is prefered.
        assertPlan("SELECT I2 FROM TBL WHERE I0 = ?", publicSchema, isIndexScan("TBL", "IDX1")
            .and(AbstractIndexScan::isInlineScan));

        // Index IDX1 is prefered, but inline scan can't be used, since I1 is not inlined.
        assertPlan("SELECT I2 FROM TBL WHERE I0 = ? AND I1 = ?", publicSchema, isIndexScan("TBL", "IDX1")
            .and(i -> !i.isInlineScan()));

        // Don't use variable length types for inline scans.
        assertPlan("SELECT I3 FROM TBL", publicSchema, isTableScan("TBL"));

        // Don't use objects for inline scans.
        assertPlan("SELECT I6 FROM TBL", publicSchema, isTableScan("TBL"));

        // Don't use any indexes that contain variable length types or objects for inline scans.
        assertPlan("SELECT I5 FROM TBL", publicSchema, isTableScan("TBL"));
    }
}
