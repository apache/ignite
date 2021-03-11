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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"FieldCanBeLocal"})
public class SortAggregatePlannerTest extends AbstractAggregatePlannerTest {
    /**
     *
     */
    @Test
    public void notApplicableForSortAggregate() {
        TestTable tbl = createAffinityTable().addIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "val0_val1");

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", tbl);

        String sqlMin = "SELECT MIN(val0) FROM test";

        GridTestUtils.assertThrows(log,
            () -> physicalPlan(
                sqlMin,
                publicSchema,
                "HashAggregateConverterRule"
            ),
            RelOptPlanner.CannotPlanException.class,
            "There are not enough rules to produce a node with desired properties"
        );
    }
}
