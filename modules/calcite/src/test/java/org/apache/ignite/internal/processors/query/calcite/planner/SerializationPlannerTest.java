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

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 * Test fragments serialization/deserialization (with different SQL operators).
 */
public class SerializationPlannerTest extends AbstractPlannerTest {
    /** */
    @Test
    public void testNotStandardFunctionsSerialization() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("TEST", IgniteDistributions.affinity(0, "TEST", "hash"),
                "ID", Integer.class, "VAL", String.class)
        );

        String queries[] = {
            "select REVERSE(val) from TEST", // MYSQL
            "select DECODE(id, 0, val, '') from TEST" // ORACLE
        };

        for (String sql : queries) {
            IgniteRel phys = physicalPlan(sql, publicSchema);

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    /** */
    @Test
    public void testMinusDateSerialization() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        IgniteRel phys = physicalPlan("SELECT (DATE '2021-03-01' - DATE '2021-01-01') MONTHS", publicSchema);

        checkSplitAndSerialization(phys, publicSchema);
    }

    /** */
    @Test
    public void testFloatSerialization() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        IgniteRel phys = physicalPlan("SELECT " + Integer.MAX_VALUE + "::FLOAT, " +
            Long.MAX_VALUE + "::FLOAT" +
            "-17014118346046923173168730371588410572::FLOAT" +
            "-17014118346046923173.168730371588410572::FLOAT",
            publicSchema);

        checkSplitAndSerialization(phys, publicSchema);
    }

    /** */
    @Test
    public void testLiteralAggSerialization() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("ORDERS", single(), "ID", Integer.class),
            createTable("ORDER_ITEMS", single(), "ID", Integer.class, "ORDER_ID", Integer.class)
        );

        assertPlan("SELECT * FROM orders WHERE id not in (SELECT order_id FROM order_items)", publicSchema,
            hasChildThat(isInstanceOf(Aggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(
                    aggCall -> aggCall.getAggregation().getKind() == SqlKind.LITERAL_AGG)
                )
            )
        );
    }
}
