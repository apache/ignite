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

import java.math.BigInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** */
public class DynamicParametersPlannerTest extends AbstractPlannerTest {
    /** Dynamic parameters in LIMIT / OFFSET. */
    @Test
    public void testLimitOffset() throws Exception {
        IgniteSchema schema = createSchema(createTable("T1", single(), "c1", Integer.class));

        TestPlanningContextBuilder builder = contextBuilder().query("SELECT * FROM t1 LIMIT ?").schema(schema);

        assertPlan(builder.params(Long.MAX_VALUE));

        // Count of dynamic parameters need to be invalidated, remove it after: IGNITE-28906
        assertPlan(builder.params(Long.MAX_VALUE, -1));

        assertThrows(() -> assertPlan(builder.params("a")), IgniteException.class,
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <VARCHAR>");

        BigInteger moreThanMaxLong = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        assertThrows(() -> assertPlan(builder.params(moreThanMaxLong)), IgniteException.class,
            "Illegal value of fetch / limit");

        assertThrows(() -> assertPlan(builder.params(-1)), IgniteException.class,
            "Illegal value of fetch / limit");

        assertThrows(() -> assertPlan(builder.params((Object)null)), IgniteException.class,
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        // OFFSET.
        builder.query("SELECT * FROM t1 OFFSET ?");

        assertThrows(() -> assertPlan(builder.params(moreThanMaxLong)), IgniteException.class,
            "Illegal value of offset");

        assertThrows(() -> assertPlan(builder.params(-1)), IgniteException.class,
            "Illegal value of offset");

        assertThrows(() -> assertPlan(builder.params((Object)null)), IgniteException.class,
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        // OFFSET Alternate syntax.
        builder.query("SELECT * FROM t1 OFFSET ? ROWS");

        assertThrows(() -> assertPlan(builder.params(moreThanMaxLong)), IgniteException.class,
            "Illegal value of offset");

        assertThrows(() -> assertPlan(builder.params(-1)), IgniteException.class,
            "Illegal value of offset");

        assertThrows(() -> assertPlan(builder.params((Object)null)), IgniteException.class,
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        // Expression.
        builder.query("SELECT * FROM TEST_REPL OFFSET 2+? ROWS");

        assertThrows(() -> assertPlan(builder), IgniteException.class,
            "Encountered \" \"+\"");
    }
}
