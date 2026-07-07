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
import java.util.function.Consumer;
import org.junit.Test;

/** */
public class DynamicParametersPlannerTest extends AbstractPlannerTest {
    /** Dynamic parameters in LIMIT / OFFSET. */
    @Test
    public void testLimitOffset() throws Exception {
        Consumer<StatementChecker> setup = (checker) -> {
            checker.table("T1", "c1", Integer.class);
        };

        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", Long.MAX_VALUE).ok();

        // Strange case, count of dynamic parameters need to be invalidated. After fix this check need to be removed.
        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", Long.MAX_VALUE, -1).ok();

        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", "a").fails(
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <VARCHAR>");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ?", "a").fails(
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <VARCHAR>");

        BigInteger moreThanMaxLong = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", moreThanMaxLong).fails(
            "Illegal value of fetch / limit");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ?", moreThanMaxLong).fails(
            "Illegal value of offset");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ? ROWS", moreThanMaxLong).fails(
            "Illegal value of offset");

        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", -1).fails(
            "Illegal value of fetch / limit");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ?", -1).fails(
            "Illegal value of offset");

        checkStatement(setup).sql("SELECT * FROM t1 LIMIT ?", (Object)null).fails(
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ?", (Object)null).fails(
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        checkStatement(setup).sql("SELECT * FROM t1 OFFSET ? ROWS", (Object)null).fails(
            "Incorrect type of a dynamic parameter. Expected <BIGINT> but got <null>");

        checkStatement(setup).sql("SELECT * FROM TEST_REPL OFFSET 2+? ROWS", 1).fails("Encountered \"+\"");
    }
}
