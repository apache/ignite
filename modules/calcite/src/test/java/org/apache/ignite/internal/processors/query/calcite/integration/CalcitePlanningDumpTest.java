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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteCommonsSystemProperties.getLong;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_PLANNER_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
@WithSystemProperty(key = IGNITE_CALCITE_PLANNER_TIMEOUT, value = "1000")
public class CalcitePlanningDumpTest extends AbstractBasicIntegrationTest {
    /** */
    private static final long PLANNER_TIMEOUT = getLong(IGNITE_CALCITE_PLANNER_TIMEOUT, 0);

    /** */
    @Test
    public void testTruncatedCalciteDump() {
        Stream.of("T1", "T2").forEach(tblName -> {
            sql(String.format("CREATE TABLE %s(A INT, B INT)", tblName));

            IgniteTable tbl = (IgniteTable)queryProcessor(client).schemaHolder().schema("PUBLIC").getTable(tblName);

            tbl.addIndex(new DelegatingIgniteIndex(tbl.getIndex(QueryUtils.PRIMARY_KEY_INDEX)) {
                @Override public RelCollation collation() {
                    doSleep(PLANNER_TIMEOUT);

                    return delegate.collation();
                }
            });

            sql(String.format("INSERT INTO %s(A, B) VALUES (1, 1)", tblName));
        });

        String longJoinQry = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        Throwable thrown = assertThrowsWithCause(() -> sql(longJoinQry), RelOptPlanner.CannotPlanException.class);

        Throwable cannotPlanE = Arrays.stream(thrown.getSuppressed())
            .filter(e -> e instanceof RelOptPlanner.CannotPlanException)
            .findFirst()
            .orElseThrow();

        String cannotPlanMsg = cannotPlanE.getMessage();

        assertTrue(cannotPlanMsg.contains("Original rel:"));

        assertFalse(cannotPlanMsg.contains("Sets:"));

        assertFalse(cannotPlanMsg.contains("Graphviz:"));
    }
}
