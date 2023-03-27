/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rules;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsTableScan;

/** Tests correctness applying of JOIN_COMMUTE* rules. */
public class JoinCommuteRulesTest extends GridCommonAbstractTest {
    /** */
    private static QueryEngine qryEngine;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx client = startClientGrid();

        qryEngine = Commons.lookupComponent(client.context(), QueryEngine.class);

        qryEngine.query(null, "PUBLIC", "CREATE TABLE HUGE(id int)");
        qryEngine.query(null, "PUBLIC", "CREATE TABLE SMALL(id int)");

        for (int i = 0; i < 100; ++i) {
            List<FieldsQueryCursor<List<?>>> req = qryEngine.query(null, "PUBLIC",
                "INSERT INTO HUGE VALUES(" + i + ")");
            req.get(0).getAll();
        }

        for (int i = 0; i < 10; ++i) {
            List<FieldsQueryCursor<List<?>>> req = qryEngine.query(null, "PUBLIC",
                "INSERT INTO SMALL VALUES(" + i + ")");
            req.get(0).getAll();
        }
    }

    /** */
    @Test
    public void testCommuteOuter() {
        String sql = "SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter') */ " +
            "COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        checkQuery(sql)
            .matches(containsTableScan("PUBLIC", "HUGE"))
            .matches(containsTableScan("PUBLIC", "SMALL"))
            .matches(containsSubPlan("IgniteNestedLoopJoin(condition=[=($0, $1)], joinType=[left]"))
            .check();

        sql = "SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter', 'JoinCommuteRule') */ " +
            "COUNT(*) FROM SMALL s RIGHT JOIN HUGE h on h.id = s.id";

        checkQuery(sql)
            .matches(containsTableScan("PUBLIC", "HUGE"))
            .matches(containsTableScan("PUBLIC", "SMALL"))
            .matches(containsSubPlan("IgniteNestedLoopJoin(condition=[=($1, $0)], joinType=[right]"))
            .check();
    }

    /** */
    @Test
    public void testCommuteInner() {
        String sql = "SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter') */ " +
            "COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id";

        checkQuery(sql)
            .matches(containsTableScan("PUBLIC", "HUGE"))
            .matches(containsTableScan("PUBLIC", "SMALL"))
            .matches(containsSubPlan("IgniteNestedLoopJoin(condition=[=($0, $1)], joinType=[inner]"))
            .check();

        sql = "SELECT /*+ DISABLE_RULE('CorrelatedNestedLoopJoin', 'MergeJoinConverter', 'JoinCommuteRule') */ " +
            "COUNT(*) FROM SMALL s JOIN HUGE h on h.id = s.id";

        checkQuery(sql)
            .matches(containsTableScan("PUBLIC", "HUGE"))
            .matches(containsTableScan("PUBLIC", "SMALL"))
            .matches(containsSubPlan("IgniteNestedLoopJoin(condition=[=($1, $0)], joinType=[inner]"))
            .check();
    }

    /** */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return qryEngine;
            }
        };
    }
}
