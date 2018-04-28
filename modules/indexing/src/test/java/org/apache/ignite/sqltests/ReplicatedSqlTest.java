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

package org.apache.ignite.sqltests;

import java.util.Arrays;
import java.util.List;

public class ReplicatedSqlTest extends BaseSqlTest {
    @Override protected void setupData() {
        createTables("template=replicated");

        fillCommonData();
    }

    public void testCrossJoin() {
        testAllNodes(node -> {
            Result act1 = executeFrom("SELECT e.id, d.id FROM Employee e, Department d", node);
            Result act2 = executeFrom("SELECT e.id, d.id FROM Employee e CROSS JOIN Department d", node);
            // todo : add no idx join.
            // todo : compare with expected.
            assertContainsEq(act1.values(), act2.values());

            List<List<Object>> expected = doInnerJoin(node.cache(EMP_CACHE_NAME), node.cache(DEP_CACHE_NAME),
                (emp, dep) -> true,
                (emp, dep) -> Arrays.asList(emp.get("ID"), dep.get("ID")));

            assertContainsEq(act1.values(), expected);

            assertEquals("Result size of the cross join is unexpected",
                DEP_CNT * EMP_CNT, act1.values().size());
        });
    }
}
