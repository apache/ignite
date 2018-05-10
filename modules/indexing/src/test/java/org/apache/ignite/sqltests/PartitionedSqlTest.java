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

public class PartitionedSqlTest extends BaseSqlTest {
    @Override protected void setupData() {
        super.createTables("template=partitioned");

        fillCommonData();
    }

    public void testInnerDistJoin() {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);

            List<List<Object>> exp = doInnerJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
        });
    }

    public void testNegativeInnerDistJoin() {
        testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node));
            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node));
        });
    }

    public void testLeftDistJoin() {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);

            List<List<Object>> exp = doLeftJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
        });
    }

    public void testNegativeLeftDistJoin() {
        testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node));
            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "idNoIdx", "depIdNoidx")), node));
        });
    }

    public void testRightDistJoin() {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOnOff = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node);

            List<List<Object>> exp = doRightJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result.", actIdxOnOff.values(), exp);
        });
    }

    public void testNegativeRightDistJoin() {
        testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node));
            assertDistJoinHasIncorrectIndex(() -> executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node));
        });
    }
}
