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

/**
 * Includes all base sql test plus tests that make sense in replicated mode.
 */
public class ReplicatedSqlTest extends BaseSqlTest {
    /** Name of the department table created in partitioned mode. */
    private String DEP_PART_TAB = "DepartmentPart";

    /**
     * Create and fill common tables in replicated mode.
     * Also create additional department table in partitioned mode to
     * test mixed partitioned/replicated scenarios.
     */
    @Override protected void setupData() {
        createTables("template=replicated");

        fillCommonData();

        createDepartmentTable("DepartmentPart", "template=partitioned");

        fillDepartmentTable("DepartmentPart");
    }

    /**
     * Checks distributed INNER JOIN of replicated and replicated tables.
     */
    public void testInnerDistributedJoinReplicatedReplicated() {
        checkInnerDistJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed INNER JOIN of partitioned and replicated tables.
     */
    public void testInnerDistJoinPartitionedReplicated() {
        checkInnerDistJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed INNER JOIN of specified and replicated table.
     *
     * @param depTab department table name.
     */
    private void checkInnerDistJoinWithReplicated(String depTab) {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOnOff = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);
            Result actIdxOffOff = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node);

            List<List<Object>> exp = doInnerJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result.", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result.", actIdxOffOff.values(), exp);
        });
    }

    /**
     * Checks distributed INNER JOIN of replicated and partitioned tables.
     */
    public void testMixedInnerDistJoinReplicatedPartitioned() {
        checkInnerDistJoinReplicatedWith(DEP_PART_TAB);
    }

    /**
     * Checks distributed INNER JOIN of replicated and specified table.
     *
     * @param depTab department table name.
     */
    private void checkInnerDistJoinReplicatedWith(String depTab) {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Address a INNER JOIN " + depTab + " d " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOnOff = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);
            Result actIdxOffOff = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node);

            List<List<Object>> exp = doInnerJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result.", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result.", actIdxOffOff.values(), exp);
        });
    }

    /**
     * Checks distributed LEFT JOIN of replicated and replicated tables.
     */
    public void testLeftDistributedJoinReplicatedReplicated() {
        checkLeftDistributedJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed LEFT JOIN of partitioned and replicated tables.
     */
    public void testLeftDistributedJoinPartitionedReplicated() {
        checkLeftDistributedJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed LEFT JOIN of specified and replicated table.
     *
     * @param depTab department table name.
     */
    private void checkLeftDistributedJoinWithReplicated(String depTab) {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOnOff = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);
            Result actIdxOffOff = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node);

            List<List<Object>> exp = doLeftJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result.", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result.", actIdxOffOff.values(), exp);
        });
    }

    /**
     * Checks distributed RIGHT JOIN of replicated and replicated tables.
     */
    public void testRightDistributedJoinReplicatedReplicated() {
        checkRightDistJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed RIGHT JOIN of partitioned and replicated tables.
     */
    public void testRightDistributedJoinPartitionedReplicated() {
        checkRightDistJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed RIGHT JOIN of specified and replicated table.
     *
     * @param depTab department table name.
     */
    public void checkRightDistJoinWithReplicated(String depTab) {
        testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depId")), node);
            Result actIdxOnOff = executeFrom(prepareDistJoin(String.format(qryTpl, "id", "depIdNoidx")), node);
            Result actIdxOffOn = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depId")), node);
            Result actIdxOffOff = executeFrom(prepareDistJoin(String.format(qryTpl, "idNoidx", "depIdNoidx")), node);

            List<List<Object>> exp = doRightJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result.", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result.", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result.", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result.", actIdxOffOff.values(), exp);
        });
    }

    /**
     * Check INNER JOIN with collocated data of replicated and partitioned tables.
     */
    public void testInnerJoinReplicatedPartitioned() {
        checkInnerJoinEmployeeDepartment(DEP_PART_TAB);
    }

    /**
     * Check INNER JOIN with collocated data of partitioned and replicated tables.
     */
    public void testMixedInnerJoinPartitionedReplicated() {
        checkInnerJoinDepartmentEmployee(DEP_PART_TAB);
    }

    /**
     * Check LEFT JOIN with collocated data of replicated and partitioned tables.
     */
    public void testLeftJoinReplicatedPartitioned() {
        checkLeftJoin(DEP_PART_TAB);
    }

    /**
     * Check RIGHT JOIN with collocated data of replicated and partitioned tables.
     */
    public void testRightJoinReplicatedPartitioned() {
        checkRightJoin(DEP_PART_TAB);
    }
}
