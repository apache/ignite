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
import org.junit.Ignore;
import org.junit.Test;

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
    @Test
    public void testInnerDistributedJoinReplicatedReplicated() {
        checkInnerDistJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed INNER JOIN of partitioned and replicated tables.
     */
    @Test
    public void testInnerDistJoinPartitionedReplicated() {
        checkInnerDistJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed INNER JOIN of specified and replicated tables.
     *
     * @param depTab department table name.
     */
    private void checkInnerDistJoinWithReplicated(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            List<List<Object>> exp = doInnerJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Checks distributed INNER JOIN of replicated and partitioned tables.
     */
    @Test
    public void testMixedInnerDistJoinReplicatedPartitioned() {
        checkInnerDistJoinReplicatedWith(DEP_PART_TAB);
    }

    /**
     * Checks distributed INNER JOIN of replicated and specified tables.
     *
     * @param depTab department table name.
     */
    private void checkInnerDistJoinReplicatedWith(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Address a INNER JOIN " + depTab + " d " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            // tables order is insufficient for golden result for inner join.
            List<List<Object>> exp = doInnerJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Checks distributed LEFT JOIN of replicated and replicated tables.
     */
    @Test
    public void testLeftDistributedJoinReplicatedReplicated() {
        checkLeftDistributedJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed LEFT JOIN of partitioned and replicated tables.
     */
    @Test
    public void testLeftDistributedJoinPartitionedReplicated() {
        setExplain(true);
        checkLeftDistributedJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed LEFT JOIN of replicated and partitioned tables.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8732")
    @Test
    public void testLeftDistributedJoinReplicatedPartitioned() {
        checkLeftDistributedJoinReplicatedWith(DEP_PART_TAB);
    }

    /**
     * Checks distributed LEFT JOIN of specified and replicated tables.
     *
     * @param depTab department table name.
     */
    private void checkLeftDistributedJoinWithReplicated(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            List<List<Object>> exp = doLeftJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Checks distributed LEFT JOIN of specified and replicated tables.
     *
     * @param depTab department table name.
     */
    private void checkLeftDistributedJoinReplicatedWith(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Address a LEFT JOIN " + depTab + " d " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            List<List<Object>> exp = doLeftJoin(node.cache(ADDR_CACHE_NAME), node.cache(cacheName(depTab)),
                (addr, dep) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (addr, dep) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Checks distributed RIGHT JOIN of replicated and replicated tables.
     */
    @Test
    public void testRightDistributedJoinReplicatedReplicated() {
        checkRightDistributedJoinWithReplicated(DEP_TAB);
    }

    /**
     * Checks distributed RIGHT JOIN of partitioned and replicated tables.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8732")
    @Test
    public void testRightDistributedJoinPartitionedReplicated() {
        checkRightDistributedJoinWithReplicated(DEP_PART_TAB);
    }

    /**
     * Checks distributed RIGHT JOIN of replicated and partitioned tables.
     */
    @Test
    public void testRightDistributedJoinReplicatedPartitioned() {
        setExplain(true);
        checkRightDistributedJoinReplicatedWith(DEP_PART_TAB);
    }

    /**
     * Checks distributed RIGHT JOIN of replicated and specified tables.
     *
     * @param depTab department table name.
     */
    public void checkRightDistributedJoinWithReplicated(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM " + depTab + " d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            List<List<Object>> exp = doRightJoin(node.cache(cacheName(depTab)), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);

            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);

            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);

            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Checks distributed RIGHT JOIN of replicated and specified tables.
     *
     * @param depTab department table name.
     */
    public void checkRightDistributedJoinReplicatedWith(String depTab) {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Address a RIGHT JOIN " + depTab + " d " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depId"), node);
            Result actIdxOffOff = executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node);

            List<List<Object>> exp = doRightJoin(node.cache(ADDR_CACHE_NAME), node.cache(cacheName(depTab)),
                (addr, dep) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (addr, dep) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOff.values(), exp);
        }));
    }

    /**
     * Check INNER JOIN with collocated data of replicated and partitioned tables.
     */
    @Test
    public void testInnerJoinReplicatedPartitioned() {
        checkInnerJoinEmployeeDepartment(DEP_PART_TAB);
    }

    /**
     * Check INNER JOIN with collocated data of partitioned and replicated tables.
     */
    @Test
    public void testInnerJoinPartitionedReplicated() {
        checkInnerJoinDepartmentEmployee(DEP_PART_TAB);
    }

    /**
     * Check LEFT JOIN with collocated data of replicated and partitioned tables.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8732")
    @Test
    public void testLeftJoinReplicatedPartitioned() {
        checkLeftJoinEmployeeDepartment(DEP_PART_TAB);
    }

    /**
     * Check LEFT JOIN with collocated data of partitioned and replicated tables.
     */
    @Test
    public void testLeftJoinPartitionedReplicated() {
        checkLeftJoinDepartmentEmployee(DEP_PART_TAB);
    }

    /**
     * Check RIGHT JOIN with collocated data of replicated and partitioned tables.
     */
    @Test
    public void testRightJoinReplicatedPartitioned() {
        checkRightJoinEmployeeDepartment(DEP_PART_TAB);
    }

    /**
     * Check RIGHT JOIN with collocated data of partitioned and replicated tables.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8732")
    @Test
    public void testRightJoinPartitionedReplicated() {
        checkRightJoinDepartmentEmployee(DEP_PART_TAB);
    }
}
