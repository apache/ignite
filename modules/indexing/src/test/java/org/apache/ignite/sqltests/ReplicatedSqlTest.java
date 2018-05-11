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

    public void testInnerDistJoin1() {
        checkInnerDistJoin1(DEP_TAB);
    }

    public void testMixedInnerDistJoin1() {
        checkInnerDistJoin1(DEP_PART_TAB);
    }

    private void checkInnerDistJoin1(String depTab) {
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

    public void testInnerDistJoin2() {
        checkInnerDistJoin2(DEP_TAB);
    }

    public void testMixedInnerDistJoin2() {
        checkInnerDistJoin2(DEP_PART_TAB);
    }

    private void checkInnerDistJoin2(String depTab) {
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

    public void testLeftDistJoin() {
        checkLeftDistJoin(DEP_TAB);
    }

    public void testMixedLeftDistJoin() {
        checkLeftDistJoin(DEP_PART_TAB);
    }

    private void checkLeftDistJoin(String depTab) {
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

    public void testRightDistJoin() {
        checkRightDistJoin(DEP_TAB);
    }

    public void testMixedRightDistJoin() {
        checkRightDistJoin(DEP_PART_TAB);
    }

    public void checkRightDistJoin(String depTab) {
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

    public void testMixedInnerJoin1() {
        checkInnerJoin1(DEP_PART_TAB);
    }

    public void testMixedInnerJoin2() {
        checkInnerJoin1(DEP_PART_TAB);
    }

    public void testMixedLeftJoin() {
        checkLeftJoin(DEP_PART_TAB);
    }

    public void testMixedRightJoin() {
        checkRightJoin(DEP_PART_TAB);
    }
}
