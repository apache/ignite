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

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.junit.Test;

/**
 * Integration tests for recursive common table expressions.
 */
public class RecursiveCteIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testEmployeeHierarchy() {
        createEmployeeTable();

        assertQuery("WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy ORDER BY depth, id")
            .returns(1, null, "CEO", 0)
            .returns(2, 1, "Manager", 1)
            .returns(4, 1, "Accountant", 1)
            .returns(3, 2, "Developer", 2)
            .check();
    }

    /** */
    @Test
    public void testRecursionStopsWhenDeltaIsEmpty() {
        sql("CREATE TABLE employee (id INT PRIMARY KEY, manager_id INT, name VARCHAR)");
        sql("INSERT INTO employee VALUES (1, NULL, 'CEO')");

        assertQuery("WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy")
            .returns(1, null, "CEO", 0)
            .check();
    }

    /** */
    @Test
    public void testExplainRecursiveCte() {
        createEmployeeTable();

        String qry = "WITH RECURSIVE employee_hierarchy (id, manager_id, name, depth) AS (" +
            "SELECT id, manager_id, name, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, e.name, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
            ") " +
            "SELECT id, manager_id, name, depth FROM employee_hierarchy";

        String plan = (String)sql("EXPLAIN PLAN FOR " + qry).get(0).get(0);

        info("PVD:: " + plan);

        assertTrue(plan, plan.contains("IgniteRepeatUnion"));
        assertTrue(plan, plan.contains("IgniteRecursiveTableSpool"));
    }

    /** */
    private void createEmployeeTable() {
        sql("CREATE TABLE employee (id INT PRIMARY KEY, manager_id INT, name VARCHAR)");

        sql("INSERT INTO employee VALUES " +
            "(1, NULL, 'CEO'), " +
            "(2, 1, 'Manager'), " +
            "(3, 2, 'Developer'), " +
            "(4, 1, 'Accountant')");
    }
}
