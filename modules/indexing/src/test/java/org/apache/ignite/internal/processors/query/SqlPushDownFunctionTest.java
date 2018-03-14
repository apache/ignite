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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for schemas.
 */
public class SqlPushDownFunctionTest extends GridCommonAbstractTest {
    /** Node. */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = (IgniteEx)startGrid();

        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSplitJoinWithSubqueryUnion() throws Exception {
        sql("CREATE TABLE Person (id INTEGER PRIMARY KEY, company_id INTEGER, salary DECIMAL)");
        sql("CREATE TABLE Company (id INTEGER PRIMARY KEY, name VARCHAR)");
        sql("CREATE TABLE Address (id INTEGER PRIMARY KEY, person_id INTEGER, city VARCHAR)");

        sql("INSERT INTO Person (id, company_id, salary) VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300)");
        sql("INSERT INTO Company (id, name) VALUES (1, 'n1'), (2, 'n2'), (3, 'n3')");
        sql("INSERT INTO Address (id, person_id, city) VALUES (1, 1, 'san francisco'), (2, 2, 'paris'), (3, 3, 'new york')");


        sql("SELECT a.id FROM \n" +
            "(" +
            "SELECT p1.id, p1.company_id FROM Person p1 WHERE p1.id = 1 \n" +
            "UNION " +
            "SELECT p2.id, p2.company_id FROM Person p2 WHERE p2.id = 2" +
            ")  p\n"
            + "LEFT JOIN Company c ON p.company_id = c.id\n"
            + "LEFT JOIN Address a ON a.person_id = p.id;"
        );

    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    private List<List<?>> sql(String sql) {
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }
}
