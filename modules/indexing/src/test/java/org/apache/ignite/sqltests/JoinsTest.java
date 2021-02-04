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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/** */
public class JoinsTest extends AbstractIndexingCommonTest {
    /** Client node instance. */
    protected static IgniteEx crd;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        crd = startGrid();
    }

    /** */
    @Test
    public void joins() {
        execute(new SqlFieldsQuery(
            "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR) WITH \"affinity_key=id\";"));

//        execute(new SqlFieldsQuery(
//            "CREATE TABLE B (ID INT PRIMARY KEY, TITLE VARCHAR);"));

        for (int i = 0; i < 10; i++)
            execute(new SqlFieldsQuery(
                "INSERT INTO A (id, title) VALUES (" + i + ", 'Val" + i + "');"));

        Object result = execute(new SqlFieldsQuery(
            "SELECT a1.* FROM A a1 INNER JOIN A a2 on a1.ID = 1;")
            .setDistributedJoins(true)).values();
    }

    /** */
    @Test
    public void explain() {
        execute(new SqlFieldsQuery(
            "CREATE TABLE TEST (ID INT PRIMARY KEY, TITLE VARCHAR);"));

        execute(new SqlFieldsQuery(
            "CREATE INDEX TEST_TITLE_ASC_IDX ON TEST(TITLE);"));

        for (int i = 0; i < 10; i++)
            execute(new SqlFieldsQuery(
                "INSERT INTO TEST (id, title) VALUES (" + i + ", 'Val" + i + "');"));

        Object result = execute(new SqlFieldsQuery(
            "EXPLAIN SELECT _KEY FROM TEST WHERE UPPER(TITLE) LIKE '%A%';")).values();

        System.out.println(result);
    }

    /** */
    @Test
    public void index() {
        execute(new SqlFieldsQuery(
            "CREATE TABLE dept (deptno LONG, dname VARCHAR, CONSTRAINT pk_dept PRIMARY KEY (deptno));"));

        execute(new SqlFieldsQuery(
            "CREATE TABLE emp (deptno LONG, empno LONG, ename VARCHAR, CONSTRAINT pk_emp PRIMARY KEY (empno));"));

        execute(new SqlFieldsQuery(
            "INSERT INTO dept (deptno, dname) VALUES (10, 'ACCOUNTING');"));

        execute(new SqlFieldsQuery(
            "INSERT INTO emp (empno, ename, deptno) VALUES(7839, 'KING', 10);"));

        List<List<?>> result = execute(new SqlFieldsQuery(
                "        SELECT d.deptno FROM emp e\n" +
                "        LEFT JOIN dept d\n" +
                "        ON e.deptno = d.deptno;\n"
        ).setDistributedJoins(false)).values();

//        List<List<?>> result = execute(new SqlFieldsQuery(
//            "        SELECT d.deptno FROM emp e\n" +
//                "        INNER JOIN dept d\n" +
//                "        ON e.deptno = d.deptno AND d.deptno = 10;\n"
//        ).setDistributedJoins(false)).values();
//
//        result = execute(new SqlFieldsQuery(
//            "        SELECT d.deptno FROM emp e\n" +
//                "        INNER JOIN dept d\n" +
//                "        ON e.deptno = d.deptno AND e.deptno = 10;\n"
//        ).setDistributedJoins(false)).values();

        System.out.println(result);
    }

    /** */
    @Test
    public void test() {
        execute(new SqlFieldsQuery(
            "CREATE TABLE dept (deptno LONG, dname VARCHAR, CONSTRAINT pk_dept PRIMARY KEY (deptno)) WITH \"affinity_key=deptno\";"));

        execute(new SqlFieldsQuery(
            "CREATE TABLE emp (deptno LONG, empno LONG, ename VARCHAR, CONSTRAINT pk_emp PRIMARY KEY (empno, deptno)) WITH \"affinity_key=deptno\";"));

        execute(new SqlFieldsQuery(
            "INSERT INTO dept (deptno, dname) VALUES (10, 'ACCOUNTING');"));

        execute(new SqlFieldsQuery(
            "INSERT INTO emp (empno, ename, deptno) VALUES(7839, 'KING', 10);"));

        List<List<?>> result = execute(new SqlFieldsQuery(
                "        SELECT d.deptno FROM emp e\n" +
                "        INNER JOIN dept d\n" +
                "        ON e.deptno = d.deptno AND d.deptno = 10;\n"
        ).setDistributedJoins(false)).values();

        System.out.println(result);

//        List<List<?>> result2 = execute(new SqlFieldsQuery(
//                "        SELECT d.deptno FROM emp e\n" +
//                "        INNER JOIN dept d\n" +
//                "        ON e.deptno = d.deptno;\n"
//        )).values();
//
//        System.out.println(result2);

//        result = execute(new SqlFieldsQuery(
//                "        SELECT d.deptno, d.dname, e.empno,\n" +
//                "            e.ename FROM emp e\n" +
//                "        INNER JOIN dept d\n" +
//                "        ON ( e.deptno = d.deptno )\n" +
//                "        WHERE EXISTS (SELECT 1) AND e.deptno IN ( 10, 20, 30 );"
//        )).values();
//
//        System.out.println(result);
    }

    /**
     * Performs query from client node.
     *
     * @param qry query.
     * @return number of changed rows.
     */
    protected BaseSqlTest.Result execute(SqlFieldsQuery qry) {
        return executeFrom(qry, crd);
    }

    /**
     * Execute query from node.
     *
     * @param qry query.
     * @param node node to use to perform query.
     * @return Result of query.
     */
    protected final BaseSqlTest.Result executeFrom(SqlFieldsQuery qry, Ignite node) {
        FieldsQueryCursor<List<?>> cursor = ((IgniteEx)node).context().query().querySqlFields(qry, false);

        return BaseSqlTest.Result.fromCursor(cursor);
    }
}
