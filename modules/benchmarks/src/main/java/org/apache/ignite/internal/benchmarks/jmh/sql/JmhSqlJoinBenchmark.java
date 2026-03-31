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

package org.apache.ignite.internal.benchmarks.jmh.sql;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark JOIN queries.
 */
public class JmhSqlJoinBenchmark extends JmhSqlAbstractBenchmark {
    /** Count of entries in DEPT table. */
    protected static final int DEPT_CNT = 1_000;

    /** Count of entries in EMP table. */
    protected static final int EMP_CNT = 10_000;

    /** Counter for forcing query replanning in cold benchmark. */
    private final AtomicInteger planRnd = new AtomicInteger();

    /**
     * Initiate new tables.
     */
    @Override public void setup() {
        super.setup();

        executeSql("CREATE TABLE emp(empid INTEGER, deptid INTEGER, name VARCHAR, salary INTEGER, " +
            "PRIMARY KEY(empid, deptid)) WITH \"AFFINITY_KEY=deptid\"");
        executeSql("CREATE TABLE dept(deptid INTEGER, name VARCHAR, addr VARCHAR, PRIMARY KEY(deptid))");

        for (int i = 0; i < DEPT_CNT; i++) {
            executeSql("INSERT INTO dept(deptid, name, addr) VALUES (?, ?, ?)",
                i, "Department " + i, "Address " + i);
        }

        for (int i = 0; i < EMP_CNT; i++) {
            executeSql("INSERT INTO emp (empid, deptid, name, salary) VALUES (?, ?, ?, ?)",
                i, i % DEPT_CNT, "Employee " + i, i / BATCH_SIZE);
        }
    }

    /**
     * Colocated distributed join.
     */
    @Benchmark
    public void colocatedDistributedJoin(Blackhole bh) {
        int key = ThreadLocalRandom.current().nextInt(EMP_CNT / BATCH_SIZE);

        List<List<?>> res = executeSql("SELECT emp.name, dept.name FROM emp JOIN dept ON emp.deptid = dept.deptid " +
                "WHERE emp.salary = ?", key);

        bh.consume(res);
    }

    /**
     * LEFT JOIN with DISTINCT subquery - regression test for query splitter optimization.
     * DISTINCT is applied to the smaller table (dept), then LEFT JOINed with the larger table (emp),
     * and filtered by a condition on the right table.
     */
    @Benchmark
    public void leftJoinDistinctRegression(Blackhole bh) {
        List<List<?>> res = executeSql(
                "SELECT d.deptid, d.name, e.empid " +
                        "FROM (SELECT DISTINCT * FROM dept) d " +
                        "LEFT JOIN emp e ON d.deptid = e.deptid " +
                        "WHERE e.name = ?",
                "Employee 5"
        );


        bh.consume(res);
    }

    /**
     * LEFT JOIN without DISTINCT subquery - baseline for comparison.
     */
    @Benchmark
    public void leftJoinNoDistinctBaseline(Blackhole bh) {
        List<List<?>> res = executeSql(
                "SELECT e.empid, e.name, d.deptid " +
                        "FROM emp e " +
                        "LEFT JOIN dept d ON e.deptid = d.deptid " +
                        "WHERE d.name = ?",
                "Department 5"
        );


        bh.consume(res);
    }

    /**
     * LEFT JOIN with DISTINCT where query text is changed every run to bypass plan cache.
     * This tests the cold path where query optimization and planning happens every time.
     */
    @Benchmark
    public void leftJoinDistinctRegressionCold(Blackhole bh) {
        int rnd = planRnd.incrementAndGet();

        List<List<?>> res = executeSql(
                "/*rnd=" + rnd + "*/ " +
                        "SELECT d.deptid, d.name, e.empid " +
                        "FROM (SELECT DISTINCT * FROM dept) d " +
                        "LEFT JOIN emp e ON d.deptid = e.deptid " +
                        "WHERE e.name = ?",
                "Employee 5"
        );


        bh.consume(res);
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlJoinBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
