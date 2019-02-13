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

package org.apache.ignite.yardstick.jdbc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Native sql benchmark that performs select operations.
 */
public class NativeSqlJoinQueryRangeBenchmark extends IgniteAbstractBenchmark {
    /** Ignite query processor. */
    GridQueryProcessor qry;

    /**
     * Benchmarked action that performs selects and validates results.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long expRsSize;

        SqlFieldsQuery qry;

        if (args.sqlRange() == 1) {
            qry = new SqlFieldsQuery("SELECT * FROM person p join organization o on p.orgId=o.id WHERE p.id = ? " +
                "order by p.id");

            qry.setArgs(ThreadLocalRandom.current().nextLong(args.range()));

            expRsSize = 1;
        }
        else if (args.sqlRange() <= 0) {
            qry = new SqlFieldsQuery("SELECT * FROM person p join organization o on p.orgId=o.id order by p.id LIMIT 1000");

            expRsSize = 1000;
        }
        else {
            qry = new SqlFieldsQuery("SELECT * FROM person p join organization o on p.orgId=o.id WHERE p.id BETWEEN ? AND ?" +
                "order by p.id");

            long id = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange());
            long maxId = id + args.sqlRange() - 1;

            qry.setArgs(id, maxId);

            expRsSize = args.sqlRange();
        }

        long rsSize = 0;

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
            .querySqlFields(qry, false)) {

            for (List<?> r : cursor)
                rsSize++;
        }

        if (rsSize != expRsSize) {
            throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize
                + ", qry=" + qry + ']');
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        IgniteSemaphore sem = ignite().semaphore("sql-setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                qry = ((IgniteEx)ignite()).context().query();

                StringBuilder withExpr = new StringBuilder(" WITH \"AFFINITY_KEY=orgId,");

                if (args.atomicMode() != null)
                    withExpr.append("atomicity=").append(args.atomicMode().name()).append(",");

                if (args.partitionedCachesNumber() == 1)
                    withExpr.append("template=replicated");
                else
                    withExpr.append("template=partitioned");

                withExpr.append("\"");

                qry.querySqlFields(
                    new SqlFieldsQuery("CREATE TABLE person (id long, orgId long, name varchar, PRIMARY KEY (id, orgId))" + withExpr), true);

                withExpr = new StringBuilder(" WITH \"");

                if (args.atomicMode() != null)
                    withExpr.append("atomicity=").append(args.atomicMode().name()).append(",");

                withExpr.append("template=partitioned");

                withExpr.append("\"");

                qry.querySqlFields(
                    new SqlFieldsQuery("CREATE TABLE organization (id long primary key, name varchar)" + withExpr), true);

                for (long k = 0; k <= args.range(); ++k) {
                    qry.querySqlFields(new SqlFieldsQuery("insert into person (id, orgId, name) values (?, ?, ?)")
                        .setArgs(k, k / 10, "person " + k), true).getAll();

                    if (k % 10 == 0) {
                        qry.querySqlFields(new SqlFieldsQuery("insert into organization (id, name) values (?, ?)")
                            .setArgs(k / 10, "organization " + k / 10), true).getAll();
                    }

                    if (k % 10000 == 0)
                        println(cfg, "Populate " + k);
                }
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }
    }
}
