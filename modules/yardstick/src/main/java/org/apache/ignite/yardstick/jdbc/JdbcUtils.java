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

import java.util.UUID;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Useful methods for JDBC benchmarks.
 */
public class JdbcUtils {
    /**
     * Common method to fill test stand with data.
     *
     * @param cfg Benchmark configuration.
     * @param ignite Ignite node.
     * @param range Data key range.
     * @param atomicMode Cache atomic mode.
     * @param columns Columns.
     */
    public static void fillData(BenchmarkConfiguration cfg,
        IgniteEx ignite,
        long range,
        CacheAtomicityMode atomicMode,
        String... columns) {
        IgniteSemaphore sem = ignite.semaphore("jdbc-setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create table...");

                StringBuilder qry = new StringBuilder("CREATE TABLE test_long (id LONG PRIMARY KEY");

                if (F.isEmpty(columns))
                    qry.append(", val LONG)");
                else {
                    for (String col : columns)
                        qry.append(", ").append(col);
                }

                qry.append(") WITH \"wrap_value=true");

                if (atomicMode != null)
                    qry.append(", atomicity=").append(atomicMode.name());

                qry.append("\";");

                String qryStr = qry.toString();

                println(cfg, "Creating table with schema: " + qryStr);

                GridQueryProcessor qProc = ignite.context().query();

                qProc.querySqlFields(
                    new SqlFieldsQuery(qryStr), true);

                println(cfg, "Populate data...");

                if (F.isEmpty(columns))
                    qry = new StringBuilder("INSERT INTO test_long VALUES (?, ?)");
                else {
                    qry = new StringBuilder("INSERT INTO test_long VALUES (?");

                    for (int i = 0; i < columns.length; ++i)
                        qry.append(", ?");

                    qry.append(")");
                }
                for (long id = 1; id <= range; ++id) {
                    if (id == 1)
                        BenchmarkUtils.println("+++ " + qry.toString());

                    qProc.querySqlFields(
                        new SqlFieldsQuery(qry.toString())
                            .setArgs(fillArgs(id, columns)), true);

                    if (id % 10000 == 0)
                        println(cfg, "Populate " + id);
                }

                if (F.isEmpty(columns))
                    qProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX val_idx ON test_long (val)"), true);

                println(cfg, "Finished populating data");
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

    /**
     * @param id ID.
     * @param cols Columns array.
     * @return Row.
     */
    private static Object[] fillArgs(long id, String... cols) {
        if (F.isEmpty(cols))
            return new Object[] {id, id + 1};
        else {
            Object[] res = new Object[cols.length + 1];
            res[0] = id;

            for (int i = 0; i < cols.length; ++i) {
                if (cols[i].endsWith("LONG"))
                    res[i + 1] = id + 1;
                else if (cols[i].endsWith("CHAR"))
                    res[i + 1] = "'val_" + i + "_" + UUID.randomUUID().toString() + '\'';
            }

            return res;
        }
    }
}
