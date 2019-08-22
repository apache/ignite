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

import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.yardstickframework.BenchmarkConfiguration;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Useful methods for JDBC benchmarks.
 */
public class JdbcUtils {
    /**
     * Common method to fill test stand with data.
     * @param cfg Benchmark configuration.
     * @param ignite Ignite node.
     * @param range Data key range.
     */
    static void fillData(BenchmarkConfiguration cfg,  IgniteEx ignite, long range) {
        println(cfg, "Create table...");

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE test_long (id long primary key, val long)"), true);

        println(cfg, "Populate data...");

        for (long l = 1; l <= range; ++l) {
            ignite.context().query().querySqlFields(
                new SqlFieldsQuery("insert into test_long (id, val) values (?, ?)")
                    .setArgs(l, l + 1), true);

            if (l % 10000 == 0)
                println(cfg, "Populate " + l);
        }

        println(cfg, "Finished populating data");
    }

    /**
     * Common method to fill test stand with data.
     *
     * @param cfg Benchmark configuration.
     * @param ignite Ignite instance.
     * @param tblName Table name for fill and creation.
     * @param range Data key range.
     * @param atomicMode Cache creation atomicity mode.
     */
    public static void fillTableWithIdx(BenchmarkConfiguration cfg,
                                        IgniteEx ignite,
                                        String tblName,
                                        long range,
                                        CacheAtomicityMode atomicMode) {
        IgniteSemaphore sem = ignite.semaphore("sql-setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create table...");

                StringBuilder qry = new StringBuilder(String.format("CREATE TABLE %s (", tblName) +
                    "id LONG PRIMARY KEY, " +
                    "DEC_COL DECIMAL, " +
                    "DATE_COL DATE, " +
                    "BIG_INT_COL BIGINT" +
                    ") WITH \"wrap_value=true");

                if (atomicMode != null)
                    qry.append(", atomicity=").append(atomicMode.name());

                qry.append("\";");

                String qryStr = qry.toString();

                println(cfg, "Creating table with schema: " + qryStr);

                GridQueryProcessor qProc = ignite.context().query();

                qProc.querySqlFields(new SqlFieldsQuery(qryStr), true);

                println(cfg, "Populate data...");

                for (long l = 1; l < range; ++l) {
                    qProc.querySqlFields(
                        new SqlFieldsQuery(String.format("INSERT INTO %s VALUES (?, ?, ?, ?)", tblName))
                            .setArgs(l, new BigDecimal(l + 1), LocalDate.ofEpochDay(l), l + 2), true);

                    if (l % 10000 == 0)
                        println(cfg, "Populate " + l);
                }

                qProc.querySqlFields(new SqlFieldsQuery(String.format("CREATE INDEX idx1 ON %s (DEC_COL, " +
                    "DATE_COL) inline_size 16", tblName)), true);

                qProc.querySqlFields(new SqlFieldsQuery(String.format("CREATE INDEX idx2 ON %s (DATE_COL, " +
                    "BIG_INT_COL) inline_size 16", tblName)), true);

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
}
