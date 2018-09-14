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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

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
    public static void fillData(BenchmarkConfiguration cfg,  IgniteEx ignite, long range, CacheAtomicityMode atomicMode) {
        println(cfg, "Create table...");

        String withExpr = atomicMode != null ? " WITH \"atomicity=" + atomicMode.name() + "\";" : ";";

        String qry = "CREATE TABLE test_long (id long primary key, val long)" + withExpr;

        println(cfg, "Creating table with schema: " + qry);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery(qry), true);

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
}
