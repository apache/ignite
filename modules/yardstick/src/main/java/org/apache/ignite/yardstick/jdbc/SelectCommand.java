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
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Creates SELECT queries and arguments for those queries. Uses data model defined in {@link
 * JdbcUtils#fillData(BenchmarkConfiguration, IgniteEx, long, CacheAtomicityMode)}
 */
public enum SelectCommand {
    /**
     * Creates SELECT query that has primary key field in the WHERE clause.
     */
    BY_PRIMARY_KEY {
        @Override public String selectOne() {
            return "SELECT id, val FROM test_long WHERE id = ?;";
        }

        @Override public String selectRange() {
            return "SELECT id, val FROM test_long WHERE id BETWEEN ? AND ?;";
        }

        @Override public long fieldByPK(long pk) {
            // This command uses PK value itself.
            return pk;
        }
    },

    /**
     * Creates SELECT query that has value field in the WHERE clause.
     */
    BY_VALUE {
        @Override public String selectOne() {
            return "SELECT id, val FROM test_long WHERE val = ?;";
        }

        @Override public String selectRange() {
            return "SELECT id, val FROM test_long WHERE val BETWEEN ? AND ?;";
        }

        @Override public long fieldByPK(long pk) {
            // data model defines that value is generated as id (PK) field plus one.
            return pk + 1;
        }
    };

    /**
     * Create SELECT query that returns one row and has one parameter - field value for WHERE clause.
     */
    public abstract String selectOne();

    /**
     * Create SELECT query that returns number of rows. This query has 2 parameters - min and max value for BETWEEN
     * operator from WHERE clause.
     */
    public abstract String selectRange();

    /**
     * Gets field value by primary key. Field can be PK itself or val field depending on type of this Select.
     * Implementation of this method is based on how {@link JdbcUtils#fillData(BenchmarkConfiguration, IgniteEx, long,
     * CacheAtomicityMode)} generates data.
     *
     * @param pk primary key for what to compute field value.
     * @return field value.
     */
    public abstract long fieldByPK(long pk);
}
