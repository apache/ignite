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

package org.apache.ignite.yardstick.cache.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.yardstick.*;
import org.yardstickframework.*;

import java.sql.*;

/**
 * Abstract class for Ignite benchmarks which use cache.
 */
public abstract class IgniteJdbcStoreAbstractBenchmark extends IgniteAbstractBenchmark {
    /** Cache. */
    protected IgniteCache<Object, Object> cache;

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return GridCache Cache to use.
     */
    protected abstract IgniteCache<Object, Object> cache();

    /**
     * Each benchmark must determine key range (from {@code 0} to this number) for fill.
     *
     * @return GridCache Cache to use.
     */
    protected abstract int fillRange();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();

        CacheConfiguration cc = cache.getConfiguration(CacheConfiguration.class);

        CacheAbstractJdbcStore store = (CacheAbstractJdbcStore)cc.getCacheStoreFactory().create();

        try (Connection conn = store.getDataSource().getConnection()) {
            conn.setAutoCommit(false);

            Statement stmt = conn.createStatement();

            try {
                stmt.executeUpdate("delete from SAMPLE");
            }
            catch (SQLException ignore) {
                // No-op.
            }

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS SAMPLE (id integer not null, value integer, PRIMARY KEY(id))");

            conn.commit();

            U.closeQuiet(stmt);

            PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO SAMPLE(id, value) VALUES (?, ?)");

            for (int i = 0; i < fillRange(); i++) {
                orgStmt.setInt(1, i);
                orgStmt.setInt(2, i);

                orgStmt.addBatch();
            }

            orgStmt.executeBatch();

            conn.commit();

            U.closeQuiet(stmt);
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        CacheConfiguration cc = cache.getConfiguration(CacheConfiguration.class);

        CacheAbstractJdbcStore store = (CacheAbstractJdbcStore)cc.getCacheStoreFactory().create();

        try (Connection conn = store.getDataSource().getConnection()) {
            conn.setAutoCommit(true);

            Statement stmt = conn.createStatement();

            try {
                stmt.executeUpdate("delete from SAMPLE");
            }
            catch (SQLException ignore) {
                // No-op.
            }

            U.closeQuiet(stmt);
        }
    }
}
