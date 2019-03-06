/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.yardstick.cache.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.jdbc.CacheAbstractJdbcStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Abstract class for Ignite benchmarks which use cache.
 */
public abstract class IgniteJdbcStoreAbstractBenchmark extends IgniteAbstractBenchmark {
    /** Cache. */
    protected IgniteCache<Object, Object> cache;

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return IgniteCache Cache to use.
     */
    protected abstract IgniteCache<Object, Object> cache();

    /**
     * Each benchmark must determine key range (from {@code 0} to this number) for fill.
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

            try {
                stmt.executeUpdate("CREATE TABLE SAMPLE (id integer not null, value integer, PRIMARY KEY(id))");
            }
            catch (SQLException ignore) {
                // No-op.
            }

            conn.commit();

            U.closeQuiet(stmt);

            PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO SAMPLE(id, value) VALUES (?, ?)");

            int i;

            for (i = 1; i <= fillRange(); i++) {
                orgStmt.setInt(1, i);
                orgStmt.setInt(2, i);

                orgStmt.addBatch();

                if (i % 1000 == 0)
                    orgStmt.executeBatch();
            }

            if (i % 1000 != 0)
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

        super.tearDown();
    }
}