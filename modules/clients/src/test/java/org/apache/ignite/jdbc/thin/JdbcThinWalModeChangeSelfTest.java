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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.WalModeChangeAbstractSelfTest;
import org.apache.ignite.internal.processors.query.QueryUtils;

/**
 * Tests for WAL mode change from within JDBC driver.
 */
public class JdbcThinWalModeChangeSelfTest extends WalModeChangeAbstractSelfTest {
    /**
     * Constructor.
     */
    public JdbcThinWalModeChangeSelfTest() {
        super(false, true);
    }

    /** {@inheritDoc} */
    @Override protected void createCache(Ignite node, CacheConfiguration ccfg) throws IgniteCheckedException {
        String template = ccfg.getCacheMode() == CacheMode.PARTITIONED ?
            QueryUtils.TEMPLATE_PARTITIONED : QueryUtils.TEMPLATE_REPLICATED;

        String cmd = "CREATE TABLE IF NOT EXISTS " + ccfg.getName() + " (k BIGINT PRIMARY KEY, v BIGINT) WITH \"" +
            "TEMPLATE=" + template + ", " +
            "CACHE_NAME=" + ccfg.getName() + ", " +
            "ATOMICITY=" + ccfg.getAtomicityMode() +
            (ccfg.getGroupName() != null ? ", CACHE_GROUP=" + ccfg.getGroupName() : "") +
            (ccfg.getDataRegionName() != null ? ", DATA_REGION=" + ccfg.getDataRegionName() : "") +
            "\"";

        execute(node, cmd);

        alignCacheTopologyVersion(node);
    }

    /** {@inheritDoc} */
    @Override protected void destroyCache(Ignite node, String cacheName) throws IgniteCheckedException {
        String cmd = "DROP TABLE IF EXISTS " + cacheName;

        execute(node, cmd);

        alignCacheTopologyVersion(node);
    }

    /** {@inheritDoc} */
    @Override protected boolean walEnable(Ignite node, String cacheName) {
        try {
            String cmd = "ALTER TABLE " + cacheName + " LOGGING";

            execute(node, cmd);

            return true;
        }
        catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof SQLException) {
                SQLException e0 = (SQLException)e.getCause();

                if (e0.getMessage().startsWith("Logging already enabled"))
                    return false;
            }

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean walDisable(Ignite node, String cacheName) {
        try {
            String cmd = "ALTER TABLE " + cacheName + " NOLOGGING";

            execute(node, cmd);

            return true;
        }
        catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof SQLException) {
                SQLException e0 = (SQLException)e.getCause();

                if (e0.getMessage().startsWith("Logging already disabled"))
                    return false;
            }

            throw e;
        }
    }

    /**
     * Execute single command.
     *
     * @param node Node.
     * @param cmd Command.
     */
    private static void execute(Ignite node, String cmd) {
        try (Connection conn = connect(node)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(cmd);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get connection for node.
     *
     * @param node Node.
     * @return Connection.
     */
    private static Connection connect(Ignite node) throws Exception {
        IgniteKernal node0 = (IgniteKernal)node;

        int port = node0.context().sqlListener().port();

        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + port);
    }
}
