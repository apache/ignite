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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
public class JdbcThinSelectAfterAlterTable extends GridCommonAbstractTest {
    /** Client connection port. */
    private int cliPort = ClientConnectorConfiguration.DFLT_PORT;

    /** JDBC connection. */
    private Connection conn;

    /** JDBC statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(cliPort++));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id))");
        stmt.executeUpdate("INSERT INTO person (id, name, city_id) values (1, 'name_1', 11)");

        stmt.executeQuery("select * from person");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stmt.close();
        conn.close();

        // Destroy all SQL caches after test.
        for (String cacheName : grid(0).cacheNames()) {
            DynamicCacheDescriptor cacheDesc = grid(0).context().cache().cacheDescriptor(cacheName);

            if (cacheDesc != null && cacheDesc.sql())
                grid(0).destroyCache0(cacheName, true);
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectAfterAlterTableSingleNode() throws Exception {
        stmt.executeUpdate("alter table person add age int");

        checkNewColumn(stmt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectAfterAlterTableMultiNode() throws Exception {
        try (Connection conn2 = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:"
            + (ClientConnectorConfiguration.DFLT_PORT + 1))) {

            try (Statement stmt2 = conn2.createStatement()) {
                stmt2.executeUpdate("alter table person add age int");
            }
        }

        checkNewColumn(stmt);
    }

    /**
     * @param stmt Statement to check new column.
     * @throws SQLException If failed.
     */
    public void checkNewColumn(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select * from person");

        ResultSetMetaData meta = rs.getMetaData();

        assertEquals(4, meta.getColumnCount());

        boolean newColExists = false;

        for (int i = 1; i <= meta.getColumnCount(); ++i) {
            if ("age".equalsIgnoreCase(meta.getColumnName(i))) {
                newColExists = true;

                break;
            }
        }

        assertTrue(newColExists);
    }
}
