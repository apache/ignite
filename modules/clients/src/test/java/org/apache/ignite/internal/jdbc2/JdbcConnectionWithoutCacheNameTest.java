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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test connection without cache name.
 */
public class JdbcConnectionWithoutCacheNameTest extends GridCommonAbstractTest {
    /** Ignite config path for JDBC v2 client. */
    private static final String CFG_PATH = "modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlSchema("\"default\"");

        cfg.setCacheConfiguration(cache);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid("srv");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithoutCache() throws Exception {
        try (Connection c = DriverManager.getConnection(CFG_URL_PREFIX + CFG_PATH)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");
                stmt.execute("INSERT INTO TEST VALUES(1, '1')");
                stmt.execute("DROP TABLE TEST ");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSchemaParameter() throws Exception {
        try (Connection c = DriverManager.getConnection(CFG_URL_PREFIX + "schema=\"default\"@" + CFG_PATH)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");
                stmt.execute("INSERT INTO TEST VALUES(1, '1')");
            }
        }

        try (Connection c = DriverManager.getConnection(CFG_URL_PREFIX + CFG_PATH)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("DROP TABLE \"default\".TEST ");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailOnMultipleStatements() throws Exception {
        try (Connection c = DriverManager.getConnection(CFG_URL_PREFIX + CFG_PATH)) {
            try (Statement stmt = c.createStatement()) {
                stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

                GridTestUtils.assertThrows(log, () -> {
                    stmt.execute("INSERT INTO TEST VALUES(0, '0'); " +
                        "INSERT INTO TEST VALUES(1, '1');");

                    return null;
                }, SQLException.class, "Multiple statements queries are not supported.");
            }
        }
    }
}
