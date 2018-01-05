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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.WalModeChangeAbstractSelfTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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

    /**
     * Test WAL mode change.
     *
     * @throws Exception If failed.
     */
    public void testWalModeChange() throws Exception {
        IgniteEx node = grid();

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE test (id BIGINT PRIMARY KEY, val VARCHAR) WITH \"CACHE_NAME=cache\"");

                assert node.cluster().isWalEnabled("cache");

                stmt.executeUpdate("ALTER TABLE test NOLOGGING");

                assert !node.cluster().isWalEnabled("cache");

                stmt.executeUpdate("ALTER TABLE test LOGGING");

                assert node.cluster().isWalEnabled("cache");
            }
        }
    }
}
