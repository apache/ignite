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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Tests for WAL mode change from within JDBC driver.
 */
public class JdbcThinWalModeChangeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteWorkFiles();

        Ignite node = startGrid();

        node.active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dataStorageCfg =
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        return cfg;
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

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
