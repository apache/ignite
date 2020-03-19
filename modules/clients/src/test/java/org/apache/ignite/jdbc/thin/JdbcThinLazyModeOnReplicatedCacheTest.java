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
import java.sql.Statement;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test lazy query on replicated cache to check table unlock logic.
 */
public class JdbcThinLazyModeOnReplicatedCacheTest extends GridCommonAbstractTest {
    /** Rows count. */
    private static final int ROWS = 10;

    /** Client connection port. */
    private int cliPort = ClientConnectorConfiguration.DFLT_PORT;

    /** JDBC connection. */
    private Connection conn;

    /** JDBC statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setPort(cliPort++));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?lazy=true");

        stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE TEST (ID LONG PRIMARY KEY, VAL VARCHAR) " +
            "WITH \"TEMPLATE=REPLICATED\"");

        for (int i = 0; i < ROWS; ++i)
            stmt.executeUpdate("INSERT INTO TEST (ID, VAL) VALUES (" + i + ", 'val_" + i + "')");
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
    public void test() throws Exception {
        stmt.setFetchSize(1);

        try(ResultSet rs = stmt.executeQuery("SELECT * FROM TEST")) {
            int cnt = 0;

            while (rs.next())
                cnt++;

            assertEquals(ROWS, cnt);
        }

    }
}
