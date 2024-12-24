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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.cluster.DistributedConfigurationUtils.CONN_DISABLED_BY_ADMIN_ERR_MSG;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.toMetaStorageKey;
import static org.apache.ignite.internal.processors.query.calcite.jdbc.JdbcThinTransactionalSelfTest.URL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class JdbcConnectionEnabledPropertyTest extends GridCommonAbstractTest {
    /** */
    private static final String JDBC_CONN_ENABLED_PROP = "newJdbcConnectionsEnabled";


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** */
    @Test
    public void testConnectionEnabledProperty() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.isValid(3));
        }

        grid().context().distributedMetastorage().write(toMetaStorageKey(JDBC_CONN_ENABLED_PROP), false);

        assertThrows(log, () -> DriverManager.getConnection(URL), SQLException.class, CONN_DISABLED_BY_ADMIN_ERR_MSG);

        grid().context().distributedMetastorage().write(toMetaStorageKey(JDBC_CONN_ENABLED_PROP), true);

        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.isValid(3));
        }
    }
}
