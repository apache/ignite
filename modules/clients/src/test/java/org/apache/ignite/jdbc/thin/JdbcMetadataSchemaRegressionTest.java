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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.hamcrest.CoreMatchers.hasItem;

/**
 * Regression test. Verifies that jdbc metadata contains all schemas, if driver connected to client node.
 */
public class JdbcMetadataSchemaRegressionTest extends GridCommonAbstractTest {
    /** Server node */
    private static Ignite server;

    /** Client node */
    private static Ignite client;

    private static final String PRESTARTED_CACHE_NAME = "PrestartedCache";



    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // server should contain a cache configuration, that client doesn't have.
        IgniteConfiguration srvConfig = getConfiguration("server")
            .setCacheConfiguration(new CacheConfiguration<>(PRESTARTED_CACHE_NAME)
                .setIndexedTypes(Long.class, UUID.class));

        IgniteConfiguration clConfig = getConfiguration("client").setClientMode(true);

        server = startGrid(srvConfig.getIgniteInstanceName(), optimize(srvConfig), null);
        client = startGrid(clConfig.getIgniteInstanceName(), optimize(clConfig), null);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    public void testServerPrestartedSchemaOnClientNode() throws SQLException{
        List<String> schemas = fetchSchemasFrom(client);

        String expectedSchema = PRESTARTED_CACHE_NAME;

        Assert.assertThat("Existing schema " + expectedSchema + " not found in all jdbc metadata schemas." ,
            schemas, hasItem(expectedSchema));

    }

    public void testServerPrestartedSchemaOnServerNode() throws SQLException{
        List<String> schemas = fetchSchemasFrom(server);

        String expectedSchema = PRESTARTED_CACHE_NAME;

        Assert.assertThat("Existing schema " + expectedSchema + " not found in all jdbc metadata schemas.",
            schemas, hasItem(expectedSchema));

    }

    public void testPublicSchema() throws SQLException {
        List<String> schemas = fetchSchemasFrom(client);

        Assert.assertThat("PUBLIC schema does NOT exist.", schemas, hasItem("PUBLIC"));
    }

    private List<String> fetchSchemasFrom(Ignite node) throws SQLException {
        int clientPort = GridTestUtils.jdbcPortOf(node);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + clientPort + "/")) {
            try (ResultSet rs = conn.getMetaData().getSchemas()) {
                List<String> schemas = new ArrayList<>();

                while(rs.next())
                    schemas.add(rs.getString(1));

                return schemas;
            }
        }
    }

}
