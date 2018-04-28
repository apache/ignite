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
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Checks that schema is verified at the moment of jdbc driver connection.
 */
public class JdbcThinConnectionSchemaTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "ClientNode";

    /** */
    private static final String SERVER_NODE_NAME = "ServerNode";

    /** Cache name for cache that will be started on cluster startup. Is defined in IgniteConfiguration. */
    private static final String PRESTARTED_CACHE = "PrestartedCache";

    /** Name of dynamic cache, that doesn't have indexed types. */
    private static final String CACHE_WITHOUT_TABLES_NAME = "CacheWithoutTables";

    /** Cache created using Ignite server instance. */
    private static final String SERVER_CACHE = "ServerNodeCache";

    /** Name of the cache, that is created using Ignite cache instance, NOT the cache on client. */
    private static final String CLIENT_CACHE = "ClientNodeCache";

    /** */
    private IgniteEx clientNode;

    /** */
    private IgniteEx serverNode;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Node that we don't connect to contains prestarted cache with table.
        startGrid(SERVER_NODE_NAME, withPrestartedCache(optimize(getConfiguration(SERVER_NODE_NAME))), null);

        startGrid(CLIENT_NODE_NAME, optimize(getConfiguration(CLIENT_NODE_NAME).setClientMode(true)), null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
        super.afterTestsStopped();
    }

    /**
     * Creates new cache configuration with a table.
     */
    private CacheConfiguration<Long, UUID> newCacheCfg(String name) {
        CacheConfiguration<Long, UUID> ccfg = new CacheConfiguration<>(name);

        ccfg.setIndexedTypes(Long.class, UUID.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        serverNode = grid(SERVER_NODE_NAME);
        clientNode = grid(CLIENT_NODE_NAME);

        serverNode.createCache(newCacheCfg(SERVER_CACHE));
        serverNode.createCache(CACHE_WITHOUT_TABLES_NAME);

        clientNode.createCache(newCacheCfg(CLIENT_CACHE));

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        serverNode.destroyCache(SERVER_CACHE);
        serverNode.destroyCache(CACHE_WITHOUT_TABLES_NAME);

        clientNode.destroyCache(CLIENT_CACHE);

        super.afterTest();
    }

    /** Add cache with table to ignite config. */
    private IgniteConfiguration withPrestartedCache(IgniteConfiguration cfg) {
        CacheConfiguration<Long, UUID> ccfg = new CacheConfiguration<Long, UUID>(PRESTARTED_CACHE)
            .setIndexedTypes(Long.class, UUID.class).setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** Basic negative test. */
    public void testNonExistingSchemas() {
        assertSchemaMissedOnBothNodes("notExistingSchema");

        assertSchemaMissedOnBothNodes("\"Public\"");

        assertSchemaMissedOnBothNodes("ServerNodeCache"); // not quoted

        assertSchemaMissedOnBothNodes("\"SERVERNODECACHE\"");

        assertSchemaMissedOnBothNodes("\"ServerNodeCa\"");

        assertSchemaMissedOnBothNodes("\"CLIENTNODECACHE\"");
    }

    /** Verifies that if cache doesn't have indexed types, there is no schema with the same name. */
    public void testCacheWithoutTables() {
        assertSchemaMissedOnBothNodes('"' + CACHE_WITHOUT_TABLES_NAME + '"');
    }

    /**
     * Connect to client node and check that schema defined in server cache configuration exists.
     */
    public void testPrestartedCacheTable() {
        assertSchemaExistOnBothNodes("\"" + PRESTARTED_CACHE + "\"");
    }

    /** PUBLIC schema test. */
    public void testDefaultSchema() {
        assertSchemaExistOnBothNodes(""); // implicitly "PUBLIC"

        assertSchemaExistOnBothNodes("public");

        assertSchemaExistOnBothNodes("Public");

        assertSchemaExistOnBothNodes("\"PUBLIC\"");
    }

    /**
     * Test that if we set custom schema in cache configuration,
     * we will find that custom schema, not schema with cache name.
     */
    public void testCustomSchemaName() {
        String cacheName = "CacheWithCustomSchema";

        String schemaName = "MyCustomSchema";

        CacheConfiguration<Long, UUID> ccfg = newCacheCfg(cacheName).setSqlSchema(schemaName);

        try (IgniteCache<Long, UUID> cache = serverNode.createCache(ccfg)) {
            assertSchemaExistOnBothNodes(schemaName);
            assertSchemaMissedOnBothNodes('"' + cacheName + '"');
        } finally {
            serverNode.destroyCache(cacheName);
        }

    }

    /** Check schema created (with the table) via {@link CacheConfiguration#setIndexedTypes} exists. */
    public void testDynamicCacheSchemas() {
        assertSchemaExistOnBothNodes("\"ClientNodeCache\"");

        assertSchemaExistOnBothNodes("\"ServerNodeCache\"");
    }

    /**
     * Try to connect with quoted empty string schema name ({@code "\"\""}).
     * Connection with such schema should be rejected by jdbc client.
     */
    public void testEmptyEscapedSchemaIsRejectedByClient() {
        Callable<Void> connect = connectToNodeCommand("\"\"", clientNode);

        GridTestUtils.assertThrows(log, connect, SQLException.class, "Schema cannot be empty sql identifier (\"\")");
    }

    /**
     * Command that connects to node, does nothing and closes connection.
     *
     * @param schemaName - schema name to use in connection url.
     * @param node - to what node to connect.
     * @return Callable command.
     */
    private Callable<Void> connectToNodeCommand (String schemaName, IgniteEx node) {
        int port = GridTestUtils.jdbcPortOf(node);

        Callable<Void> command = () -> {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + port + "/" + schemaName)) {
                // do nothing, just connect, possibly get an exception and close connection.
            }

            return null;
        };

        return command;
    }

    /** Check that we can connect to node with specified schema. */
    private void assertSchemaExistsOn(String schemaName, IgniteEx node) {
        try  {
            connectToNodeCommand(schemaName, node).call();
        }
        catch (SQLException sqlEx) {
            throw new AssertionError("Schema " + schemaName + " seems to be missed, but it should exist. " +
                "Tried to connect to " + node.name() + ".", sqlEx);
        } catch (Exception unexpected){
            throw new AssertionError("Unexpected exception have been thrown. Seems to be a environment error.", unexpected);
        }
    }

    /**
     * Checks that thin driver is able to connect to both server and client nodes with {@code schemaName}
     */
    private void assertSchemaExistOnBothNodes(String schemaName) {
        assertSchemaExistsOn(schemaName, clientNode);
        assertSchemaExistsOn(schemaName, serverNode);
    }


    /** Check that we can't connect to specified node with specified schema */
    private void assertSchemaMissedOn(String incorrectSchema, IgniteEx node) {
        Callable<Void> mustThrow = connectToNodeCommand(incorrectSchema, node);

        GridTestUtils.assertThrows(log, mustThrow, SQLException.class, "Schema with name");
    }

    /**
     * Verifies that correct exception is thrown, if thin driver connected with specified non-existing schema name.
     */
    private void assertSchemaMissedOnBothNodes(String incorrectSchema) {
        assertSchemaMissedOn(incorrectSchema, clientNode);
        assertSchemaMissedOn(incorrectSchema, serverNode);
    }
}
