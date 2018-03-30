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
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class JdbcThinConnectionSchemaTest extends GridCommonAbstractTest {

    private IgniteEx connectNode;

    private IgniteEx otherNode;

    private static final String OTHER_NODE_CACHE = "OtherNodeCache";

    private static final String CONNECT_NODE_CACHE = "ConnectNodeCache";

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2, true);
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    private CacheConfiguration <Long, UUID> newCfg(String name) {
        CacheConfiguration<Long, UUID> ccfg = new CacheConfiguration<>(name);

        ccfg.setIndexedTypes(Long.class, UUID.class);

        return ccfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        connectNode = grid(0);
        otherNode = grid(1);

        otherNode.createCache(newCfg(OTHER_NODE_CACHE));
        connectNode.createCache(newCfg(CONNECT_NODE_CACHE));
    }

    @Override protected void afterTest() throws Exception {
        otherNode.destroyCache(OTHER_NODE_CACHE);

        connectNode.destroyCache(OTHER_NODE_CACHE);
        super.afterTest();
    }

    private int portOf(IgniteEx node) {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class)
                return rec.port();
        }

        throw new RuntimeException("Could not find port to connect to node " + node );
    }

    public void testNonExistingSchemas () throws Exception {
        assertSchemaMissed("notExistingSchema");

        assertSchemaMissed("\"Public\"");

        assertSchemaMissed("OtherNodeCache"); // not quoted

        assertSchemaMissed("\"OTHERNODECACHE\"");

        assertSchemaMissed("\"OtherNodeCa\"");

        assertSchemaMissed("\"CONNECTNODECACHE\"");
    }

    public void testExistingSchemas () throws Exception {
        assertSchemaExist("public");

        assertSchemaExist("Public");

        assertSchemaExist("\"PUBLIC\"");

        assertSchemaExist("\"ConnectNodeCache\"");

        assertSchemaExist("\"OtherNodeCache\"");
    }

    public void assertSchemaExist(String schema) throws Exception {
        int port = portOf(connectNode);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + port + "/" + schema)) {
            // do nothing
        } catch (SQLException sqlEx){
            throw new AssertionError("Schema " + schema + " seems to be missed, but it should exist.", sqlEx);
        }
    }

    public void assertSchemaMissed (String schema) throws Exception {
        Callable<Void> mustThrow = () -> {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/" + schema)) {
                // do nothing
            }

            return null;
        };

        GridTestUtils.assertThrows(log, mustThrow, SQLException.class, "Should found exactly one schemas");
    }
}
