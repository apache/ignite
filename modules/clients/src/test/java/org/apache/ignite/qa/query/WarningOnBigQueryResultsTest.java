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

package org.apache.ignite.qa.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import javax.management.InvalidAttributeValueException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for log print for long running query.
 */
public class WarningOnBigQueryResultsTest extends WarningOnBigQueryResultsBaseTest {
    /** JDBC thin URL. */
    private static final String JDBC_THIN_URL = "jdbc:ignite:thin://127.0.0.1:" + CLI_PORT;

    /** Address to connect thin client. */
    private static final String THIN_CLI_ADDR = "127.0.0.1:" + CLI_PORT;

    /** JDBC v2 URL. */
    private static final String JDBC_V2_URL = "jdbc:ignite:cfg://modules/clients/src/test/config/jdbc-config.xml";

    /**
     */
    @Test
    public void testDisableWarning() throws Exception {
        setBigResultThreshold(grid(0), 0, 0);
        setBigResultThreshold(grid(1), 0, 0);
        setBigResultThreshold(grid(2), 0, 0);
        setBigResultThreshold(grid(3), 0, 0);

        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(new SqlFieldsQueryEx("SELECT * FROM TEST0", true)
                    .setSchema("TEST0"),
                false).getAll().size());

        assertEquals(0, listener(grid(0)).messageCount());
        assertEquals(0, listener(grid(1)).messageCount());
        assertEquals(0, listener(grid(2)).messageCount());
        assertEquals(0, listener(grid(3)).messageCount());

        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(new SqlFieldsQueryEx("SELECT * FROM TEST1", true)
                    .setSchema("TEST1"),
                false).getAll().size());

        assertEquals(0, listener(grid(0)).messageCount());
        assertEquals(0, listener(grid(1)).messageCount());
        assertEquals(0, listener(grid(2)).messageCount());
        assertEquals(0, listener(grid(3)).messageCount());

        setBigResultThreshold(grid(0), -1, -1);
        setBigResultThreshold(grid(1), -1, -1);
        setBigResultThreshold(grid(2), -1, -1);
        setBigResultThreshold(grid(3), -1, -1);

        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(new SqlFieldsQueryEx("SELECT * FROM TEST0", true)
                    .setSchema("TEST0"),
                false).getAll().size());

        assertEquals(0, listener(grid(0)).messageCount());
        assertEquals(0, listener(grid(1)).messageCount());
        assertEquals(0, listener(grid(2)).messageCount());
        assertEquals(0, listener(grid(3)).messageCount());

        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(new SqlFieldsQueryEx("SELECT * FROM TEST1", true)
                    .setSchema("TEST1"),
                false).getAll().size());

        assertEquals(0, listener(grid(0)).messageCount());
        assertEquals(0, listener(grid(1)).messageCount());
        assertEquals(0, listener(grid(2)).messageCount());
        assertEquals(0, listener(grid(3)).messageCount());
    }

    /**
     */
    @Test
    public void testQueryCacheTest0() throws Exception {
        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(
                new SqlFieldsQueryEx("SELECT * FROM TEST0 ORDER BY val DESC", true)
                    .setSchema("TEST0"),
                false).getAll().size());

        assertEquals(6, listener(grid("cli")).messageCount());
        assertEquals(Arrays.asList(10L, 30L, 90L, 270L, 810L, 2000L), listener(grid("cli")).fetched);

        checkDurations(listener(grid("cli")).duration);

        assertEquals("REDUCE", listener(grid("cli")).type);
        assertEquals("TEST0", listener(grid("cli")).schema);

        assertFalse(listener(grid("cli")).enforceJoinOrder);
        assertFalse(listener(grid("cli")).distributedJoin);
        assertFalse(listener(grid("cli")).lazy);

        checkStateAfterQuery0("TEST0");
    }

    /**
     */
    @Test
    public void testQueryInsideCompute() throws Exception {
        List<List<?>> res = grid("cli").compute(grid("cli").cluster().forNode(grid(0).localNode())).call(
            new IgniteCallable<List<List<?>>>() {
                @IgniteInstanceResource
                Ignite ign;

                @Override public List<List<?>> call() throws Exception {
                    return ign.cache("test0").query(new SqlFieldsQuery("SELECT * FROM TEST0")).getAll();
                }
            });

        assertEquals(KEYS_PER_NODE * 2, res.size());

        checkStateAfterQuery0("TEST0");
    }

    /**
     */
    @Test
    public void testQueryCacheTest1() throws Exception {
        assertEquals(KEYS_PER_NODE * 2,
            grid("cli").context().query().querySqlFields(new SqlFieldsQueryEx("SELECT * FROM TEST1", true)
                    .setSchema("TEST1")
                    .setLazy(true)
                    .setEnforceJoinOrder(true),
                false).getAll().size());

        assertEquals(0, listener(grid(0)).messageCount());
        assertEquals(0, listener(grid(1)).messageCount());
        assertEquals(6, listener(grid(2)).messageCount());
        assertEquals(2, listener(grid(3)).messageCount());

        assertEquals(Arrays.asList(50L, 100L, 200L, 400L, 800L, 1000L), listener(grid(2)).fetched);
        assertEquals(Arrays.asList(100L, 1000L), listener(grid(3)).fetched);

        checkDurations(listener(grid(2)).duration);
        checkDurations(listener(grid(3)).duration);

        assertEquals("MAP", listener(grid(2)).type);
        assertEquals("MAP", listener(grid(3)).type);

        assertEquals("TEST1", listener(grid(2)).schema);
        assertEquals("TEST1", listener(grid(3)).schema);

        assertTrue(listener(grid(2)).enforceJoinOrder);
        assertTrue(listener(grid(3)).enforceJoinOrder);

        assertFalse(listener(grid(2)).distributedJoin);
        assertFalse(listener(grid(3)).distributedJoin);

        assertTrue(listener(grid(2)).lazy);
        assertTrue(listener(grid(3)).lazy);
    }

    /**
     */
    @Test
    public void testQueryJdbcThin() throws Exception {
        checkJdbc(JDBC_THIN_URL);
    }

    /**
     */
    @Test
    public void testQueryJdbcV2() throws Exception {
        checkJdbc(JDBC_V2_URL);
    }

    /**
     */
    @Test
    public void testThinClient() throws Exception {
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(THIN_CLI_ADDR))) {
            assertEquals(KEYS_PER_NODE * 2, cli.query(new SqlFieldsQueryEx("SELECT * FROM TEST0", true)
                .setSchema("TEST0")).getAll().size());

            checkStateAfterQuery0("TEST0");
        }
    }

    /**
     *
     */
    @Test
    public void testJmxAttributesValues() throws Exception {
        GridTestUtils.assertThrows(log, () -> {
            GridTestUtils.setJmxAttribute(grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold", "qwer");

            return null;
        }, InvalidAttributeValueException.class, "Invalid value for attribute ResultSetSizeThreshold: qwer");

        GridTestUtils.assertThrows(log, () -> {
            GridTestUtils.setJmxAttribute(grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold", 123.456);

            return null;
        }, InvalidAttributeValueException.class, "Invalid value for attribute ResultSetSizeThreshold: 123.456");

        GridTestUtils.setJmxAttribute(grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold", -1);
        assertEquals(-1L, GridTestUtils.getJmxAttribute(
            grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold"));

        GridTestUtils.setJmxAttribute(grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold", 100000000);
        assertEquals(100000000L, GridTestUtils.getJmxAttribute(
            grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThreshold"));

        GridTestUtils.assertThrows(log, () -> {
            GridTestUtils.setJmxAttribute(
                grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier", "qwer");

            return null;
        }, InvalidAttributeValueException.class, "Invalid value for attribute ResultSetSizeThresholdMultiplier: qwer");

        GridTestUtils.assertThrows(log, () -> {
            GridTestUtils.setJmxAttribute(
                grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier", 123.456);

            return null;
        }, InvalidAttributeValueException.class, "Invalid value for attribute ResultSetSizeThresholdMultiplier: 123.456");

        GridTestUtils.assertThrows(log, () -> {
            GridTestUtils.setJmxAttribute(
                grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier", 0.63);

            return null;
        }, InvalidAttributeValueException.class, "Invalid value for attribute ResultSetSizeThresholdMultiplier: 0.63");

        GridTestUtils.setJmxAttribute(
            grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier", 100000000);
        assertEquals(100000000, GridTestUtils.getJmxAttribute(
            grid(0), "SQL Query", "SqlQueryMXBeanImpl", "ResultSetSizeThresholdMultiplier"));
    }

    /**
     */
    void checkJdbc(String url) throws Exception {
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement stmt = c.createStatement()) {

                stmt.execute("SELECT * FROM TEST0.TEST0");

                try (ResultSet rs = stmt.getResultSet()) {

                    int cnt = 0;
                    while (rs.next())
                        cnt++;

                    assertEquals(KEYS_PER_NODE * 2, cnt);

                    checkStateAfterQuery0("PUBLIC");
                }
            }
        }
    }

    /**
     */
    private void checkStateAfterQuery0(String schema) {
        assertEquals(6, listener(grid(0)).messageCount());
        assertEquals(7, listener(grid(1)).messageCount());
        assertEquals(0, listener(grid(2)).messageCount());
        assertEquals(0, listener(grid(3)).messageCount());

        assertEquals(Arrays.asList(10L, 30L, 90L, 270L, 810L, 1000L), listener(grid(0)).fetched);
        assertEquals(Arrays.asList(25L, 50L, 100L, 200L, 400L, 800L, 1000L), listener(grid(1)).fetched);

        checkDurations(listener(grid(0)).duration);
        checkDurations(listener(grid(1)).duration);

        assertEquals("MAP", listener(grid(0)).type);
        assertEquals("MAP", listener(grid(1)).type);

        assertEquals(schema, listener(grid(0)).schema);
        assertEquals(schema, listener(grid(1)).schema);

        assertFalse(listener(grid(0)).enforceJoinOrder);
        assertFalse(listener(grid(1)).enforceJoinOrder);

        assertFalse(listener(grid(0)).distributedJoin);
        assertFalse(listener(grid(1)).distributedJoin);

        assertFalse(listener(grid(0)).lazy);
        assertFalse(listener(grid(1)).lazy);
    }
}
