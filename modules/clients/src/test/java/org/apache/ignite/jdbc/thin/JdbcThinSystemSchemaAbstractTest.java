/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Abstract test verifies that system schema has one of the predefined names.
 */
public abstract class JdbcThinSystemSchemaAbstractTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    protected static final String BASE_URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Connection. */
    protected Connection conn;

    /** */
    protected abstract String expectedSysSchemaName();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.setFieldValue(QueryUtils.class, "schemaSys", null);

        startGrid(0);
    }

    /** */
    @Before
    public void init() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
    }

    /** */
    @After
    public void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed())
            conn.close();
    }

    /** */
    protected String getUrl() {
        return BASE_URL;
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testSysSchema() throws Exception {
        final String schemaName = "SYS";

        if (schemaName.equals(expectedSysSchemaName()))
            verifyNodesView(schemaName);
        else
            GridTestUtils.assertThrows(log, () -> verifyNodesView(schemaName),
                IgniteException.class, "Failed to parse query. Schema \"" + schemaName +"\" not found;");
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testIgniteSchema() throws SQLException {
        final String schemaName = "IGNITE";

        if (schemaName.equals(expectedSysSchemaName()))
            verifyNodesView(schemaName);
        else
            GridTestUtils.assertThrows(log, () -> verifyNodesView(schemaName),
                IgniteException.class, "Failed to parse query. Schema \"" + schemaName +"\" not found;");
    }

    /** Verifies that nodes from nodes view and actual set of running nodes are equal. */
    private void verifyNodesView(String schema) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT NODE_ID FROM " + schema + ".nodes");

        HashSet<String> act = new HashSet<>();

        while (rs.next())
            act.add(rs.getString(1));

        Set<String> exp = G.allGrids().stream()
            .map(iEx -> ((IgniteEx)iEx).localNode().id().toString())
            .collect(Collectors.toSet());

        Assert.assertEquals(exp, act);
    }
}
