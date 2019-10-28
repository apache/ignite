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
import java.sql.Statement;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_SYSTEM_SCHEMA_NAME_IGNITE;

/**
 * Ensure that change of the system schema name has no effect on sys table content.
 */
@WithSystemProperty(key = IGNITE_SQL_SYSTEM_SCHEMA_NAME_IGNITE, value = "false")
public class JdbcThinSystemSchemaDurabilityTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    protected static final String BASE_URL = "jdbc:ignite:thin://127.0.0.1/";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );
    }

    /** */
    @Test
    public void testMetaAvailableAfterSysNameChanged() throws Exception {
        GridTestUtils.setFieldValue(QueryUtils.class, "schemaSys", null);

        startAndActivateGrid();

        try (Connection conn = DriverManager.getConnection(BASE_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TTT (id int primary key, a long)");

            ensureTblPresent(stmt, "SYS", "TTT");

            stmt.execute(" INSERT INTO TTT (id, a) VALUES (0, 1)");
        }

        stopGrid(0);

        System.clearProperty(IGNITE_SQL_SYSTEM_SCHEMA_NAME_IGNITE);

        GridTestUtils.setFieldValue(QueryUtils.class, "schemaSys", null);

        startAndActivateGrid();

        try (Connection conn = DriverManager.getConnection(BASE_URL); Statement stmt = conn.createStatement()) {
            ensureTblPresent(stmt, "IGNITE", "TTT");
        }
    }

    /** Starts grid with index {@code 0} and activates cluster. */
    private void startAndActivateGrid() throws Exception {
        startGrid(0);

        grid(0).cluster().active(true);
    }

    /**
     * @param stmt Stmt.
     * @param tblName Table name.
     */
    private void ensureTblPresent(Statement stmt, String sysSchemaName, String tblName) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + sysSchemaName + ".tables " +
            "WHERE TABLE_NAME = '" + tblName + "'");

        Assert.assertTrue("Table \"" + tblName + "\" not found", rs.next());
        Assert.assertEquals("Found more than 1 table with name \"" + tblName + "\"",
            1, rs.getInt(1));
    }
}
