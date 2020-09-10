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

package org.apache.ignite.compatibility.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests that current client version can connect to the server with specified version and
 * specified client version can connect to the current server version.
 */
@RunWith(Parameterized.class)
public class JdbcThinCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Rows count. */
    private static final int ROWS_CNT = 10;

    /** Parameters. */
    @Parameterized.Parameters(name = "Version {0}")
    public static Iterable<String[]> versions() {
        return Arrays.asList(
                new String[] {"2.7.0"},
                new String[] {"2.7.5"},
                new String[] {"2.7.6"},
                new String[] {"2.8.0"},
                new String[] {"2.8.1"}
        );
    }

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String ver;

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing", "ignite-indexing", false));

        return dependencies;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOldClientToCurrentServer() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            initTable(ignite);

            GridJavaProcess proc = GridJavaProcess.exec(
                JdbcThinQueryRunner.class.getName(),
                null,
                log,
                log::info,
                null,
                null,
                getProcessProxyJvmArgs(ver),
                null
            );

            try {
                GridTestUtils.waitForCondition(() -> !proc.getProcess().isAlive(), 5_000L);

                assertEquals(0, proc.getProcess().exitValue());
            }
            finally {
                if (proc.getProcess().isAlive())
                    proc.kill();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCurrentClientToOldServer() throws Exception {
        IgniteProcessProxy proxy = null;

        try {
            Ignite ignite = startGrid(1, ver,
                cfg -> cfg
                    .setLocalHost("127.0.0.1")
                    .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true))),
                JdbcThinCompatibilityTest::initTable);

            proxy = IgniteProcessProxy.ignite(ignite.name());

            testJdbcQuery();
        }
        finally {
            stopAllGrids();

            if (proxy != null) {
                Process proc = proxy.getProcess().getProcess();

                // We should wait until process exits, or it can affect next tests.
                GridTestUtils.waitForCondition(() -> !proc.isAlive(), 5_000L);
            }
        }
    }

    /** Execute sql. */
    private static void executeSql(IgniteEx igniteEx, String sql) {
        igniteEx.context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /** */
    private static void initTable(Ignite ignite) {
        IgniteEx igniteEx = (IgniteEx)ignite;

        executeSql(igniteEx, "CREATE TABLE " + TABLE_NAME + " (id int primary key, name varchar)");

        for (int i = 0; i < ROWS_CNT; i++)
            executeSql(igniteEx, "INSERT INTO " + TABLE_NAME + " (id, name) VALUES(" + i + ", 'name" + i + "')");
    }

    /** */
    private static void testJdbcQuery() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM " + TABLE_NAME + " ORDER BY id");

            assertNotNull(rs);

            int cnt = 0;

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");

                assertEquals(cnt, id);
                assertEquals("name" + cnt, name);

                cnt++;
            }

            assertEquals(ROWS_CNT, cnt);
        }
    }

    /**
     * Runner class to test query from remote JVM process with old Ignite version as dependencies in class path.
     */
    public static class JdbcThinQueryRunner {
        /** */
        public static void main(String[] args) throws Exception {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());
            X.println("Start JDBC connection with Ignite version: " + IgniteVersionUtils.VER);

            testJdbcQuery();

            X.println("Success");
        }
    }
}
