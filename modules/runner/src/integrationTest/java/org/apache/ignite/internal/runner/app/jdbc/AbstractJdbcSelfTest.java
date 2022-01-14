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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Abstract jdbc self test.
 */
public class AbstractJdbcSelfTest {
    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.0.1:10800";

    /** Cluster nodes. */
    protected static final List<Ignite> clusterNodes = new ArrayList<>();

    /** Connection. */
    protected static Connection conn;

    /** Statement. */
    protected Statement stmt;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * Creates a cluster of three nodes.
     *
     * @param temp Temporal directory.
     */
    @BeforeAll
    public static void beforeAll(@TempDir Path temp, TestInfo testInfo) throws SQLException {
        String nodeName = testNodeName(testInfo, 47500);

        String configStr = "node.metastorageNodes: [ \"" + nodeName + "\" ]";

        clusterNodes.add(IgnitionManager.start(nodeName, configStr, temp.resolve(nodeName)));

        conn = DriverManager.getConnection(URL);

        conn.setSchema("PUBLIC");
    }

    /**
     * Close all cluster nodes.
     *
     * @throws Exception if failed.
     */
    @AfterAll
    public static void afterAll() throws Exception {
        IgniteUtils.closeAll(
                Stream.concat(
                        Stream.of(conn != null && !conn.isClosed() ? conn : (AutoCloseable) () -> {/* NO-OP */}),
                        clusterNodes.stream()
                )
        );

        conn = null;
        clusterNodes.clear();
    }

    @BeforeEach
    protected void beforeTest() throws Exception {
        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    @AfterEach
    protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.close();

            assert stmt.isClosed();
        }
    }

    /**
     * Constructor.
     */
    @SuppressWarnings("AssignmentToStaticFieldFromInstanceMethod")
    protected AbstractJdbcSelfTest() {
        log = IgniteLogger.forClass(getClass());
    }

    /**
     * Returns logger.
     *
     * @return Logger.
     */
    protected IgniteLogger logger() {
        return log;
    }

    /**
     * Checks that the function throws SQLException about a closed result set.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkResultSetClosed(Executable ex) {
        assertThrows(SQLException.class, ex, "Result set is closed");
    }

    /**
     * Checks that the function throws SQLException about a closed statement.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkStatementClosed(Executable ex) {
        assertThrows(SQLException.class, ex, "Statement is closed");
    }

    /**
     * Checks that the function throws SQLException about a closed connection.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkConnectionClosed(Executable ex) {
        assertThrows(SQLException.class, ex, "Connection is closed");
    }

    /**
     * Checks that the function throws SQLFeatureNotSupportedException.
     *
     * @param ex Executable function that throws an error.
     */
    protected void checkNotSupported(Executable ex) {
        assertThrows(SQLFeatureNotSupportedException.class, ex);
    }
}
