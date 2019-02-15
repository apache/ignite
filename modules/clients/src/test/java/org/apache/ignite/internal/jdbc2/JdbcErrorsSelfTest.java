/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.jdbc.JdbcErrorsAbstractSelfTest;
import org.apache.ignite.lang.IgniteCallable;
import org.junit.Test;

/**
 * Test SQLSTATE codes propagation with thin client driver.
 */
public class JdbcErrorsSelfTest extends JdbcErrorsAbstractSelfTest {
    /** Path to JDBC configuration for node that is to start. */
    private static final String CFG_PATH = "modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:cfg://cache=test@" + CFG_PATH);
    }

    /**
     * Test error code for the case when connection string is fine but client can't reach server
     * due to <b>communication problems</b> (not due to clear misconfiguration).
     * @throws SQLException if failed.
     */
    @Test
    public void testConnectionError() throws SQLException {
        final String path = "jdbc:ignite:сfg://cache=test@/unknown/path";

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(path);

                return null;
            }
        }, "08001", "No suitable driver found for " + path);
    }

    /**
     * Test error code for the case when connection string is a mess.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidConnectionStringFormat() throws SQLException {
        final String cfgPath = "cache=";

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                // Empty config path yields an error.
                DriverManager.getConnection("jdbc:ignite:cfg://" + cfgPath);

                return null;
            }
        }, "08001", "Failed to start Ignite node. Spring XML configuration path is invalid: " + cfgPath);
    }
}
