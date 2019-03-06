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

package org.apache.ignite.console.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

import static org.apache.ignite.console.agent.AgentUtils.resolvePath;

/**
 * Demo for metadata load from database.
 *
 * H2 database will be started and several tables will be created.
 */
public class AgentMetadataDemo {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataDemo.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /**
     * @param jdbcUrl Connection url.
     * @return true if url is used for test-drive.
     */
    public static boolean isTestDriveUrl(String jdbcUrl) {
        return "jdbc:h2:mem:demo-db".equals(jdbcUrl);
    }

    /**
     * Start H2 database and populate it with several tables.
     */
    public static Connection testDrive() throws SQLException {
        if (initLatch.compareAndSet(false, true)) {
            log.info("DEMO: Prepare in-memory H2 database...");

            try {
                Class.forName("org.h2.Driver");

                Connection conn = DriverManager.getConnection("jdbc:h2:mem:demo-db;DB_CLOSE_DELAY=-1", "sa", "");

                File sqlScript = resolvePath("demo/db-init.sql");

                //noinspection ConstantConditions
                RunScript.execute(conn, new FileReader(sqlScript));

                log.info("DEMO: Sample tables created.");

                conn.close();

                Server.createTcpServer("-tcpDaemon").start();

                log.info("DEMO: TcpServer stared.");

                log.info("DEMO: JDBC URL for test drive metadata load: jdbc:h2:mem:demo-db");
            }
            catch (ClassNotFoundException e) {
                log.error("DEMO: Failed to load H2 driver!", e);

                throw new SQLException("Failed to load H2 driver", e);
            }
            catch (SQLException e) {
                log.error("DEMO: Failed to start test drive for metadata!", e);

                throw e;
            }
            catch (FileNotFoundException | NullPointerException e) {
                log.error("DEMO: Failed to find demo database init script file: demo/db-init.sql");

                throw new SQLException("Failed to start demo for metadata", e);
            }
        }

        return DriverManager.getConnection("jdbc:h2:mem:demo-db;DB_CLOSE_DELAY=-1", "sa", "");
    }
}
