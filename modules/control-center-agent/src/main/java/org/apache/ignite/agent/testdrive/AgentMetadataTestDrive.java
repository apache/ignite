/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.agent.testdrive;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.agent.AgentUtils;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

/**
 * Test drive for metadata load from database.
 *
 * H2 database will be started and several tables will be created.
 */
public class AgentMetadataTestDrive {
    /** */
    private static final Logger log = Logger.getLogger(AgentMetadataTestDrive.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /**
     * Execute query.
     *
     * @param conn Connection to database.
     * @param qry Statement to execute.
     */
    private static void query(Connection conn, String qry) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(qry)) {
            ps.executeUpdate();
        }
    }

    /**
     * Start H2 database and populate it with several tables.
     */
    public static void testDrive() {
        if (initLatch.compareAndSet(false, true)) {
            log.log(Level.FINE, "TEST-DRIVE: Prepare in-memory H2 database...");

            try {
                Connection conn = DriverManager.getConnection("jdbc:h2:mem:test-drive-db;DB_CLOSE_DELAY=-1", "sa", "");

                File agentHome = AgentUtils.getAgentHome();

                File sqlScript = new File((agentHome != null) ? new File(agentHome, "test-drive") : new File("test-drive"),
                    "test-drive.sql");

                RunScript.execute(conn, new FileReader(sqlScript));
                log.log(Level.FINE, "TEST-DRIVE: Sample tables created.");

                conn.close();

                Server.createTcpServer("-tcpDaemon").start();

                log.log(Level.INFO, "TEST-DRIVE: TcpServer stared.");

                log.log(Level.INFO, "TEST-DRIVE: JDBC URL for test drive metadata load: jdbc:h2:mem:test-drive-db");
            }
            catch (Exception e) {
                log.log(Level.SEVERE, "TEST-DRIVE: Failed to start test drive for metadata!", e);
            }
        }
    }
}
