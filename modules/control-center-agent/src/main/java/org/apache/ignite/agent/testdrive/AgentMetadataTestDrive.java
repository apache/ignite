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

package org.apache.ignite.agent.testdrive;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

import static org.apache.ignite.agent.AgentUtils.resolvePath;

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
     * @param jdbcUrl Connection url.
     * @return true if url is used for test-drive.
     */
    public static boolean isTestDriveUrl(String jdbcUrl) {
        return "jdbc:h2:mem:test-drive-db".equals(jdbcUrl);
    }

    /**
     * Start H2 database and populate it with several tables.
     */
    public static void testDrive() {
        if (initLatch.compareAndSet(false, true)) {
            log.info("TEST-DRIVE: Prepare in-memory H2 database...");

            try {
                Connection conn = DriverManager.getConnection("jdbc:h2:mem:test-drive-db;DB_CLOSE_DELAY=-1", "sa", "");

                File sqlScript = resolvePath("test-drive/test-drive.sql");

                if (sqlScript == null) {
                    log.error("TEST-DRIVE: Failed to find test drive script file: test-drive/test-drive.sql");
                    log.error("TEST-DRIVE: Test drive for metadata not started");

                    return;
                }

                RunScript.execute(conn, new FileReader(sqlScript));

                log.info("TEST-DRIVE: Sample tables created.");

                conn.close();

                Server.createTcpServer("-tcpDaemon").start();

                log.info("TEST-DRIVE: TcpServer stared.");

                log.info("TEST-DRIVE: JDBC URL for test drive metadata load: jdbc:h2:mem:test-drive-db");
            }
            catch (Exception e) {
                log.error("TEST-DRIVE: Failed to start test drive for metadata!", e);
            }
        }
    }
}
