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
package org.apache.ignite.logger.log4j2;

import java.io.File;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests that several grids log to files with correct names.
 */
@GridCommonTest(group = "Logger")
public class GridLog4j2CorrectFileNameTest extends TestCase {

    /**
     * Tests correct behaviour in case 2 local nodes are started.
     *
     * @throws Exception If error occurs.
     */
    public void testLogFilesTwoNodes() throws Exception {
        checkOneNode(0);
        checkOneNode(1);
    }

    /**
     * Starts the local node and checks for presence of log file. Also checks
     * that this is really a log of a started node.
     * 
     * @param id Test-local node ID.
     * @throws Exception If error occurred.
     */
    private void checkOneNode(int id) throws Exception {
        try (Ignite ignite = G.start(getConfiguration("grid" + id))) {
            String id8 = U.id8(ignite.cluster().localNode().id());
            String logPath = "work/log/ignite-" + id8 + ".log";
            File logFile = U.resolveIgnitePath(logPath);
            assertNotNull("Failed to resolve path: " + logPath, logFile);
            assertTrue("Log file does not exist: " + logFile, logFile.exists());
            // We have a row in log with the following content
            // con >>> Local node [ID=NodeId ]
            String logContent = U.readFileToString(logFile.getAbsolutePath(),
                    "UTF-8");
            assertTrue(
                    "Log file does not contain it's node ID: " + logFile,
                    logContent.contains(">>> Local node [ID="
                            + id8.toUpperCase()));
        }
    }

    /**
     * Creates grid configuration.
     *
     * @param gridName Grid name.
     * @return Grid configuration.
     * @throws Exception If error occurred.
     */
    private static IgniteConfiguration getConfiguration(String gridName)
            throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        
   
        cfg.setGridName(gridName);
        // We need of a configuration file passed in
        File xml = GridTestUtils
                .resolveIgnitePath("modules/core/src/test/config/log4j2-test.xml");

        assert xml != null;
        assert xml.exists() == true;

        cfg.setGridLogger(new Log4J2Logger(xml));
        cfg.setConnectorConfiguration(null);

        return cfg;
    }
}