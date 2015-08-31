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

package org.apache.ignite.logger.log4j;

import java.io.File;
import java.util.Enumeration;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.varia.LevelRangeFilter;

/**
 * Tests that several grids log to files with correct names.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jCorrectFileNameTest extends TestCase {
    /** Appender */
    private Log4jRollingFileAppender appender;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        Logger root = Logger.getRootLogger();

        for (Enumeration appenders = root.getAllAppenders(); appenders.hasMoreElements(); ) {
            if (appenders.nextElement() instanceof Log4jRollingFileAppender)
                return;
        }

        appender = createAppender();

        root.addAppender(appender);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        if (appender != null) {
            Logger.getRootLogger().removeAppender(Log4jRollingFileAppender.class.getSimpleName());

            Log4JLogger.removeAppender(appender);
        }
    }

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
     * Starts the local node and checks for presence of log file.
     * Also checks that this is really a log of a started node.
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

            String logContent = U.readFileToString(logFile.getAbsolutePath(), "UTF-8");

            assertTrue("Log file does not contain it's node ID: " + logFile,
                logContent.contains(">>> Local node [ID=" + id8.toUpperCase()));
        }
    }

    /**
     * Creates grid configuration.
     *
     * @param gridName Grid name.
     * @return Grid configuration.
     * @throws Exception If error occurred.
     */
    private static IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);
        cfg.setGridLogger(new Log4JLogger());
        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * Creates new GridLog4jRollingFileAppender.
     *
     * @return GridLog4jRollingFileAppender.
     * @throws Exception If error occurred.
     */
    private static Log4jRollingFileAppender createAppender() throws Exception {
        Log4jRollingFileAppender appender = new Log4jRollingFileAppender();

        appender.setLayout(new PatternLayout("[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n"));
        appender.setFile("work/log/ignite.log");
        appender.setName(Log4jRollingFileAppender.class.getSimpleName());

        LevelRangeFilter lvlFilter = new LevelRangeFilter();

        lvlFilter.setLevelMin(Level.DEBUG);
        lvlFilter.setLevelMax(Level.INFO);

        appender.addFilter(lvlFilter);

        return appender;
    }
}
