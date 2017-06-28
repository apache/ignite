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

import junit.framework.TestCase;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

/**
 * Log4j not initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jInitializationTest extends TestCase {
    /** */
    private static final boolean VERBOSE = true;

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        resetLogger();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        resetLogger();
    }

    /** */
    private void resetLogger() {
        Log4JLogger.reset();

        LogManager.resetConfiguration();

        System.clearProperty(IgniteSystemProperties.IGNITE_QUIET);
    }

    /** */
    public void testLogNotInitialized() {
        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertTrue(log instanceof Log4JLogger);

        assertEquals(Level.OFF, LogManager.getRootLogger().getEffectiveLevel());
    }

    /** */
    public void testLogInitialized() {
        BasicConfigurator.configure();

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertTrue(log instanceof Log4JLogger);

        assertEquals(Level.DEBUG, LogManager.getRootLogger().getEffectiveLevel());
    }

    /** */
    public void testNoAppendersConfigured() {
        LogManager.getRootLogger().setLevel(Level.WARN);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        assertEquals(Level.WARN, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.OFF, logger.getEffectiveLevel());
    }

    /** */
    public void testAutoAddConsoleAppender() {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, String.valueOf(false));

        LogManager.getRootLogger().setLevel(Level.WARN);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        assertEquals(Level.WARN, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.INFO, logger.getEffectiveLevel()); // LogLevel is allowed to be dropped.
    }

    /** */
    public void testAutoAddConsoleAppender2() {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, String.valueOf(false));

        LogManager.getRootLogger().setLevel(Level.DEBUG);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        assertEquals(Level.DEBUG, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.INFO, logger.getEffectiveLevel());
    }

    /** */
    public void testOtherLoggerConfigured() {
        LogManager.getRootLogger().setLevel(Level.DEBUG);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        logger.addAppender(new NullAppender());

        assertEquals(Level.DEBUG, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.DEBUG, logger.getEffectiveLevel()); // LogLevel should not be OFF.
    }

    /** */
    public void testAutoAddConsoleAppenderWithOtherLoggerConfigured() {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, String.valueOf(false));

        LogManager.getRootLogger().setLevel(Level.DEBUG);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        logger.addAppender(new NullAppender());

        assertEquals(Level.DEBUG, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.INFO, logger.getEffectiveLevel());
    }

    /** */
    public void testAutoAddConsoleAppenderWithOtherLoggerConfigured2() {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, String.valueOf(false));

        LogManager.getRootLogger().setLevel(Level.WARN);

        final Logger logger = LogManager.getLogger(GridLog4jInitializationTest.class);

        logger.addAppender(new NullAppender());

        assertEquals(Level.WARN, logger.getEffectiveLevel());

        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jInitializationTest.class);

        if (VERBOSE)
            printLoggerResults(log);

        assertEquals(Level.INFO, logger.getEffectiveLevel()); // LogLevel is allowed to be dropped.
    }

    /** */
    private void printLoggerResults(IgniteLogger log) {
        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");
        else
            System.out.println("DEBUG level is not enabled.");

        if (log.isInfoEnabled())
            log.info("This is 'info' message.");
        else
            System.out.println("INFO level is not enabled.");

        log.warning("This is 'warning' message.");
        log.error("This is 'error' message.");
    }
}