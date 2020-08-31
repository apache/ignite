/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testframework.junits.logger;

import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Checks that assertion error will be thrown, if logging for the level disabled and log message on this level was invoked.
 */
@RunWith(JUnit4.class)
public class GridTestLog4jLoggerSelfTest {
    /** Logger message. */
    private static final String LOG_MESSAGE = "TEST MESSAGE";

    /** Assertion message formatter. */
    private static final String ASSERTION_FORMAT_MSG = "Logging at %s level without checking if %s level is enabled: " + LOG_MESSAGE;

    /** Logger. */
    private static final GridTestLog4jLogger LOGGER = new GridTestLog4jLogger();

    /** Default root level. */
    private static final Level defaultRootLevel = Logger.getRootLogger().getLevel();

    /** */
    @BeforeClass
    public static void beforeTests() {
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    /** */
    @AfterClass
    public static void afterTests() {
        Logger.getRootLogger().setLevel(defaultRootLevel);

        assertEquals(defaultRootLevel, Logger.getRootLogger().getLevel());
    }

    /** */
    @Test
    public void testDebug() {
        assertFalse(LOGGER.isDebugEnabled());

        tryLog(() -> LOGGER.debug(LOG_MESSAGE), Level.DEBUG);
    }

    /** */
    @Test
    public void testInfo() {
        assertFalse(LOGGER.isInfoEnabled());

        tryLog(() -> LOGGER.info(LOG_MESSAGE), Level.INFO);
    }

    /** */
    @Test
    public void testTrace() {
        assertFalse(LOGGER.isTraceEnabled());

        tryLog(() -> LOGGER.trace(LOG_MESSAGE), Level.TRACE);
    }

    /** */
    private static void tryLog(RunnableX clo, Level level) {
        String assertionMsg = format(ASSERTION_FORMAT_MSG, level.toString(), level.toString());

        GridTestUtils.assertThrows(null, clo, AssertionError.class, assertionMsg);
    }
}
