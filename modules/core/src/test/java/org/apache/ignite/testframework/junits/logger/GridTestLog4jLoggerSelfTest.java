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

import java.io.File;
import java.net.URL;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checks that assertion error will be thrown, if logging for the level disabled and log message on this level was invoked.
 */
@RunWith(JUnit4.class)
public class GridTestLog4jLoggerSelfTest {
    /** Path to test configuration. */
    private static final String LOG_PATH_TEST = "modules/log4j2/src/test/config/log4j2-test.xml";

    /** Logger message. */
    private static final String LOG_MESSAGE = "TEST MESSAGE";

    /** Assertion message formatter. */
    private static final String ASSERTION_FORMAT_MSG = "Logging at %s level without checking if %s level is enabled: " + LOG_MESSAGE;

    /** Default root level. */
    private static final Level defaultRootLevel = LogManager.getRootLogger().getLevel();

    /** */
    @Before
    public void beforeTest() {
        GridTestUtils.setFieldValue(GridTestLog4jLogger.class, GridTestLog4jLogger.class, "inited", false);
        Configurator.setRootLevel(Level.WARN);
    }

    /** */
    @After
    public void afterTest() {
        Configurator.setRootLevel(defaultRootLevel);

        assertEquals(defaultRootLevel, LoggerContext.getContext(false).getConfiguration().getRootLogger().getLevel());
    }

    /** */
    @Test
    public void testFileConstructor() throws Exception {
        File xml = GridTestUtils.resolveIgnitePath(LOG_PATH_TEST);

        assert xml != null;

        IgniteLoggerEx log = new GridTestLog4jLogger(xml).getLogger(getClass());

        assertTrue(log.toString().contains("GridTestLog4jLogger"));
        assertTrue(log.toString().contains(xml.getPath()));

        checkLog(log);
    }

    /** */
    @Test
    public void testUrlConstructor() throws Exception {
        File xml = GridTestUtils.resolveIgnitePath(LOG_PATH_TEST);

        assert xml != null;

        URL url = xml.toURI().toURL();
        IgniteLoggerEx log = new GridTestLog4jLogger(url).getLogger(getClass());

        assertTrue(log.toString().contains("GridTestLog4jLogger"));
        assertTrue(log.toString().contains(url.getPath()));

        checkLog(log);
    }

    /** */
    @Test
    public void testPathConstructor() throws Exception {
        IgniteLoggerEx log = new GridTestLog4jLogger(LOG_PATH_TEST).getLogger(getClass());

        assertTrue(log.toString().contains("GridTestLog4jLogger"));
        assertTrue(log.toString().contains(LOG_PATH_TEST));

        checkLog(log);
    }

    /** */
    @Test
    public void testDebug() {
        GridTestLog4jLogger log = new GridTestLog4jLogger();
        assertFalse(log.isDebugEnabled());

        tryLog(() -> log.debug(LOG_MESSAGE), Level.DEBUG);
    }

    /** */
    @Test
    public void testInfo() {
        GridTestLog4jLogger log = new GridTestLog4jLogger();
        assertFalse(log.isInfoEnabled());

        tryLog(() -> log.info(LOG_MESSAGE), Level.INFO);
    }

    /** */
    @Test
    public void testTrace() {
        GridTestLog4jLogger log = new GridTestLog4jLogger();
        assertFalse(log.isTraceEnabled());

        tryLog(() -> log.trace(LOG_MESSAGE), Level.TRACE);
    }

    /** */
    private static void tryLog(RunnableX clo, Level level) {
        String assertionMsg = format(ASSERTION_FORMAT_MSG, level.toString(), level.toString());

        GridTestUtils.assertThrows(null, clo, AssertionError.class, assertionMsg);
    }

    /**
     * Tests logging SPI.
     */
    private void checkLog(IgniteLogger log) {
        assert !log.isDebugEnabled();
        assert log.isInfoEnabled();

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }
}
