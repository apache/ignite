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

package org.apache.ignite.logger.java;

import java.io.File;
import java.util.UUID;
import java.util.logging.LogManager;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.logger.java.JavaLogger.DFLT_CONFIG_PATH;

/**
 * Java logger test.
 */
@GridCommonTest(group = "Logger")
public class JavaLoggerTest extends GridCommonAbstractTest {
    /**
     * Path to jul configuration with DEBUG enabled.
     */
    private static final String LOG_CONFIG_DEBUG = "modules/core/src/test/config/jul-debug.properties";

    /**
     * Reset JavaLogger.
     */
    @Override protected void afterTest() throws Exception {
        GridTestUtils.setFieldValue(JavaLogger.class, JavaLogger.class, "inited", false);
    }


    /**
     * Check JavaLogger default constructor.
     */
    @Test
    public void testDefaultConstructorWithDefaultConfig() {
        IgniteLogger log1 = new JavaLogger();
        IgniteLogger log2 = log1.getLogger(getClass());

        assertTrue(log1.toString().contains("JavaLogger"));
        assertTrue(log1.toString().contains(DFLT_CONFIG_PATH));

        assertTrue(log2.toString().contains("JavaLogger"));
        assertTrue(log2.toString().contains(DFLT_CONFIG_PATH));
    }

    /**
     * Check JavaLogger constructor from java.util.logging.config.file property.
     */
    @Test
    public void testDefaultConstructorWithProperty() throws Exception {
        File file = new File(U.getIgniteHome(), LOG_CONFIG_DEBUG);
        System.setProperty("java.util.logging.config.file", file.getPath());
        // Call readConfiguration explicitly because Logger.getLogger was already called during IgniteUtils initialization
        LogManager.getLogManager().readConfiguration();

        IgniteLogger log1 = new JavaLogger();
        assertTrue(log1.toString().contains("JavaLogger"));
        assertTrue(log1.toString().contains(LOG_CONFIG_DEBUG));
        assertTrue(log1.isDebugEnabled());

        IgniteLogger log2 = log1.getLogger(getClass());
        assertTrue(log2.toString().contains("JavaLogger"));
        assertTrue(log2.toString().contains(LOG_CONFIG_DEBUG));
        assertTrue(log1.isDebugEnabled());

        System.clearProperty("java.util.logging.config.file");
    }

    /**
     * Check Grid logging.
     */
    @Test
    public void testGridLoggingWithDefaultLogger() throws Exception {
        LogListener lsn = LogListener.matches("JavaLogger [quiet=true,")
            .andMatches(DFLT_CONFIG_PATH)
            .build();
        ListeningTestLogger log = new ListeningTestLogger(new JavaLogger(), lsn);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName());
        cfg.setGridLogger(log);
        startGrid(cfg);

        assertTrue(GridTestUtils.waitForCondition(lsn::check, 2_000));
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLogInitialize() throws Exception {
        JavaLogger log = new JavaLogger();

        ((JavaLogger)log).setWorkDirectory(U.defaultWorkDirectory());
        ((IgniteLoggerEx)log).setApplicationAndNode(null, UUID.fromString("00000000-1111-2222-3333-444444444444"));

        System.out.println(log.toString());

        assertTrue(log.toString().contains("JavaLogger"));
        assertTrue(log.toString().contains(DFLT_CONFIG_PATH));

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        assert log.isInfoEnabled();

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(JavaLoggerTest.class.getName()) instanceof JavaLogger;

        assert log.fileName() != null;

        // Ensure we don't get pattern, only actual file name is allowed here.
        assert !log.fileName().contains("%");
        assert log.fileName().contains("ignite");

        System.clearProperty("java.util.logging.config.file");
        GridTestUtils.setFieldValue(JavaLogger.class, JavaLogger.class, "inited", false);

        log = new JavaLogger();

        ((JavaLogger)log).setWorkDirectory(U.defaultWorkDirectory());
        ((IgniteLoggerEx)log).setApplicationAndNode("other-app", UUID.fromString("00000000-1111-2222-3333-444444444444"));

        assert log.fileName() != null;

        // Ensure we don't get pattern, only actual file name is allowed here.
        assert !log.fileName().contains("%");
        assert log.fileName().contains("other-app");
    }
}
