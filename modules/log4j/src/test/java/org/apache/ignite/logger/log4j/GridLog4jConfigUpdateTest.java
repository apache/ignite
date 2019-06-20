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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.log4j.helpers.FileWatchdog;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checking that Log4j configuration is updated when its source file is changed.
 */
public class GridLog4jConfigUpdateTest {
    /** Path to log4j configuration with INFO enabled. */
    private static final String LOG_CONFIG_INFO = "modules/log4j/src/test/config/log4j-info.xml";

    /** Path to log4j configuration with DEBUG enabled. */
    private static final String LOG_CONFIG_DEBUG = "modules/log4j/src/test/config/log4j-debug.xml";

    /** Path to log4j configuration with DEBUG enabled. */
    private static final String LOG_CONFIG_MAIN = "work/log/log4j-GridLog4jConfigUpdateTest.xml";

    /** Path to log file. */
    private static final String LOG_DEST = "work/log/GridLog4jConfigUpdateTest.log";

    /**
     * Time to wait before updating the configuration file.
     * Should be large enough to be recorded on different file systems.
     */
    private static final int DELAY_BEFORE_MODIFICATION = 5000;

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * String-accepting constructor is used.
     */
    @Test
    public void testConfigChangeStringConstructor() throws Exception {
        checkConfigUpdate(new Log4JLoggerSupplier() {
            @Override public Log4JLogger get(File cfgFile) throws Exception {
                return new Log4JLogger(cfgFile.getPath(), 10);
            }
        }, 5000); // use larger delay to avoid relying on exact timing
    }

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * String-accepting constructor is used.
     */
    @Test
    public void testConfigChangeStringConstructorDefaultDelay() throws Exception {
        checkConfigUpdate(new Log4JLoggerSupplier() {
            @Override public Log4JLogger get(File cfgFile) throws Exception {
                return new Log4JLogger(cfgFile.getPath());
            }
        }, FileWatchdog.DEFAULT_DELAY + 5000); // use larger delay to avoid relying on exact timing
    }

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * File-accepting constructor is used.
     */
    @Test
    public void testConfigChangeFileConstructor() throws Exception {
        checkConfigUpdate(new Log4JLoggerSupplier() {
            @Override public Log4JLogger get(File cfgFile) throws Exception {
                return new Log4JLogger(cfgFile, 10);
            }
        }, 5000); // use larger delay to avoid relying on exact timing
    }

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * File-accepting constructor is used.
     */
    @Test
    public void testConfigChangeUrlConstructor() throws Exception {
        checkConfigUpdate(new Log4JLoggerSupplier() {
            @Override public Log4JLogger get(File cfgFile) throws Exception {
                return new Log4JLogger(cfgFile.toURI().toURL(), 10);
            }
        }, 5000); // use larger delay to avoid relying on exact timing
    }

    /**
     * Checks that Log4JLogger is updated after configuration file is changed.
     *
     * @param logSupplier Function returning a logger instance to be tested.
     * @param delay time to wait after configuration file has been changed.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void checkConfigUpdate(Log4JLoggerSupplier logSupplier, long delay) throws Exception {
        Log4JLogger.cleanup();

        File infoCfgFile = new File(U.getIgniteHome(), LOG_CONFIG_INFO);
        File debugCfgFile = new File(U.getIgniteHome(), LOG_CONFIG_DEBUG);
        File mainCfgFile = new File(U.getIgniteHome(), LOG_CONFIG_MAIN);
        File logFile = new File(U.getIgniteHome(), LOG_DEST);

        System.out.println("INFO config: " + infoCfgFile);
        System.out.println("DEBUG config: " + debugCfgFile);
        System.out.println("Main config: " + mainCfgFile);
        System.out.println("Log file: " + logFile);

        if (logFile.delete())
            System.out.println("Old log file was deleted.");

        // Install INFO config.
        mainCfgFile.getParentFile().mkdirs();
        Files.copy(infoCfgFile.toPath(), mainCfgFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        mainCfgFile.setLastModified(new Date().getTime());

        Log4JLogger log = logSupplier.get(mainCfgFile);

        log.info("Accepted info");
        log.debug("Ignored debug");

        // Wait a bit before copying the file so that new modification date is guaranteed to be different.
        Thread.sleep(DELAY_BEFORE_MODIFICATION);

        // Replace current config with DEBUG config.
        Files.copy(debugCfgFile.toPath(), mainCfgFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        mainCfgFile.setLastModified(new Date().getTime());

        // Wait for the update to happen.
        Thread.sleep(delay);

        log.debug("Accepted debug");

        String logContent = U.readFileToString(logFile.getPath(), "UTF-8");

        assertTrue(logContent.contains("[INFO] Accepted info"));
        assertFalse(logContent.contains("[DEBUG] Ignored debug"));
        assertTrue(logContent.contains("[DEBUG] Accepted debug"));

        mainCfgFile.delete();
        Log4JLogger.cleanup();
    }

    /** Creates Log4JLogger instance for testing. */
    private interface Log4JLoggerSupplier {
        /** Creates Log4JLogger instance for testing. */
        Log4JLogger get(File cfgFile) throws Exception;
    }

}
