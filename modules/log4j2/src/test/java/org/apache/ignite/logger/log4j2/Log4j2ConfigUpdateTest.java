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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checking that Log4j2 configuration is updated when its source file is changed.
 */
public class Log4j2ConfigUpdateTest {
    /** Path to log4j2 configuration with INFO enabled. */
    private static final String LOG_CONFIG_INFO = "modules/log4j2/src/test/config/log4j2-info.xml";

    /** Path to log4j2 configuration with DEBUG enabled. */
    private static final String LOG_CONFIG_DEBUG = "modules/log4j2/src/test/config/log4j2-debug.xml";

    /** Path to log4j2 configuration with DEBUG enabled. */
    private static final String LOG_CONFIG_MAIN = "work/log/log4j2-Log4j2ConfigUpdateTest.xml";

    /** Path to log file. */
    private static final String LOG_DEST = "work/log/Log4j2ConfigUpdateTest.log";

    /**
     * Time to wait before logger configuration changes.
     * This value is made greater than the `monitorInterval` in the config files to avoid relying on exact timing.
     */
    private static final long UPDATE_DELAY = 10000;

    /**
     * Time to wait before updating the configuration file.
     * Should be large enough to be recorded on different file systems.
     */
    private static final int DELAY_BEFORE_MODIFICATION = 5000;

    /**
     * Check that changing log4j2 config file causes the logger configuration to be updated.
     * String-accepting constructor is used.
     */
    @Test
    public void testConfigChangeStringConstructor() throws Exception {
        checkConfigUpdate(new Log4J2LoggerSupplier() {
            @Override public Log4J2Logger get(File cfgFile) throws Exception {
                return new Log4J2Logger(cfgFile.getPath());
            }
        }); // use larger delay to avoid relying on exact timing
    }

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * File-accepting constructor is used.
     */
    @Test
    public void testConfigChangeFileConstructor() throws Exception {
        checkConfigUpdate(new Log4J2LoggerSupplier() {
            @Override public Log4J2Logger get(File cfgFile) throws Exception {
                return new Log4J2Logger(cfgFile);
            }
        }); // use larger delay to avoid relying on exact timing
    }

    /**
     * Check that changing log4j config file causes the logger configuration to be updated.
     * File-accepting constructor is used.
     */
    @Test
    public void testConfigChangeUrlConstructor() throws Exception {
        checkConfigUpdate(new Log4J2LoggerSupplier() {
            @Override public Log4J2Logger get(File cfgFile) throws Exception {
                return new Log4J2Logger(cfgFile.toURI().toURL());
            }
        }); // use larger delay to avoid relying on exact timing
    }

    /**
     * Checks that Log4JLogger is updated after configuration file is changed.
     *  @param logSupplier Function returning a logger instance to be tested.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void checkConfigUpdate(Log4J2LoggerSupplier logSupplier) throws Exception {
        Log4J2Logger.cleanup();

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

        Log4J2Logger log = logSupplier.get(mainCfgFile);

        log.info("Accepted info");
        log.debug("Ignored debug");

        // Wait a bit before copying the file so that new modification date is guaranteed to be different.
        Thread.sleep(DELAY_BEFORE_MODIFICATION);

        // Replace current config with DEBUG config.
        Files.copy(debugCfgFile.toPath(), mainCfgFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        mainCfgFile.setLastModified(new Date().getTime());

        // Wait for the update to happen.
        Thread.sleep(UPDATE_DELAY);

        log.debug("Accepted debug");

        String logContent = U.readFileToString(logFile.getPath(), "UTF-8");

        assertTrue(logContent.contains("[INFO] Accepted info"));
        assertFalse(logContent.contains("[DEBUG] Ignored debug"));
        assertTrue(logContent.contains("[DEBUG] Accepted debug"));

        mainCfgFile.delete();
        Log4J2Logger.cleanup();
    }

    /** Creates Log4J2Logger instance for testing. */
    private interface Log4J2LoggerSupplier {
        /** Creates Log4J2Logger instance for testing. */
        Log4J2Logger get(File cfgFile) throws Exception;
    }
}
