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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Testing that markers are supported by log4j2 implementation.
 */
public class Log4j2LoggerMarkerTest {
    /** Path to log4j configuration. */
    private static final String LOG_CONFIG = "modules/log4j2/src/test/config/log4j2-markers.xml";

    /** Path to full log. */
    private static final String LOG_ALL = "work/log/all.log";

    /** Path to filtered log. */
    private static final String LOG_FILTERED = "work/log/filtered.log";

    /** */
    @Before
    public void setUp() {
        Log4J2Logger.cleanup();

        deleteLogs();
    }

    /** */
    @After
    public void tearDown() {
        deleteLogs();
    }

    /** */
    @Test
    public void testMarkerFiltering() throws Exception {
        // create log
        Log4J2Logger log = new Log4J2Logger(LOG_CONFIG);

        // populate log with messages
        log.error("IGNORE_ME", "Ignored error", null);
        log.warning("IGNORE_ME", "Ignored warning", null);
        log.info("IGNORE_ME", "Ignored info");
        log.debug("IGNORE_ME", "Ignored debug");
        log.trace("IGNORE_ME", "Ignored trace");

        log.error("ACCEPT_ME", "Accepted error", null);
        log.warning("ACCEPT_ME", "Accepted warning", null);
        log.info("ACCEPT_ME", "Accepted info");
        log.debug("ACCEPT_ME", "Accepted debug");
        log.trace("ACCEPT_ME", "Accepted trace");

        // check file with all messages
        File allFile = U.resolveIgnitePath(LOG_ALL);
        assertNotNull(allFile);
        String all = U.readFileToString(allFile.getPath(), "UTF-8");

        assertTrue(all.contains("Ignored error"));
        assertTrue(all.contains("Ignored warning"));
        assertTrue(all.contains("Ignored info"));
        assertTrue(all.contains("Ignored debug"));
        assertTrue(all.contains("Ignored trace"));
        assertTrue(all.contains("Accepted error"));
        assertTrue(all.contains("Accepted warning"));
        assertTrue(all.contains("Accepted info"));
        assertTrue(all.contains("Accepted debug"));
        assertTrue(all.contains("Accepted trace"));

        // check file with one marker filtered out
        File filteredFile = U.resolveIgnitePath(LOG_FILTERED);
        assertNotNull(filteredFile);
        String filtered = U.readFileToString(filteredFile.getPath(), "UTF-8");

        assertFalse(filtered.contains("Ignored error"));
        assertFalse(filtered.contains("Ignored warning"));
        assertFalse(filtered.contains("Ignored info"));
        assertFalse(filtered.contains("Ignored debug"));
        assertFalse(filtered.contains("Ignored trace"));
        assertTrue(filtered.contains("Accepted error"));
        assertTrue(filtered.contains("Accepted warning"));
        assertTrue(filtered.contains("Accepted info"));
        assertTrue(filtered.contains("Accepted debug"));
        assertTrue(filtered.contains("Accepted trace"));
    }

    /** Delete existing logs, if any */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteLogs() {
        new File(LOG_ALL).delete();
        new File(LOG_FILTERED).delete();
    }
}
