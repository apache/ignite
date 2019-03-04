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

package org.apache.ignite.logger.slf4j;

import java.io.File;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Testing that markers are supported by log4j2 implementation.
 */
public class Slf4jLoggerMarkerTest {
    /** Path to full log. */
    private static final String LOG_ALL = "work/log/all.log";

    /** Path to filtered log. */
    private static final String LOG_FILTERED = "work/log/filtered.log";

    /** */
    @Before
    public void setUp() {
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
        Slf4jLogger log = new Slf4jLogger(LoggerFactory.getLogger(Slf4jLoggerMarkerTest.class));

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

        assertTrue(all.contains("[IGNORE_ME] Ignored error"));
        assertTrue(all.contains("[IGNORE_ME] Ignored warning"));
        assertTrue(all.contains("[IGNORE_ME] Ignored info"));
        assertTrue(all.contains("[IGNORE_ME] Ignored debug"));
        assertTrue(all.contains("[IGNORE_ME] Ignored trace"));

        assertTrue(all.contains("[ACCEPT_ME] Accepted error"));
        assertTrue(all.contains("[ACCEPT_ME] Accepted warning"));
        assertTrue(all.contains("[ACCEPT_ME] Accepted info"));
        assertTrue(all.contains("[ACCEPT_ME] Accepted debug"));
        assertTrue(all.contains("[ACCEPT_ME] Accepted trace"));

        // check file with one marker filtered out
        File filteredFile = U.resolveIgnitePath(LOG_FILTERED);
        assertNotNull(filteredFile);
        String filtered = U.readFileToString(filteredFile.getPath(), "UTF-8");

        assertFalse(filtered.contains("[IGNORE_ME] Ignored error"));
        assertFalse(filtered.contains("[IGNORE_ME] Ignored warning"));
        assertFalse(filtered.contains("[IGNORE_ME] Ignored info"));
        assertFalse(filtered.contains("[IGNORE_ME] Ignored debug"));
        assertFalse(filtered.contains("[IGNORE_ME] Ignored trace"));

        assertTrue(filtered.contains("[ACCEPT_ME] Accepted error"));
        assertTrue(filtered.contains("[ACCEPT_ME] Accepted warning"));
        assertTrue(filtered.contains("[ACCEPT_ME] Accepted info"));
        assertTrue(filtered.contains("[ACCEPT_ME] Accepted debug"));
        assertTrue(filtered.contains("[ACCEPT_ME] Accepted trace"));
    }

    /** Delete existing logs, if any */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteLogs() {
        new File(LOG_ALL).delete();
        new File(LOG_FILTERED).delete();
    }
}
