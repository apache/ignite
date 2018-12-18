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
import java.net.URL;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Grid Log4j SPI test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jLoggingUrlTest {
    /** */
    private IgniteLogger log;

    /** Logger config */
    private URL url;

    /** TODO IGNITE-10177 add annotation @Before here. */
    private void setUp() throws Exception {
        File xml = GridTestUtils.resolveIgnitePath("modules/core/src/test/config/log4j-test.xml");

        assert xml != null;
        assert xml.exists();

        url = xml.toURI().toURL();
        log = new Log4JLogger(url).getLogger(getClass());
    }

    /**
     * Tests log4j logging SPI.
     */
    @Test
    public void testLog() throws Exception {
        setUp();

        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4JLogger"));
        assertTrue(log.toString().contains(url.getPath()));

        assertTrue(log.isInfoEnabled());

        log.debug("This is 'debug' message.");
        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }
}
