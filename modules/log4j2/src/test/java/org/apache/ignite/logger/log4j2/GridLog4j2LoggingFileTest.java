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
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Grid Log4j SPI test.
 */
public class GridLog4j2LoggingFileTest extends TestCase {
    /** */
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        

        File xml = GridTestUtils
                .resolveIgnitePath("modules/core/src/test/config/log4j2-test.xml");

        assert xml != null;
        assert xml.exists() == true;

        log = new Log4J2Logger(xml).getLogger(getClass());
        ((LoggerNodeIdAware) log).setNodeId(UUID.randomUUID());

    }

    /**
     * Tests log4j logging SPI.
     */
    @Test
    public void testLog() {
        assert log.isDebugEnabled() == true;
        assert log.isInfoEnabled() == true;

        log.debug("This is 'debug' message.");
        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception(
                "It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception(
                "It's a test error exception"));
    }

}