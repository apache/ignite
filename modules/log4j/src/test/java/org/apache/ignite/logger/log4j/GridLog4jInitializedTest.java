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
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.log4j.BasicConfigurator;

/**
 * Log4j initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jInitializedTest extends TestCase {

    /**
     * @throws Exception If failed.
     */
    @Override protected void setUp() throws Exception {
        BasicConfigurator.configure();
    }

    /** */
    public void testLogInitialize() {
        IgniteLogger log = new Log4JLogger();

        assert log.isInfoEnabled() == true;

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(GridLog4jInitializedTest.class.getName()) instanceof Log4JLogger;
    }
}