/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.logger.log4j;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Log4j not initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jNotInitializedTest {
    /** */
    @Test
    public void testLogInitialize() {
        IgniteLogger log = new Log4JLogger().getLogger(GridLog4jNotInitializedTest.class);

        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4JLogger"));
        assertTrue(log.toString().contains("config=null"));

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");
        else
            System.out.println("DEBUG level is not enabled.");

        if (log.isInfoEnabled())
            log.info("This is 'info' message.");
        else
            System.out.println("INFO level is not enabled.");

        log.warning("This is 'warning' message.");
        log.error("This is 'error' message.");
    }
}
