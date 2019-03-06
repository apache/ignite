/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.logger.log4j;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Grid Log4j SPI test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jLoggingPathTest {
    /** */
    private IgniteLogger log;

    /** Logger config */
    private String path = "modules/core/src/test/config/log4j-test.xml";

    /** */
    @Before
    public void setUp() throws Exception {
        log = new Log4JLogger(path).getLogger(getClass());
    }

    /**
     * Tests log4j logging SPI.
     */
    @Test
    public void testLog() {
        System.out.println(log.toString());

        assertTrue(log.toString().contains("Log4JLogger"));
        assertTrue(log.toString().contains(path));

        assertTrue(log.isInfoEnabled());

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }
}
