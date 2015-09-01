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

import java.net.URL;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Log4j initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4j2InitializedTest extends TestCase {

    /**
     * @throws Exception If failed.
     */
    @Override
    protected void setUp() throws Exception {

    }

    /** */
    public void testLogInitialize() {

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid" + 1);
        cfg.setNodeId(new UUID(1, 1));
        // cfg.setIgniteHome("/home/glutters/Documenti/apache-ignite/ignite-master/ignite/");

        URL xml = U.resolveIgniteUrl("config/ignite-log4j2.xml");
        IgniteLogger log;
        try {

            log = new Log4J2Logger(xml);
            // log.isQuiet();
            cfg.setGridLogger(log);
        } catch (IgniteCheckedException e) {
            e.printStackTrace();
            return;
        }

        assert log.isInfoEnabled() == true;

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception(
                "It's a test warning exception"));
        log.error("This is 'error' message.");

        assert log.getLogger(GridLog4j2InitializedTest.class.getName()) instanceof Log4J2Logger;
    }

}