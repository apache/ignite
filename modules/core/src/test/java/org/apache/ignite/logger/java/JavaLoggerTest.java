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

package org.apache.ignite.logger.java;

import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Java logger test.
 */
@GridCommonTest(group = "Logger")
public class JavaLoggerTest extends TestCase {
    /** */
    @SuppressWarnings({"FieldCanBeLocal"})
    private IgniteLogger log;

    /**
     * @throws Exception If failed.
     */
    public void testLogInitialize() throws Exception {
        U.setWorkDirectory(null, U.getIgniteHome());

        log = new JavaLogger();

        ((LoggerNodeIdAware)log).setNodeId(UUID.fromString("00000000-1111-2222-3333-444444444444"));

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        assert log.isInfoEnabled();

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(JavaLoggerTest.class.getName()) instanceof JavaLogger;

        assert log.fileName() != null;

        // Ensure we don't get pattern, only actual file name is allowed here.
        assert !log.fileName().contains("%");
    }
}