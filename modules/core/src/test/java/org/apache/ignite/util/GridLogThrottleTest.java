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

package org.apache.ignite.util;

import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid log throttle test. To verify correctness, you need to run this test
 * and check that all messages that should be logged are indeed logged and
 * all messages that should be omitted are indeed omitted.
 */
@GridCommonTest(group = "Utils")
public class GridLogThrottleTest extends GridCommonAbstractTest {
    /** Constructor. */
    public GridLogThrottleTest() {
        super(false);
    }

    /**
     * Tests throttle.
     *
     * @throws Exception If any error occurs.
     */
    public void testThrottle() throws Exception {
        LT.throttleTimeout(1000);

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test1");

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 2."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test1");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test3");

        // LOGGED.
        LT.error(log, null, "Test - without throwable.");

        // OMITTED.
        LT.error(log, null, "Test - without throwable.");

        // LOGGED.
        LT.warn(log, null, "Test - without throwable1.");

        // OMITTED.
        LT.warn(log, null, "Test - without throwable1.");

        Thread.sleep(LT.throttleTimeout());

        info("Slept for throttle timeout: " + LT.throttleTimeout());

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test1");

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 2."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test1");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test3");

        Thread.sleep(LT.throttleTimeout());

        info("Slept for throttle timeout: " + LT.throttleTimeout());

        //LOGGED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");
    }
}