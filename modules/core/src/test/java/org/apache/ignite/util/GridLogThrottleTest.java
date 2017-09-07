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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid log throttle test. To verify correctness, you need to run this test
 * and check that all messages that should be logged are indeed logged and
 * all messages that should be omitted are indeed omitted.
 * Also test checks that outdated entries are removed (compares entry timestamp with current
 * timestamp minus timeout). It protects the map from causing memory leak.
 */
@GridCommonTest(group = "Utils")
public class GridLogThrottleTest extends GridCommonAbstractTest {

    @LoggerResource
    private IgniteLogger log;

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

        // LOGGED.
        LT.error(log, null, "Test - without throwable.");

        // OMITTED.
        LT.error(log, null, "Test - without throwable.");

        // OMITTED.
        LT.warn(log, "Test - without throwable.");

        // LOGGED.
        LT.warn(log, "Test - without throwable1.");

        // OMITTED.
        LT.warn(log, "Test - without throwable1.");

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

        // LOGGED.
        LT.warn(log, "Test - without throwable.");

        // OMITTED.
        LT.warn(log, "Test - without throwable.");

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

    /**
     * Test cleans up old entries from map.
     *
     * @throws Exception If any error occurs.
     */
    public void testCleanUpOldEntries() throws Exception {
        final GridStringLogger log = new GridStringLogger(false, this.log);

        LT.throttleTimeout(1000);

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        System.out.println("Test exception 1." + U.currentTimeMillis());
        assertTrue(log.toString().contains("Test\r\njava.lang.RuntimeException: Test exception 1."));
        assertEquals(1, LT.errorsSize());
        log.reset();

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        assertEquals(log.toString(), "");
        assertEquals(1, LT.errorsSize());
        log.reset();

        LT.error(log, new RuntimeException("Test exception 2."), "Test");
        assertTrue(log.toString().contains("Test\r\njava.lang.RuntimeException: Test exception 2."));
        assertEquals(2, LT.errorsSize());
        log.reset();

        LT.error(log, null, "Test - without throwable.");
        assertTrue(log.toString().contains("Test - without throwable."));
        assertEquals(3, LT.errorsSize());
        log.reset();

        LT.warn(log, "Test - without throwable1.");
        assertTrue(log.toString().contains("Test - without throwable1."));
        assertEquals(4, LT.errorsSize());
        log.reset();

        info("Slept for throttle timeout: " + LT.throttleTimeout() + 50);
        Thread.sleep(LT.throttleTimeout() + 50);

        LT.error(log, new RuntimeException("Test exception 3."), "Test");
        assertTrue(log.toString().contains("Test\r\njava.lang.RuntimeException: Test exception 3."));
        assertEquals(1, LT.errorsSize());
        log.reset();

        LT.warn(log, "Test - without throwable.");
        assertEquals(2, LT.errorsSize());
        assertTrue(log.toString().contains("Test - without throwable."));
        log.reset();

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        assertEquals(3, LT.errorsSize());
        assertTrue(log.toString().contains("Test\r\njava.lang.RuntimeException: Test exception 1."));
        log.reset();

        info("Slept for throttle timeout: " + LT.throttleTimeout());
        Thread.sleep(LT.throttleTimeout());

        info("Slept for throttle timeout: " + LT.throttleTimeout());
        Thread.sleep(LT.throttleTimeout());

        assertEquals(0, LT.errorsSize());
    }

    /**
     * Test check changing throttleTimeout.
     *
     * @throws Exception If any error occurs.
     */
    public void testOnChangingTimeout() throws Exception {
        final GridStringLogger log = new GridStringLogger(false, this.log);

        LT.throttleTimeout(1000);

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        assertEquals(1, LT.errorsSize());

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        assertEquals(1, LT.errorsSize());

        info("Slept for throttle timeout: " + (LT.throttleTimeout() + 50));
        Thread.sleep(LT.throttleTimeout() + 50);

        assertEquals(0, LT.errorsSize());

        LT.throttleTimeout(500);

        LT.error(log, new RuntimeException("Test exception 1."), "Test");
        assertEquals(1, LT.errorsSize());

        info("Set new throttle timeout and slept : " + LT.throttleTimeout() + 50);
        Thread.sleep(LT.throttleTimeout() + 50);

        assertEquals(0, LT.errorsSize());
    }
}