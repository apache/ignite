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
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Grid log throttle test. To verify correctness, you need to run this test
 * and check that all messages that should be logged are indeed logged and
 * all messages that should be omitted are indeed omitted.
 */
@GridCommonTest(group = "Utils")
@RunWith(JUnit4.class)
public class GridLogThrottleTest extends GridCommonAbstractTest {
    /** */
    private final GridStringLogger log0 = new GridStringLogger(false, this.log);

    /** Constructor. */
    public GridLogThrottleTest() {
        super(false);
    }

    /**
     * Tests throttle.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testThrottle() throws Exception {
        LT.throttleTimeout(1000);

        String sep = System.getProperty("line.separator");

        checkError("Test exception 1.", "Test msg", true);
        checkError("Test exception 1.", "Test msg", false);

        checkError("Test exception 2.", "Test msg", true);

        checkErrorNoThrowable("Test - without throwable.", true);
        checkErrorNoThrowable("Test - without throwable.", false);
        checkWarn("Test - without throwable.", false);

        checkWarn("Test - without throwable 1.", true);
        checkWarn("Test - without throwable 1.", false);

        Thread.sleep(LT.throttleTimeout() * 2);
        info("Slept for throttle timeout: " + LT.throttleTimeout() * 2);

        checkError("Test exception 1.", "Test msg", true);
        checkError("Test exception 1.", "Test msg", false);
        checkError("Test exception 1.", "Test msg1", false);

        checkError("Test exception 2.", "Test msg", true);

        checkWarn("Test - without throwable.", true);
        checkWarn("Test - without throwable.", false);

        LT.throttleTimeout(200);

        Thread.sleep(LT.throttleTimeout() * 2);
        info("Slept for throttle timeout: " + LT.throttleTimeout() * 2);

        checkInfo("Test info message.", true);
        checkInfo("Test info message.", false);

        for (int i = 1; i <= LT.throttleCapacity(); i++)
            checkInfo("Test info message " + i, true);

        checkInfo("Test info message.", true);
    }

    /**
     * @param eMsg Exception message.
     * @param msg Log message.
     * @param isLogExpected Is log expected or not.
     */
    private void checkError(String eMsg, String msg, boolean isLogExpected) {
        Exception e = eMsg != null ? new RuntimeException(eMsg) : null;

        LT.error(log0, e, msg);

        check(e, msg, isLogExpected);
    }

    /**
     * @param msg Log message.
     * @param isLogExpected Is log expected or not.
     */
    private void checkErrorNoThrowable(String msg, boolean isLogExpected) {
        LT.error(log0, null, msg);

        check(null, msg, isLogExpected);
    }

    /**
     * @param msg Log message.
     * @param isLogExpected Is log expected or not.
     */
    private void checkWarn(String msg, boolean isLogExpected) {
        LT.warn(log0, msg);

        check(null, msg, isLogExpected);
    }

    /**
     * @param msg Log message.
     * @param isLogExpected Is log expected or not.
     */
    private void checkInfo(String msg, boolean isLogExpected) {
        LT.info(log0, msg);

        check(null, msg, isLogExpected);
    }

    /**
     * @param e Exception.
     * @param msg Log message.
     * @param isLogExpected Is log expected or not.
     */
    private void check(Exception e, String msg, boolean isLogExpected) {
        String sep = System.getProperty("line.separator");

        if (isLogExpected) {
            String s = msg;

            if (e != null)
                s += sep + "java.lang.RuntimeException: " + e.getMessage();

            assertTrue(log0.toString().contains(s));
        } else
            assertEquals(log0.toString(), "");

        log0.reset();
    }
}
