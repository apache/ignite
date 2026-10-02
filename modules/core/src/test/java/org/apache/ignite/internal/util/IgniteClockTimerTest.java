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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that {@link U#currentTimeMillis()} keeps being updated when the test clock is used.
 */
public class IgniteClockTimerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks that the internal clock timer is not started while the test clock is used
     * and time is updated when no nodes are running.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInternalClockNotStartedWithTestClock() throws Exception {
        assertTrue(IgniteUtils.extClock);

        startGrid(0);

        assertFalse(internalClockRunning());

        stopAllGrids();

        assertClockAdvances();
    }

    /**
     * Emulates a node left running by a previous test before the test clock was started
     * (see IGNITE-29084): such a node must neither override the mocked time while it is alive
     * nor leave {@link U#currentTimeMillis()} frozen after it is stopped.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLeakedNodeDoesNotFreezeClock() throws Exception {
        synchronized (IgniteUtils.mux) {
            IgniteUtils.extClock = false;
        }

        try {
            // Node "leaked" by a previous test: it starts the internal clock timer.
            startGrid(0);

            assertTrue(internalClockRunning());

            // GridAbstractTest starts the test clock on class initialization regardless of running nodes.
            assertTrue(GridTestClockTimer.startTestTimer());

            IgniteUtils.useExternalClock();

            assertFalse(internalClockRunning());

            // The mocked time must not be overridden by the internal clock timer of the leaked node.
            long mockedTime = U.currentTimeMillis();

            GridTestClockTimer.timeSupplier(() -> mockedTime);

            try {
                // Several periods of the internal clock timer.
                doSleep(100);

                assertEquals(mockedTime, U.currentTimeMillis());
            }
            finally {
                GridTestClockTimer.timeSupplier(GridTestClockTimer.DFLT_TIME_SUPPLIER);
            }

            // Stopping the leaked node must not affect the time updates.
            stopAllGrids();

            assertClockAdvances();

            // Internal clock timer must not be started again.
            startGrid(0);

            assertFalse(internalClockRunning());
        }
        finally {
            synchronized (IgniteUtils.mux) {
                IgniteUtils.extClock = true;
            }
        }
    }

    /**
     * @return {@code True} if the internal clock timer is running.
     */
    private static boolean internalClockRunning() {
        synchronized (IgniteUtils.mux) {
            return GridTestUtils.getFieldValue(CommonUtils.class, "timer") != null;
        }
    }

    /**
     * Checks that {@link U#currentTimeMillis()} is updated.
     *
     * @throws Exception If failed.
     */
    private static void assertClockAdvances() throws Exception {
        long start = U.currentTimeMillis();

        // System time is used on purpose: waiting based on U.currentTimeMillis() hangs if the clock is frozen.
        long deadline = System.currentTimeMillis() + 5_000;

        while (U.currentTimeMillis() == start && System.currentTimeMillis() < deadline)
            Thread.sleep(10);

        assertTrue("U.currentTimeMillis() is not updated", U.currentTimeMillis() > start);
    }
}
