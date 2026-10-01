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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that {@link U#currentTimeMillis()} keeps being updated when the test clock is used.
 */
public class IgniteClockTimerTest extends GridCommonAbstractTest {
    /** Name prefix of the internal clock timer thread started by {@link IgniteUtils#onGridStart(String)}. */
    private static final String INTERNAL_CLOCK_THREAD_PREFIX = "ignite-clock-#";

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
     * (see IGNITE-29084): stopping such a node must not leave {@link U#currentTimeMillis()} frozen.
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

            // The same steps GridAbstractTest performs on class initialization.
            assertTrue(GridTestClockTimer.startTestTimer());

            new GridTestClockTimer();

            assertFalse(internalClockRunning());

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
     * @return {@code True} if the internal clock timer thread is running.
     */
    private static boolean internalClockRunning() {
        return Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.isAlive() && t.getName().startsWith(INTERNAL_CLOCK_THREAD_PREFIX));
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
