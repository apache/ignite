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

import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class StripedExecutorTest extends GridCommonAbstractTest {
    /** */
    private StripedExecutor stripedExecSvc;

    /** {@inheritDoc} */
    @Override public void beforeTest() {
        stripedExecSvc = new StripedExecutor(3, "foo name", "pool name", new JavaLogger());
    }

    /** {@inheritDoc} */
    @Override public void afterTest() {
        stripedExecSvc.shutdown();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompletedTasks() throws Exception {
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable());

        sleepASec();

        assertEquals(2, stripedExecSvc.completedTasks());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStripesCompletedTasks() throws Exception {
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable());

        sleepASec();

        long[] completedTaks = stripedExecSvc.stripesCompletedTasks();

        assertEquals(1, completedTaks[0]);
        assertEquals(1, completedTaks[1]);
        assertEquals(0, completedTaks[2]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStripesActiveStatuses() throws Exception {
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));

        sleepASec();

        boolean[] statuses = stripedExecSvc.stripesActiveStatuses();

        assertFalse(statuses[0]);
        assertTrue(statuses[1]);
        assertFalse(statuses[0]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActiveStripesCount() throws Exception {
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));

        sleepASec();

        assertEquals(1, stripedExecSvc.activeStripesCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStripesQueueSizes() throws Exception {
        stripedExecSvc.execute(0, new TestRunnable());
        stripedExecSvc.execute(0, new TestRunnable(true));
        stripedExecSvc.execute(0, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));

        sleepASec();

        int[] queueSizes = stripedExecSvc.stripesQueueSizes();

        assertEquals(1, queueSizes[0]);
        assertEquals(2, queueSizes[1]);
        assertEquals(0, queueSizes[2]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueSize() throws Exception {
        stripedExecSvc.execute(1, new TestRunnable());
        stripedExecSvc.execute(1, new TestRunnable(true));
        stripedExecSvc.execute(1, new TestRunnable(true));

        sleepASec();

        assertEquals(1, stripedExecSvc.queueSize());
    }

    /**
     *
     */
    private final class TestRunnable implements Runnable {
        /** */
        private final boolean infinitely;

        /**
         *
         */
        public TestRunnable() {
            this(false);
        }

        /**
         * @param infinitely {@code True} if should sleep infinitely.
         */
        public TestRunnable(boolean infinitely) {
            this.infinitely = infinitely;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (infinitely)
                    sleepASec();
            }
            catch (InterruptedException e) {
                info("Got interrupted exception while sleeping: " + e);
            }
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private void sleepASec() throws InterruptedException {
        Thread.sleep(1000);
    }
}