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

package org.apache.ignite.util.mbeans;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * {@link WorkersControlMXBean} test.
 */
public class WorkersControlMXBeanTest extends GridCommonAbstractTest {
    /** Test thread name. */
    private static final String TEST_THREAD_NAME = "test-thread";

    /**
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testStopThreadByUniqueName() throws Exception {
        WorkersControlMXBean workersCtrlMXBean = new WorkersControlMXBeanImpl(null);

        Thread t = startTestThread();

        assertTrue(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        t.join(500);

        assertFalse(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        Thread t1 = startTestThread();
        Thread t2 = startTestThread();

        assertFalse(workersCtrlMXBean.stopThreadByUniqueName(TEST_THREAD_NAME));

        t1.stop();
        t2.stop();
    }

    /**
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testStopThreadById() throws Exception {
        WorkersControlMXBean workersCtrlMXBean = new WorkersControlMXBeanImpl(null);

        Thread t1 = startTestThread();
        Thread t2 = startTestThread();

        assertTrue(workersCtrlMXBean.stopThreadById(t1.getId()));
        assertTrue(workersCtrlMXBean.stopThreadById(t2.getId()));

        t1.join(500);
        t2.join(500);

        assertFalse(workersCtrlMXBean.stopThreadById(t1.getId()));
        assertFalse(workersCtrlMXBean.stopThreadById(t2.getId()));
    }

    /**
     * @return Started thread.
     */
    private static Thread startTestThread() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(TEST_THREAD_NAME) {
            @Override public void run() {
                latch.countDown();

                for (;;)
                    ;
            }
        };

        t.start();

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS));

        assertTrue(t.isAlive());

        return t;
    }
}
