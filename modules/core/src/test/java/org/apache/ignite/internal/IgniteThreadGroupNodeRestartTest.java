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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.Collectors.joining;

/**
 *
 */
public class IgniteThreadGroupNodeRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNodeRestartInsideThreadGroup() throws Exception {
        ThreadGroup tg = new ThreadGroup("test group");

        AtomicReference<Exception> err = new AtomicReference<>();

        Thread t = new Thread(tg, () -> {
            try {
                startStopGrid(0);
            }
            catch (Exception e) {
                err.set(e);
            }
        });

        t.start();
        t.join(getTestTimeout());
        assertFalse(t.isAlive());

        if (err.get() != null)
            throw err.get();

        destroyWithWaitAllThreadIsComplete(tg, getTestTimeout());

        startStopGrid(0);
    }

    /** */
    private void startStopGrid(int idx) throws Exception {
        startGrid(idx);
        stopGrid(idx);
    }

    /** */
    private static void destroyWithWaitAllThreadIsComplete(ThreadGroup tg, long millis) throws Exception {
        if (GridTestUtils.waitForCondition(() -> tg.activeCount() == 0, millis, 10) || tg.activeCount() == 0) {
            tg.destroy();

            return;
        }

        Thread[] threads = new Thread[tg.activeCount() + 256];
        int copied = tg.enumerate(threads, true);

        if (copied == 0) {
            tg.destroy();

            return;
        }

        fail(String.format(
            "Thread group still has active threads: [count=%s, threads=%s]",
            copied, threadInfos(threads)
        ));
    }

    /** */
    private static String threadInfos(Thread... threads) {
        return Arrays.stream(threads)
            .filter(Objects::nonNull)
            .map(IgniteThreadGroupNodeRestartTest::threadInfo)
            .collect(joining(", ", "[", "]"));
    }

    /** */
    private static String threadInfo(Thread t) {
        return String.format(
            "[id=%s, name=%s, alive=%s, deamon=%s, state=%s]",
            t.getId(), t.getName(), t.isAlive(), t.isDaemon(), t.getState()
        );
    }
}
