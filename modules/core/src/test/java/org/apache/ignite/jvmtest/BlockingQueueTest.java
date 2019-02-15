/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jvmtest;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.GridTimer;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Blocking queue performance benchmark.
 */
public class BlockingQueueTest {
    /** Number of retries. */
    private static final int RETRIES = 3;

    /** Number of attempts. */
    private static final int CNT = 1000000;

    /** Number of threads. */
    private static final int THREAD_CNT = Runtime.getRuntime().availableProcessors();

    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < RETRIES; i++) {
            X.println(">>>");
            X.println(">>> Executing single threaded attempt: " + i);
            X.println(">>>");

            testBlockingQueue("single-threaded-linked-queue", new LinkedBlockingQueue<>());
            testBlockingQueue("single-threaded-linked-deque", new LinkedBlockingDeque<>());
            testBlockingQueue("single-threaded-array-queue", new ArrayBlockingQueue<>(CNT + 10));
        }

        for (int i = 0; i < RETRIES; i++) {
            X.println(">>>");
            X.println(">>> Executing multi-threaded attempt: " + i);
            X.println(">>>");

            testBlockingQueueMultithreaded("multi-threaded-linked-queue", new LinkedBlockingQueue<>());
            testBlockingQueueMultithreaded("multi-threaded-linked-deque", new LinkedBlockingDeque<>());
            testBlockingQueueMultithreaded("multi-threaded-array-queue", new ArrayBlockingQueue<>(
                THREAD_CNT * CNT + 100));
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private static void waitGc() throws InterruptedException {
        System.gc();
        System.gc();

        X.println(">>> Waiting for GC to complete...");

        Thread.sleep(1000);
    }

    /**
     * @param testName Test name.
     * @param q Queue to test.
     * @throws InterruptedException If interrupted.
     */
    private static void testBlockingQueue(String testName, BlockingQueue<Object> q) throws InterruptedException {
        waitGc();

        X.println(">>> Starting test for: " + testName);

        long dur = testBlockingQueue(q);

        X.println(">>> Tested queue [testName=" + testName + ", dur=" + dur + "ms]");

        assert q.isEmpty();
    }

    /**
     * @param testName Test name.
     * @param q Queue.
     * @throws Exception If failed.
     */
    private static void testBlockingQueueMultithreaded(String testName, final BlockingQueue<Object> q)
        throws Exception {
        waitGc();

        X.println(">>> Starting test for: " + testName);

        final LongAdder adder = new LongAdder();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                adder.add(testBlockingQueue(q));

                return null;
            }
        }, THREAD_CNT, "queue-test-worker");

        X.println(">>> Tested queue [testName=" + testName + ", dur=" + adder.sum() + "ms]");

        assert q.isEmpty();
    }

    /**
     * @param q Queue to test.
     * @throws InterruptedException If interrupted.
     */
    private static long testBlockingQueue(BlockingQueue<Object> q) throws InterruptedException {
        GridTimer timer = new GridTimer("blocking-queue");

        for (int i = 0; i < CNT; i++)
            q.put(new Object());

        for (int i = 0; i < CNT; i++) {
            Object o = q.take();

            assert o != null;
        }

        timer.stop();

        return timer.duration();
    }
}