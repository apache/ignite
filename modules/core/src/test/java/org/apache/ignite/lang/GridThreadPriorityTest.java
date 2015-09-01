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

package org.apache.ignite.lang;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Run with: -server -XX:+UseThreadPriorities -XX:ThreadPriorityPolicy=42
 */
public class GridThreadPriorityTest {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        AtomicBoolean finish = new AtomicBoolean();

        for (int j = 0; j < Runtime.getRuntime().availableProcessors(); j++) {
            for (int i = Thread.MIN_PRIORITY; i <= Thread.MAX_PRIORITY; i++)
                new TestThread(finish, i).start();
        }

        Thread.sleep(30 * 1000);

        finish.set(true);
    }

    /**
     *
     */
    private static class TestThread extends Thread {
        /** */
        private final AtomicBoolean finish;

        /** */
        private long i;

        /**
         * @param finish Finish.
         * @param prio Priority.
         */
        private TestThread(AtomicBoolean finish, int prio) {
            this.finish = finish;

            setPriority(prio);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!finish.get())
                i++;

            System.out.println("Thread finished [id=" + getId() + ", prio=" + getPriority() + ", i=" +
                i / 1_000_000_000d + ']');
        }
    }
}