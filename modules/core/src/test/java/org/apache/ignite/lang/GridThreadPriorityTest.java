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