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

/**
 *
 */
public class GridSystemCurrentTimeMillisTest {
    /** */
    private static long time;

    /**
     *
     */
    private GridSystemCurrentTimeMillisTest() {
        // No-op.
    }

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        new Thread(new Timer()).start();

        new Thread(new Client()).start();
        new Thread(new Client()).start();
        new Thread(new Client()).start();
        new Thread(new Client()).start();
    }

    /**
     *
     */
    private static class Timer implements Runnable {
        @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
        @Override public void run() {
            while (true) {
                time = System.currentTimeMillis();

                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     */
    private static class Client implements Runnable {
        @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
        @Override public void run() {
            int readsCnt = 0;

            int staleReadsCnt = 0;

            long lastVal = -1;

            while (true) {
                try {
                    Thread.sleep(15);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                readsCnt++;

                long time0 = time;

                if (lastVal == time0)
                    staleReadsCnt++;

                lastVal = time0;

                if (readsCnt % 500 == 0)
                    System.out.println("Stats [thread=" + Thread.currentThread().getId() + ", reads=" + readsCnt +
                        ", staleReadsCnt=" + staleReadsCnt + ']');

                try {
                    Thread.sleep(40);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}