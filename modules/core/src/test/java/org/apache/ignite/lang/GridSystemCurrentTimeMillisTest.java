/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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