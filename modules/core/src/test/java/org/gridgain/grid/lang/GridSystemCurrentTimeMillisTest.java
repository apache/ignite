/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

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
