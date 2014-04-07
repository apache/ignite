/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.streamer.average;

/**
 * Average helper class.
 */
class TestAverage {
    /** */
    private int total;

    /** */
    private int cnt;

    /**
     * @param avg Average.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void increment(TestAverage avg) {
        int total;
        int cnt;

        synchronized (avg) {
            total = avg.total;
            cnt = avg.cnt;
        }

        increment(total, cnt);
    }

    /**
     * @param total Increment total.
     * @param cnt Increment count.
     */
    public synchronized void increment(int total, int cnt) {
        this.total += total;
        this.cnt += cnt;
    }

    /**
     * @param total Total.
     * @param cnt Count.
     */
    public synchronized void set(int total, int cnt) {
        this.total = total;
        this.cnt = cnt;
    }

    /**
     * @return Running average.
     */
    public synchronized double average() {
        return (double)total / cnt;
    }
}
