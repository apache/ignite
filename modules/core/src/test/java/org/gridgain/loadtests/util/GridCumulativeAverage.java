/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.util;

/**
 * Counts the cumulative average as new data arrives.
 */
public class GridCumulativeAverage {
    /** Iteration number. */
    private int i;

    /** Current value. */
    private long cur;

    /**
     * Updates the current average and the counter, taking into account
     * the next coming value.
     *
     * @param nextVal The next value to recalculate the average with.
     */
    public void update(long nextVal) {
        cur = (nextVal + i * cur) / (i + 1);

        i++;
    }

    /**
     * @return The current average value.
     */
    public long get() {
        return cur;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return Long.toString(cur);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.valueOf(cur).hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return Long.valueOf(cur).equals(obj);
    }
}
