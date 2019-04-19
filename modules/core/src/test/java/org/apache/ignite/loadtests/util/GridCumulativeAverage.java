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

package org.apache.ignite.loadtests.util;

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