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

package org.apache.ignite.internal.tx;

import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.NotNull;

/**
 * The timestamp.
 */
public class Timestamp implements Comparable<Timestamp> {
    /**
     *
     */
    private static long localTime;

    /**
     *
     */
    private static long cntr;

    /**
     *
     */
    private final long timestamp;

    /**
     * @param timestamp The timestamp.
     */
    Timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @param other Other version.
     * @return Comparison result.
     */
    @Override
    public int compareTo(@NotNull Timestamp other) {
        int ret = Long.compare(timestamp >> 16 << 16, other.timestamp >> 16 << 16);

        return ret != 0 ? ret : Long.compare(timestamp << 48 >> 48, other.timestamp << 48 >> 48);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Timestamp)) {
            return false;
        }

        return compareTo((Timestamp) o) == 0;
    }

    @Override
    public int hashCode() {
        return (int) (timestamp ^ (timestamp >>> 32));
    }

    /**
     * TODO https://issues.apache.org/jira/browse/IGNITE-15129
     *
     * @return Next timestamp (monotonically increasing).
     */
    public static synchronized Timestamp nextVersion() {
        long localTimeCpy = localTime;

        // Truncate nanotime to 48 bits.
        localTime = Math.max(localTimeCpy, System.nanoTime() >> 16 << 16);

        if (localTimeCpy == localTime) {
            cntr++;
        } else {
            cntr = 0;
        }

        return new Timestamp(localTime | cntr);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Timestamp.class, this);
    }
}
