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

package org.apache.ignite.internal.ducktest.utils;

/** Accumulates latency of completed operations. */
public class OpStats {
    /** Sum of recorded latencies, nanoseconds. */
    private long totalNs;

    /** Minimum recorded latency, nanoseconds. */
    private long minNs = Long.MAX_VALUE;

    /** Maximum recorded latency, nanoseconds. */
    private long maxNs;

    /** Number of recorded operations. */
    private long cnt;

    /** Records a single operation latency. */
    public void record(long ns) {
        totalNs += ns;

        minNs = Math.min(minNs, ns);
        maxNs = Math.max(maxNs, ns);

        cnt++;
    }

    /** @return Minimum latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public long minNs() {
        return cnt > 0 ? minNs : -1;
    }

    /** @return Maximum latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public long maxNs() {
        return cnt > 0 ? maxNs : -1;
    }

    /** @return Average latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public double avgNs() {
        return cnt > 0 ? totalNs / (double)cnt : -1;
    }

    /**
     * @return Derived throughput: recorded operations per second of pure operation time,
     * or {@code -1} if nothing was recorded.
     */
    public double tps() {
        return totalNs > 0 ? cnt * 1e9 / totalNs : -1;
    }
}
