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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** Cache continuous query statistics helper. */
public class CacheContinuousQueryStatisticsHelper {
    /** Current cuery statistics. */
    private static final ThreadLocal<StatisticsHolder> CUR_QRY_STATS = new ThreadLocal<>();

    /** Starts gathering statistics. */
    public static void startGatheringStatistics() {
        StatisticsHolder parent = CUR_QRY_STATS.get();

        CUR_QRY_STATS.set(new StatisticsHolder(parent));
    }

    /** Finishes gathering statistics. */
    public static StatisticsHolder finishGatheringStatistics() {
        StatisticsHolder curr = CUR_QRY_STATS.get();

        long duration = System.nanoTime() - curr.startTimeNanos;

        curr.duration += duration;

        StatisticsHolder parent = curr.parent;

        if (parent == null)
            CUR_QRY_STATS.remove();
        else {
            parent.duration -= duration;

            CUR_QRY_STATS.set(parent);
        }

        return curr;
    }

    /** Statistics holder. */
    public static class StatisticsHolder {
        /** Parent holder. */
        @Nullable private final StatisticsHolder parent;

        /** Start time. */
        private final long startTime;

        /** Start time in nanoseconds to measure duration. */
        private final long startTimeNanos;

        /** Duration. */
        private long duration;

        /** @param parent Parent holder. */
        private StatisticsHolder(@Nullable StatisticsHolder parent) {
            startTime = U.currentTimeMillis();
            startTimeNanos = System.nanoTime();
            this.parent = parent;
        }

        /** @return Start time. */
        public long startTime() {
            return startTime;
        }

        /** @return Duration. */
        public long duration() {
            return duration;
        }
    }
}