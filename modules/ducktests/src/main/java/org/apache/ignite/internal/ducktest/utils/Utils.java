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

import java.util.Locale;
import java.util.function.Supplier;

/** */
public final class Utils {
    /**
     * Runs the operation, records its latency into {@code stats} and returns its result.
     * If the operation throws, nothing is recorded and the exception propagates.
     */
    public static <T> T timed(OpStats stats, Supplier<T> op) {
        long startNs = System.nanoTime();

        T val = op.get();

        stats.record(System.nanoTime() - startNs);

        return val;
    }

    /**
     * Runs the void operation and records its latency into {@code stats}.
     * If the operation throws, nothing is recorded and the exception propagates.
     */
    public static void timed(OpStats stats, Runnable op) {
        timed(stats, () -> {
            op.run();

            return null;
        });
    }

    /** Formats a nanosecond latency as milliseconds with 3 decimal places. */
    public static String fmtMs(double ns) {
        return String.format(Locale.US, "%.3f", ns < 0 ? -1.0 : ns / 1e6);
    }
}
