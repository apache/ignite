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
import com.fasterxml.jackson.databind.JsonNode;

/** Small helpers shared by the ducktest applications. */
public final class Utils {
    /** */
    private Utils() {
        // No-op.
    }

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

    /**
     * Parses an optional enum-valued field. A missing, null or unrecognized value falls back
     * to {@code dfltVal}.
     */
    public static <E extends Enum<E>> E getEnum(JsonNode jNode, String fieldName, E dfltVal) {
        JsonNode field = jNode.path(fieldName);

        if (field.isMissingNode() || field.isNull())
            return dfltVal;

        try {
            return Enum.valueOf(dfltVal.getDeclaringClass(), field.asText().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e) {
            return dfltVal; // Fallback if string doesn't match any enum constant.
        }
    }

    /**
     * Parses a required enum-valued field. Throws if the field is missing, null or not a valid
     * constant of {@code cls} (case-insensitively) - a typo must fail the run, not pick a default.
     */
    public static <E extends Enum<E>> E getEnum(JsonNode jNode, String fieldName, Class<E> cls) {
        JsonNode field = jNode.path(fieldName);

        if (field.isMissingNode() || field.isNull())
            throw new IllegalArgumentException("Missing required enum field '" + fieldName + "'");

        return Enum.valueOf(cls, field.asText().toUpperCase(Locale.ROOT));
    }
}
