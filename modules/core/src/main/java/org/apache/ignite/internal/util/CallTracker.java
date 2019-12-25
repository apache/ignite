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

package org.apache.ignite.internal.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Call tracker.
 */
public class CallTracker {
    /** */
    private static final Map<String, CallTracker> NAMED_INSTANCES = new ConcurrentHashMap<>();

    /** */
    private static final String NL = U.nl();

    /** */
    private final Map<Integer, String> traces = new ConcurrentHashMap<>();

    /**
     * @param name Name.
     */
    public static CallTracker named(String name) {
        return NAMED_INSTANCES.computeIfAbsent(name, n -> new CallTracker());
    }

    /**
     *
     */
    public String track() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();

        return traces.computeIfAbsent(Arrays.hashCode(trace), t -> toString(trace));
    }

    /**
     *
     */
    public Collection<String> allTraces() {
        return traces.values();
    }

    /**
     *
     */
    public static String toStringAll() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, CallTracker> entry : NAMED_INSTANCES.entrySet()) {
            Collection<String> traces = entry.getValue().allTraces();

            sb.append(entry.getKey())
                .append(" [traces: ").append(traces.size()).append(']').append(NL);

            int n = 0;

            for (String trace : traces)
                sb.append('#').append(++n).append(':').append(NL).append(trace);
        }

        return sb.toString();
    }

    /**
     * @param trace Trace.
     */
    private String toString(StackTraceElement[] trace) {
        StringBuilder sb = new StringBuilder((trace.length - 2) * 128);

        for (int i = 2; i < trace.length; i++)
            sb.append("  ").append(trace[i]).append(NL);

        return sb.toString();
    }
}
