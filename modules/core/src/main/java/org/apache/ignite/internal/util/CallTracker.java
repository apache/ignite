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
import java.util.concurrent.atomic.AtomicInteger;
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
    private final Map<Integer, Stats> traces = new ConcurrentHashMap<>();

    /**
     * @param name Name.
     */
    public static CallTracker named(String name) {
        return NAMED_INSTANCES.computeIfAbsent(name, n -> new CallTracker());
    }

    /**
     *
     */
    public Track track() {
        Thread caller = Thread.currentThread();

        StackTraceElement[] trace = caller.getStackTrace();

        Stats stats = traces.compute(Arrays.hashCode(trace), (hc, s) -> {
            if (s == null)
                s = new Stats(toString(trace));

            s.totalCalls++;

            s.callsPerThread.compute(caller.getName(), (tn, ts) -> {
                if (ts == null)
                    ts = new AtomicInteger(1);
                else
                    ts.incrementAndGet();

                return ts;
            });

            return s;
        });

        return new Track(caller.getName(), stats.trace);
    }

    /**
     *
     */
    public Collection<Stats> allTraces() {
        return traces.values();
    }

    /**
     *
     */
    public static String toStringAll() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, CallTracker> entry : NAMED_INSTANCES.entrySet()) {
            Collection<Stats> allStats = entry.getValue().allTraces();

            sb.append(entry.getKey())
                .append(" [traces: ").append(allStats.size()).append(']').append(NL);

            int n = 0;

            for (Stats stats : allStats) {
                sb.append('#').append(++n).append(": ").append(stats.totalCalls).append(NL);

                stats.callsPerThread.forEach((name, calls) ->
                    sb.append("  ").append(name).append(": ").append(calls).append(NL));

                sb.append(stats.trace);
            }
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

    /**
     *
     */
    public static class Track {
        /** */
        private final String threadName;
        /** */
        private final String trace;

        /**
         * @param threadName Thread name.
         * @param trace Trace.
         */
        public Track(String threadName, String trace) {
            this.threadName = threadName;
            this.trace = trace;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return threadName + NL + trace;
        }
    }
    /**
     *
     */
    private static class Stats {
        /** */
        volatile int totalCalls;
        /** */
        final Map<String, AtomicInteger> callsPerThread = new ConcurrentHashMap<>();
        /** */
        final String trace;

        /**
         * @param trace Trace.
         */
        Stats(String trace) {
            this.trace = trace;
        }
    }
}
