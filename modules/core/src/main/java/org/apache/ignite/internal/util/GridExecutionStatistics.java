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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jsr166.ConcurrentHashMap8;

/**
 * Accumulates execution statistics for named pieces of code.
 */
public class GridExecutionStatistics {
    /** */
    private String name;

    /** Map of execution counters. */
    private ConcurrentMap<String, AtomicInteger> cntMap = new ConcurrentHashMap8<>();

    /** Map of execution durations. */
    private ConcurrentMap<String, AtomicLong> durationMap = new ConcurrentHashMap8<>();

    /** Execution start time for the current thread. */
    private ThreadLocal<IgniteBiTuple<String, Long>> startTime = new ThreadLocal<IgniteBiTuple<String, Long>>() {
        @Override protected IgniteBiTuple<String, Long> initialValue() {
            return F.t(null, 0L);
        }
    };

    /**
     * @param name Statistics name.
     */
    public GridExecutionStatistics(String name) {
        this.name = name;
    }

    /**
     * @param name Execution name.
     */
    public void watchExecution(String name) {
        assert name != null;

        startTime.get().put(name, U.currentTimeMillis());
    }

    /**
     * Stop watching execution started previously in the same thread.
     */
    public void stopWatching() {
        String name = startTime.get().get1();

        long time = startTime.get().get2();

        AtomicInteger cnt = F.addIfAbsent(cntMap, name, F.newAtomicInt());

        assert cnt != null;

        cnt.incrementAndGet();

        AtomicLong d = F.addIfAbsent(durationMap, name, F.newAtomicLong());

        assert d != null;

        d.addAndGet(U.currentTimeMillis() - time);
    }

    /**
     * Prints statistics.
     */
    public void print() {
        X.println("*** Execution statistics: " + name);

        for (Map.Entry<String, AtomicInteger> e : cntMap.entrySet()) {
            int cnt = e.getValue().get();

            assert cnt > 0;

            long totalDuration = durationMap.get(e.getKey()).get();

            long avgDuration = totalDuration / cnt;

            X.println("\t" + e.getKey() + "->[executions=" + cnt +
                ", average duration=" + avgDuration + ", total duration=" + totalDuration + "]");
        }
    }
}