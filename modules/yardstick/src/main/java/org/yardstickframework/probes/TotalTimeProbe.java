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

package org.yardstickframework.probes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkExecutionAwareProbe;
import org.yardstickframework.BenchmarkProbePoint;
import org.yardstickframework.BenchmarkTotalsOnlyProbe;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Probe that measures total execution time of action for each thread.
 */
public class TotalTimeProbe implements BenchmarkExecutionAwareProbe, BenchmarkTotalsOnlyProbe {
    /** Thread local agents. */
    private volatile ThreadAgent[] agents;

    /** Benchmark configuration. */
    private BenchmarkConfiguration cfg;

    /** {@inheritDoc} */
    @Override public void beforeExecute(int threadIdx) {
        agents[threadIdx].before();
    }

    /** {@inheritDoc} */
    @Override public void afterExecute(int threadIdx) {
        agents[threadIdx].after();
    }

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) throws Exception {
        this.cfg = cfg;

        agents = new ThreadAgent[cfg.threads()];

        for (int thId = 0; thId < agents.length; thId++)
            agents[thId] = new ThreadAgent();

        println(cfg, "TotalTime probe started");
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        println(cfg, "TotalTime probe stopped");
    }

    /**
     * This probe producing two-dimension points:
     * Number of thread that executed {@code test} method and
     * time it took to finish.
     *
     * @return meta information about each point.
     */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList("Thread ID", "Duration, ms");
    }

    /** {@inheritDoc} */
    @Override public Collection<BenchmarkProbePoint> points() {
        Collection<BenchmarkProbePoint> points = new ArrayList<>();

        for (int thId = 0; thId < agents.length; thId++) {

            double durationMs = agents[thId].getDurationNs() / 1000_000d;

            BenchmarkProbePoint pnt = new BenchmarkProbePoint(thId, new double[] {durationMs});

            points.add(pnt);
        }

        return points;
    }

    /**
     * Useless for {@link BenchmarkTotalsOnlyProbe}.
     */
    @Override public void buildPoint(long time) {
        // No-op
    }

    /**
     * Measures execution time. Used in thread local context.
     */
    static class ThreadAgent {
        /** Start time of {@link org.yardstickframework.BenchmarkDriver#test(java.util.Map)} method. */
        private volatile long startTs;

        /** Finish time. */
        private volatile long finishTs;

        /**
         * Start measuring.
         */
        public void before() {
            startTs = System.nanoTime();
        }

        /**
         * Stop measuring.
         */
        public void after() {
            finishTs = System.nanoTime();
        }

        /**
         * @return measured duration in nanoseconds.
         */
        public long getDurationNs() {
            if (finishTs == 0L && startTs != 0)
                throw new IllegalStateException("Cannot provide results of TotalTime probe; " +
                    "after() method haven't been called");

            return (finishTs - startTs);
        }
    }
}
