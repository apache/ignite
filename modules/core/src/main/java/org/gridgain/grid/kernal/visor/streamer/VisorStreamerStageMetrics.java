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

package org.gridgain.grid.kernal.visor.streamer;

import org.apache.ignite.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link org.apache.ignite.streamer.StreamerStageMetrics}.
 */
public class VisorStreamerStageMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Stage name. */
    private String name;

    /** Minimum execution time. */
    private long minExecTm;

    /** Average execution time. */
    private long avgExecTm;

    /** Maximum execution time. */
    private long maxExecTm;

    /** Minimum waiting time. */
    private long minWaitingTm;

    /** Average waiting time. */
    private long avgWaitingTm;

    /** Maximum waiting time. */
    private long maxWaitingTm;

    /** Executed count. */
    private long executed;

    /** Failures count. */
    private int failures;

    /** If executing. */
    private boolean executing;

    /** Throughput. */
    private long throughput = -1;

    /** Failures frequency. */
    private int failuresFreq = -1;

    /** Create data transfer object for given metrics. */
    public static VisorStreamerStageMetrics from(StreamerStageMetrics m) {
        assert m != null;

        VisorStreamerStageMetrics metrics = new VisorStreamerStageMetrics();

        metrics.name(m.name());

        metrics.minExecutionTime(m.minimumExecutionTime());
        metrics.avgExecutionTime(m.averageExecutionTime());
        metrics.maxExecutionTime(m.maximumExecutionTime());

        metrics.minWaitingTime(m.minimumWaitingTime());
        metrics.avgWaitingTime(m.averageWaitingTime());
        metrics.maxWaitingTime(m.maximumWaitingTime());

        metrics.executed(m.totalExecutionCount());
        metrics.failures(m.failuresCount());
        metrics.executing(m.executing());

        return metrics;
    }

    /** Create data transfer objects for all stages. */
    public static Collection<VisorStreamerStageMetrics> stages(IgniteStreamer streamer) {
        assert streamer != null;

        Collection<VisorStreamerStageMetrics> res = new ArrayList<>();

        for (StreamerStageMetrics m : streamer.metrics().stageMetrics())
            res.add(from(m));

        return res;
    }

    /**
     * @return Stage name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name New stage name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Minimum execution time.
     */
    public long minExecutionTime() {
        return minExecTm;
    }

    /**
     * @param minExecTm New minimum execution time.
     */
    public void minExecutionTime(long minExecTm) {
        this.minExecTm = minExecTm;
    }

    /**
     * @return Average execution time.
     */
    public long avgExecutionTime() {
        return avgExecTm;
    }

    /**
     * @param avgExecTm New average execution time.
     */
    public void avgExecutionTime(long avgExecTm) {
        this.avgExecTm = avgExecTm;
    }

    /**
     * @return Maximum execution time.
     */
    public long maxExecutionTime() {
        return maxExecTm;
    }

    /**
     * @param maxExecTm New maximum execution time.
     */
    public void maxExecutionTime(long maxExecTm) {
        this.maxExecTm = maxExecTm;
    }

    /**
     * @return Minimum waiting time.
     */
    public long minWaitingTime() {
        return minWaitingTm;
    }

    /**
     * @param minWaitingTm New minimum waiting time.
     */
    public void minWaitingTime(long minWaitingTm) {
        this.minWaitingTm = minWaitingTm;
    }

    /**
     * @return Average waiting time.
     */
    public long avgWaitingTime() {
        return avgWaitingTm;
    }

    /**
     * @param avgWaitingTm New average waiting time.
     */
    public void avgWaitingTime(long avgWaitingTm) {
        this.avgWaitingTm = avgWaitingTm;
    }

    /**
     * @return Maximum waiting time.
     */
    public long maxWaitingTime() {
        return maxWaitingTm;
    }

    /**
     * @param maxWaitingTm New maximum waiting time.
     */
    public void maxWaitingTime(long maxWaitingTm) {
        this.maxWaitingTm = maxWaitingTm;
    }

    /**
     * @return Executed count.
     */
    public long executed() {
        return executed;
    }

    /**
     * @param executed New executed count.
     */
    public void executed(long executed) {
        this.executed = executed;
    }

    /**
     * @return Failures count.
     */
    public int failures() {
        return failures;
    }

    /**
     * @param failures New failures count.
     */
    public void failures(int failures) {
        this.failures = failures;
    }

    /**
     * @return If executing.
     */
    public boolean executing() {
        return executing;
    }

    /**
     * @param executing New if executing.
     */
    public void executing(boolean executing) {
        this.executing = executing;
    }

    /**
     * @return Throughput.
     */
    public long throughput() {
        return throughput;
    }

    /**
     * @param throughput New throughput.
     */
    public void throughput(long throughput) {
        this.throughput = throughput;
    }

    /**
     * @return Failures frequency.
     */
    public int failuresFrequency() {
        return failuresFreq;
    }

    /**
     * @param failuresFreq New failures frequency.
     */
    public void failuresFrequency(int failuresFreq) {
        this.failuresFreq = failuresFreq;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorStreamerStageMetrics.class, this);
    }
}
