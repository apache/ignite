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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;

/**
 * Streamer metrics adapter.
 */
public class StreamerMetricsAdapter implements StreamerMetrics {
    /** */
    private int stageActiveExecCnt;

    /** */
    private int stageWaitingExecCnt;

    /** */
    private long stageTotalExecCnt;

    /** */
    private long pipelineMaxExecTime;

    /** */
    private long pipelineMinExecTime;

    /** */
    private long pipelineAvgExecTime;

    /** */
    private int pipelineMaxExecNodes;

    /** */
    private int pipelineMinExecNodes;

    /** */
    private int pipelineAvgExecNodes;

    /** */
    private long qryMaxExecTime;

    /** */
    private long qryMinExecTime;

    /** */
    private long qryAvgExecTime;

    /** */
    private int qryMaxExecNodes;

    /** */
    private int qryMinExecNodes;

    /** */
    private int qryAvgExecNodes;

    /** */
    private int curActiveSes;

    /** */
    private int maxActiveSes;

    /** */
    private int failuresCnt;

    /** */
    private int execSvcCap;

    /** */
    @GridToStringInclude
    private Map<String, StreamerStageMetrics> stageMetrics;

    /** */
    @GridToStringInclude
    private Map<String, StreamerWindowMetrics> windowMetrics;

    /**
     * Empty constructor.
     */
    public StreamerMetricsAdapter() {
        // No-op.
    }

    /**
     * @param metrics Metrics.
     */
    public StreamerMetricsAdapter(StreamerMetrics metrics) {
        // Preserve alphabetic order for maintenance.
        curActiveSes = metrics.currentActiveSessions();
        execSvcCap = metrics.executorServiceCapacity();
        failuresCnt = metrics.failuresCount();
        maxActiveSes = metrics.maximumActiveSessions();
        pipelineAvgExecNodes = metrics.pipelineAverageExecutionNodes();
        pipelineAvgExecTime = metrics.pipelineAverageExecutionTime();
        pipelineMaxExecNodes = metrics.pipelineMaximumExecutionNodes();
        pipelineMaxExecTime = metrics.pipelineMaximumExecutionTime();
        pipelineMinExecNodes = metrics.pipelineMinimumExecutionNodes();
        pipelineMinExecTime = metrics.pipelineMinimumExecutionTime();
        qryAvgExecNodes = metrics.queryAverageExecutionNodes();
        qryAvgExecTime = metrics.queryAverageExecutionTime();
        qryMaxExecNodes = metrics.queryMaximumExecutionNodes();
        qryMaxExecTime = metrics.queryMaximumExecutionTime();
        qryMinExecNodes = metrics.queryMinimumExecutionNodes();
        qryMinExecTime = metrics.queryMinimumExecutionTime();
        stageActiveExecCnt = metrics.stageActiveExecutionCount();
        stageTotalExecCnt = metrics.stageTotalExecutionCount();
        stageWaitingExecCnt = metrics.stageWaitingExecutionCount();

        // Stage metrics.
        Map<String, StreamerStageMetrics> map = U.newLinkedHashMap(metrics.stageMetrics().size());

        for (StreamerStageMetrics m : metrics.stageMetrics())
            map.put(m.name(), new StreamerStageMetricsAdapter(m));

        stageMetrics = Collections.unmodifiableMap(map);

        Map<String, StreamerWindowMetrics> map0 = U.newLinkedHashMap(metrics.windowMetrics().size());

        for (StreamerWindowMetrics m : metrics.windowMetrics())
            map0.put(m.name(), new StreamerWindowMetricsAdapter(m));

        windowMetrics = Collections.unmodifiableMap(map0);
    }

    /** {@inheritDoc} */
    @Override public int stageActiveExecutionCount() {
        return stageActiveExecCnt;
    }

    /** {@inheritDoc} */
    @Override public int stageWaitingExecutionCount() {
        return stageWaitingExecCnt;
    }

    /** {@inheritDoc} */
    @Override public long stageTotalExecutionCount() {
        return stageTotalExecCnt;
    }

    /** {@inheritDoc} */
    @Override public long pipelineMaximumExecutionTime() {
        return pipelineMaxExecTime;
    }

    /** {@inheritDoc} */
    @Override public long pipelineMinimumExecutionTime() {
        return pipelineMinExecTime;
    }

    /** {@inheritDoc} */
    @Override public long pipelineAverageExecutionTime() {
        return pipelineAvgExecTime;
    }

    /** {@inheritDoc} */
    @Override public int pipelineMaximumExecutionNodes() {
        return pipelineMaxExecNodes;
    }

    /** {@inheritDoc} */
    @Override public int pipelineMinimumExecutionNodes() {
        return pipelineMinExecNodes;
    }

    /** {@inheritDoc} */
    @Override public int pipelineAverageExecutionNodes() {
        return pipelineAvgExecNodes;
    }

    /** {@inheritDoc} */
    @Override public long queryMaximumExecutionTime() {
        return qryMaxExecTime;
    }

    /** {@inheritDoc} */
    @Override public long queryMinimumExecutionTime() {
        return qryMinExecTime;
    }

    /** {@inheritDoc} */
    @Override public long queryAverageExecutionTime() {
        return qryAvgExecTime;
    }

    /** {@inheritDoc} */
    @Override public int queryMaximumExecutionNodes() {
        return qryMaxExecNodes;
    }

    /** {@inheritDoc} */
    @Override public int queryMinimumExecutionNodes() {
        return qryMinExecNodes;
    }

    /** {@inheritDoc} */
    @Override public int queryAverageExecutionNodes() {
        return qryAvgExecNodes;
    }

    /** {@inheritDoc} */
    @Override public int currentActiveSessions() {
        return curActiveSes;
    }

    /** {@inheritDoc} */
    @Override public int maximumActiveSessions() {
        return maxActiveSes;
    }

    /** {@inheritDoc} */
    @Override public int failuresCount() {
        return failuresCnt;
    }

    /** {@inheritDoc} */
    @Override public int executorServiceCapacity() {
        return execSvcCap;
    }

    /** {@inheritDoc} */
    @Override public StreamerStageMetrics stageMetrics(String stageName) {
        StreamerStageMetrics metrics = stageMetrics.get(stageName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer stage is not configured: " + stageName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<StreamerStageMetrics> stageMetrics() {
        return stageMetrics.values();
    }

    /** {@inheritDoc} */
    @Override public StreamerWindowMetrics windowMetrics(String winName) {
        StreamerWindowMetrics metrics = windowMetrics.get(winName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer window is not configured: " + winName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<StreamerWindowMetrics> windowMetrics() {
        return windowMetrics.values();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerMetricsAdapter.class, this);
    }
}
