/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;
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
    private Map<String, GridStreamerStageMetrics> stageMetrics;

    /** */
    @GridToStringInclude
    private Map<String, GridStreamerWindowMetrics> windowMetrics;

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
        Map<String, GridStreamerStageMetrics> map = U.newLinkedHashMap(metrics.stageMetrics().size());

        for (GridStreamerStageMetrics m : metrics.stageMetrics())
            map.put(m.name(), new GridStreamerStageMetricsAdapter(m));

        stageMetrics = Collections.unmodifiableMap(map);

        Map<String, GridStreamerWindowMetrics> map0 = U.newLinkedHashMap(metrics.windowMetrics().size());

        for (GridStreamerWindowMetrics m : metrics.windowMetrics())
            map0.put(m.name(), new GridStreamerWindowMetricsAdapter(m));

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
    @Override public GridStreamerStageMetrics stageMetrics(String stageName) {
        GridStreamerStageMetrics metrics = stageMetrics.get(stageName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer stage is not configured: " + stageName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridStreamerStageMetrics> stageMetrics() {
        return stageMetrics.values();
    }

    /** {@inheritDoc} */
    @Override public GridStreamerWindowMetrics windowMetrics(String winName) {
        GridStreamerWindowMetrics metrics = windowMetrics.get(winName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer window is not configured: " + winName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridStreamerWindowMetrics> windowMetrics() {
        return windowMetrics.values();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerMetricsAdapter.class, this);
    }
}
