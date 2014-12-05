/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;

/**
 * Metrics holder.
 *
 * Note that for current active stages we use maximum active stages over last second.
 */
public class StreamerMetricsHolder implements StreamerMetrics {
    /** Max active stages over last minute. */
    private GridAtomicInteger stageActiveMaxLastSec = new GridAtomicInteger();

    /** Last stage max update ts. */
    private volatile long lastStageSampleTs;

    /** Number of running stages. */
    private LongAdder stageActiveCnt = new LongAdder();

    /** Number of waiting stages. */
    private LongAdder stageWaitingCnt = new LongAdder();

    /** Total number of stages executed. */
    private LongAdder stageTotalCnt = new LongAdder();

    /** Max exec time. */
    private GridAtomicLong pipelineMaxExecTime = new GridAtomicLong();

    /** Min exec time. */
    private GridAtomicLong pipelineMinExecTime = new GridAtomicLong(Long.MAX_VALUE);

    /** Pipeline average exec time sampler. */
    private LongAdder pipelineSumExecTime = new LongAdder();

    /** Max exec nodes. */
    private GridAtomicInteger pipelineMaxExecNodes = new GridAtomicInteger();

    /** Min exec nodes. */
    private GridAtomicInteger pipelineMinExecNodes = new GridAtomicInteger(Integer.MAX_VALUE);

    /** Avg exec nodes. */
    private LongAdder pipelineSumExecNodes = new LongAdder();

    /** Total number of pipelines finished on this node. */
    private LongAdder pipelineTotalCnt = new LongAdder();

    /** Query max exec time. */
    private GridAtomicLong qryMaxExecTime = new GridAtomicLong();

    /** Query min exec time. */
    private GridAtomicLong qryMinExecTime = new GridAtomicLong(Long.MAX_VALUE);

    /** Query average exec time sampler. */
    private LongAdder qrySumExecTime = new LongAdder();

    /** Query max exec nodes. */
    private GridAtomicInteger qryMaxExecNodes = new GridAtomicInteger();

    /** Query min exec nodes. */
    private GridAtomicInteger qryMinExecNodes = new GridAtomicInteger(Integer.MAX_VALUE);

    /** Query avg exec nodes. */
    private LongAdder qrySumExecNodes = new LongAdder();

    /** Total number of queries finished on this node. */
    private LongAdder qryTotalCnt = new LongAdder();

    /** Current active sessions. */
    private LongAdder curActiveSessions = new LongAdder();

    /** Max active sessions. */
    private GridAtomicInteger maxActiveSessions = new GridAtomicInteger();

    /** Failures count. */
    private LongAdder failuresCnt = new LongAdder();

    /** Stages metrics. */
    private final GridStreamerStageMetricsHolder[] stageMetrics;

    /** Stage metrics map. */
    private final Map<String, GridStreamerStageMetrics> stageMetricsMap;

    /** Window metrics map. */
    private final Map<String, GridStreamerWindowMetrics> windowMetricsMap;

    /** Executor service capacity. */
    private final int execSvcCap;

    /**
     * @param stageMetrics Array of stage metrics holders.
     * @param windowMetrics Array of window metrics holders.
     * @param execSvcCap Executor service capacity.
     */
    public StreamerMetricsHolder(
        GridStreamerStageMetricsHolder[] stageMetrics,
        GridStreamerWindowMetricsHolder[] windowMetrics,
        int execSvcCap
    ) {
        this.execSvcCap = execSvcCap;
        this.stageMetrics = stageMetrics;

        Map<String, GridStreamerStageMetrics> map = new LinkedHashMap<>();

        for (GridStreamerStageMetricsHolder holder : stageMetrics)
            map.put(holder.name(), holder);

        stageMetricsMap = Collections.unmodifiableMap(map);

        Map<String, GridStreamerWindowMetrics> map0 = new LinkedHashMap<>();

        for (GridStreamerWindowMetricsHolder holder : windowMetrics)
            map0.put(holder.name(), holder);

        windowMetricsMap = Collections.unmodifiableMap(map0);
    }

    /** {@inheritDoc} */
    @Override public int stageActiveExecutionCount() {
        return stageActiveMaxLastSec.get();
    }

    /** {@inheritDoc} */
    @Override public int stageWaitingExecutionCount() {
        return stageWaitingCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public long stageTotalExecutionCount() {
        return stageTotalCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public long pipelineMaximumExecutionTime() {
        return pipelineMaxExecTime.get();
    }

    /** {@inheritDoc} */
    @Override public long pipelineMinimumExecutionTime() {
        long min = pipelineMinExecTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long pipelineAverageExecutionTime() {
        long totalTime = pipelineSumExecTime.sum();

        long execs = pipelineTotalCnt.sum();

        return execs == 0 ? 0 : totalTime / execs;
    }

    /** {@inheritDoc} */
    @Override public int pipelineMaximumExecutionNodes() {
        return pipelineMaxExecNodes.get();
    }

    /** {@inheritDoc} */
    @Override public int pipelineMinimumExecutionNodes() {
        int min = pipelineMinExecNodes.get();

        return min == Integer.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public int pipelineAverageExecutionNodes() {
        long totalNodes = pipelineSumExecNodes.sum();

        long execs = pipelineTotalCnt.sum();

        return execs == 0 ? 0 : (int)(totalNodes / execs);
    }

    /** {@inheritDoc} */
    @Override public long queryMaximumExecutionTime() {
        return qryMaxExecTime.get();
    }

    /** {@inheritDoc} */
    @Override public long queryMinimumExecutionTime() {
        long min = qryMinExecTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long queryAverageExecutionTime() {
        long totalTime = qrySumExecTime.sum();

        long execs = qryTotalCnt.sum();

        return execs == 0 ? 0 : totalTime / execs;
    }

    /** {@inheritDoc} */
    @Override public int queryMaximumExecutionNodes() {
        return qryMaxExecNodes.get();
    }

    /** {@inheritDoc} */
    @Override public int queryMinimumExecutionNodes() {
        int min = qryMinExecNodes.get();

        return min == Integer.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public int queryAverageExecutionNodes() {
        long totalNodes = qrySumExecNodes.sum();

        long execs = qryTotalCnt.sum();

        return execs == 0 ? 0 : (int)(totalNodes / execs);
    }

    /** {@inheritDoc} */
    @Override public int currentActiveSessions() {
        return curActiveSessions.intValue();
    }

    /** {@inheritDoc} */
    @Override public int maximumActiveSessions() {
        return maxActiveSessions.get();
    }

    /** {@inheritDoc} */
    @Override public int failuresCount() {
        return failuresCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public int executorServiceCapacity() {
        return execSvcCap;
    }

    /** {@inheritDoc} */
    @Override public GridStreamerStageMetrics stageMetrics(String stageName) {
        GridStreamerStageMetrics metrics = stageMetricsMap.get(stageName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer stage is not configured: " + stageName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridStreamerStageMetrics> stageMetrics() {
        return stageMetricsMap.values();
    }

    /** {@inheritDoc} */
    @Override public GridStreamerWindowMetrics windowMetrics(String winName) {
        GridStreamerWindowMetrics metrics = windowMetricsMap.get(winName);

        if (metrics == null)
            throw new IllegalArgumentException("Streamer window is not configured: " + winName);

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridStreamerWindowMetrics> windowMetrics() {
        return windowMetricsMap.values();
    }

    /**
     * Stage scheduled callback.
     */
    public void onStageScheduled() {
        stageWaitingCnt.increment();
    }

    /**
     * Stage execution started callback.
     *
     * @param idx Stage index.
     * @param waitTime Stage wait time.
     */
    public void onStageExecutionStarted(int idx, long waitTime) {
        if (waitTime < 0)
            waitTime = 0;

        stageActiveCnt.increment();
        stageWaitingCnt.decrement();

        stageMetrics[idx].onExecutionStarted(waitTime);

        sampleCurrentStages();
    }

    /**
     * Stage execution finished callback.
     *
     * @param idx Stage index.
     * @param execTime Stage execution time.
     */
    public void onStageExecutionFinished(int idx, long execTime) {
        if (execTime < 0)
            execTime = 0;

        stageActiveCnt.decrement();

        stageTotalCnt.increment();

        stageMetrics[idx].onExecutionFinished(execTime);

        sampleCurrentStages();
    }

    /**
     * Pipeline completed callback.
     *
     * @param execTime Pipeline execution time.
     * @param execNodes Pipeline execution nodes.
     */
    public void onPipelineCompleted(long execTime, int execNodes) {
        if (execTime < 0)
            execTime = 0;

        pipelineMaxExecTime.setIfGreater(execTime);
        pipelineMinExecTime.setIfLess(execTime);
        pipelineSumExecTime.add(execTime);

        pipelineMaxExecNodes.setIfGreater(execNodes);
        pipelineMinExecNodes.setIfLess(execNodes);
        pipelineSumExecNodes.add(execNodes);

        pipelineTotalCnt.increment();
    }

    /**
     * Query completed callback.
     *
     * @param execTime Query execution time.
     * @param execNodes Query execution nodes.
     */
    public void onQueryCompleted(long execTime, int execNodes) {
        if (execTime < 0)
            execTime = 0;

        qryMaxExecTime.setIfGreater(execTime);
        qryMinExecTime.setIfLess(execTime);
        qrySumExecTime.add(execTime);

        qryMaxExecNodes.setIfGreater(execNodes);
        qryMinExecNodes.setIfLess(execNodes);
        qrySumExecNodes.add(execNodes);

        qryTotalCnt.increment();
    }

    /**
     * Session started callback.
     */
    public void onSessionStarted() {
        curActiveSessions.increment();

        maxActiveSessions.setIfGreater(curActiveSessions.intValue());
    }

    /**
     * Session finished callback.
     */
    public void onSessionFinished() {
        curActiveSessions.decrement();
    }

    /**
     * Session failed callback.
     */
    public void onSessionFailed() {
        curActiveSessions.decrement();

        failuresCnt.increment();
    }

    /**
     * Stage failure callback.
     *
     * @param idx Stage index.
     */
    public void onStageFailure(int idx) {
        stageMetrics[idx].onFailure();
    }

    /**
     * Samples current sessions.
     */
    public void sampleCurrentStages() {
        long now = U.currentTimeMillis();

        int cur = (int)stageActiveCnt.sum();

        if (now - lastStageSampleTs > 1000) {
            stageActiveMaxLastSec.set(cur);

            lastStageSampleTs = now;
        }
        else
            stageActiveMaxLastSec.setIfGreater(cur);
    }
}
