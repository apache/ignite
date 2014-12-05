/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.streamer;

import java.util.*;

/**
 * Streamer metrics.
 */
public interface StreamerMetrics {
    /**
     * Gets number of stages currently being executed in streamer pool.
     *
     * @return Number of stages. Cannot be more than pool thread count.
     */
    public int stageActiveExecutionCount();

    /**
     * Gets number of event batches currently waiting to be executed.
     *
     * @return Number of event batches waiting to be processed.
     */
    public int stageWaitingExecutionCount();

    /**
     * Gets total number of stages executed since last reset.
     *
     * @return Total number of stages executed since last reset.
     */
    public long stageTotalExecutionCount();

    /**
     * Gets pipeline maximum execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split to different nodes, metrics for each split will be
     * recorded independently.
     *
     * @return Pipeline maximum execution time.
     */
    public long pipelineMaximumExecutionTime();

    /**
     * Gets pipeline minimum execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split to different nodes, metrics for each split will be
     * recorded independently.
     *
     * @return Pipeline minimum execution time.
     */
    public long pipelineMinimumExecutionTime();

    /**
     * Gets pipeline average execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split, metrics for each split will be recorded independently.
     *
     * @return Pipeline average execution time.
     */
    public long pipelineAverageExecutionTime();

    /**
     * Gets maximum number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Maximum number of unique nodes in pipeline execution.
     */
    public int pipelineMaximumExecutionNodes();

    /**
     * Gets minimum number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Minimum number of unique nodes in pipeline execution.
     */
    public int pipelineMinimumExecutionNodes();

    /**
     * Gets average number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Average number of unique nodes in pipeline execution.
     */
    public int pipelineAverageExecutionNodes();

    /**
     * Gets query maximum execution time. If query execution was split to different nodes, metrics for each split
     * will be recorded independently.
     *
     * @return Query maximum execution time.
     */
    public long queryMaximumExecutionTime();

    /**
     * Gets query minimum execution time. If query execution was split to different nodes, metrics for each split
     * will be recorded independently.
     *
     * @return Query minimum execution time.
     */
    public long queryMinimumExecutionTime();

    /**
     * Gets query average execution time. If query execution was split to different nodes, metrics for each split
     * will be recorded independently.
     *
     * @return Query average execution time.
     */
    public long queryAverageExecutionTime();

    /**
     * Gets maximum number of unique nodes participated in query execution. If query execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Maximum number of unique nodes in query execution.
     */
    public int queryMaximumExecutionNodes();

    /**
     * Gets minimum number of unique nodes participated in query execution. If query execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Minimum number of unique nodes in query execution.
     */
    public int queryMinimumExecutionNodes();

    /**
     * Gets average number of unique nodes participated in query execution. If query execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Average number of unique nodes in query execution.
     */
    public int queryAverageExecutionNodes();

    /**
     * Gets number of current active sessions. Since event execution sessions are tracked only when
     * {@code atLeastOnce} configuration property is set to {@code true}, this metric will be collected
     * only in this case. When {@code atLeastOnce} is set to {@code false}, this metric will always be zero.
     *
     * @return Number of current active sessions.
     */
    public int currentActiveSessions();

    /**
     * Gets maximum number of active sessions since last reset. Since event execution sessions are tracked only when
     * {@code atLeastOnce} configuration property is set to {@code true}, this metric will be collected
     * only in this case. When {@code atLeastOnce} is set to {@code false}, this metric will always be zero.
     *
     * @return Maximum active sessions since last reset.
     */
    public int maximumActiveSessions();

    /**
     * Gets number of failures. If {@code atLeastOnce} flag is set to steamer configuration, then only root node
     * failures will be counted. Otherwise each node will count failures independently.
     *
     * @return Failures count.
     */
    public int failuresCount();

    /**
     * Gets maximum number of threads in executor service.
     *
     * @return Maximum number of threads in executor service.
     */
    public int executorServiceCapacity();

    /**
     * Gets current stage metrics, if stage with given name is not configured
     * then {@link IllegalArgumentException} will be thrown.
     *
     * @param stageName Stage name.
     * @return Stage metrics.
     */
    public StreamerStageMetrics stageMetrics(String stageName);

    /**
     * Gets metrics for all stages. Stage metrics order is the same as stage order in configuration.
     *
     * @return Stage metrics collection.
     */
    public Collection<StreamerStageMetrics> stageMetrics();

    /**
     * Gets current window metrics, if window with given name is not configured
     * then {@link IllegalArgumentException} will be thrown.
     *
     * @param winName Window name.
     * @return Window metrics.
     */
    public StreamerWindowMetrics windowMetrics(String winName);

    /**
     * Gets metrics for all windows.
     *
     * @return Collection of window metrics.
     */
    public Collection<StreamerWindowMetrics> windowMetrics();
}
