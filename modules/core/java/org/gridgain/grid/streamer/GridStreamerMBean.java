/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer;

import org.gridgain.grid.util.mbean.*;
import org.jetbrains.annotations.*;

/**
 * Streamer MBean interface.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean that provides access to streamer description and metrics.")
public interface GridStreamerMBean {
    /**
     * Gets streamer name.
     *
     * @return Streamer name.
     */
    @GridMBeanDescription("Streamer name.")
    @Nullable public String getName();

    /**
     * Gets {@code atLeastOnce} configuration flag.
     *
     * @return {@code True} if {@code atLeastOnce} is configured.
     */
    @GridMBeanDescription("True if atLeastOnce is configured.")
    public boolean isAtLeastOnce();

    /**
     * Gets size of stage futures map. This map is maintained only when {@code atLeastOnce} configuration
     * flag is set to true.
     *
     * @return Stage future map size.
     */
    @GridMBeanDescription("Stage future map size.")
    public int getStageFutureMapSize();

    /**
     * Gets size of batch futures map.
     *
     * @return Batch future map size.
     */
    @GridMBeanDescription("Batch future map size.")
    public int getBatchFutureMapSize();

    /**
     * Gets number of stages currently being executed in streamer pool.
     *
     * @return Number of stages. Cannot be more than pool thread count.
     */
    @GridMBeanDescription("Number of stages currently being executed in streamer pool.")
    public int getStageActiveExecutionCount();

    /**
     * Gets number of event batches currently waiting to be executed.
     *
     * @return Number of event batches waiting to be processed.
     */
    @GridMBeanDescription("Number of event batches currently waiting to be executed.")
    public int getStageWaitingExecutionCount();

    /**
     * Gets total number of stages executed since last reset.
     *
     * @return Total number of stages executed since last reset.
     */
    @GridMBeanDescription("Total number of stages executed since last reset.")
    public long getStageTotalExecutionCount();

    /**
     * Gets pipeline maximum execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split to different nodes, metrics for each split will be
     * recorded independently.
     *
     * @return Pipeline maximum execution time.
     */
    @GridMBeanDescription("Pipeline maximum execution time.")
    public long getPipelineMaximumExecutionTime();

    /**
     * Gets pipeline minimum execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split to different nodes, metrics for each split will be
     * recorded independently.
     *
     * @return Pipeline minimum execution time.
     */
    @GridMBeanDescription("Pipeline minimum execution time.")
    public long getPipelineMinimumExecutionTime();

    /**
     * Gets pipeline average execution time, i.e. time between execution start and time when last stage in pipeline
     * returned empty map. If pipeline execution was split, metrics for each split will be recorded independently.
     *
     * @return Pipeline average execution time.
     */
    @GridMBeanDescription("Pipeline average execution time.")
    public long getPipelineAverageExecutionTime();

    /**
     * Gets maximum number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Maximum number of unique nodes in pipeline execution.
     */
    @GridMBeanDescription("Maximum number of unique nodes participated in pipeline execution.")
    public int getPipelineMaximumExecutionNodes();

    /**
     * Gets minimum number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Minimum number of unique nodes in pipeline execution.
     */
    @GridMBeanDescription("Minimum number of unique nodes participated in pipeline execution.")
    public int getPipelineMinimumExecutionNodes();

    /**
     * Gets average number of unique nodes participated in pipeline execution. If pipeline execution was split,
     * metrics for each split will be recorded independently.
     *
     * @return Average number of unique nodes in pipeline execution.
     */
    @GridMBeanDescription("Average number of unique nodes participated in pipeline execution.")
    public int getPipelineAverageExecutionNodes();

    /**
     * Gets number of current active sessions. Since event execution sessions are tracked only when
     * {@code atLeastOnce} configuration property is set to {@code true}, this metric will be collected
     * only in this case. When {@code atLeastOnce} is set to {@code false}, this metric will always be zero.
     *
     * @return Number of current active sessions.
     */
    @GridMBeanDescription("Number of current active sessions.")
    public int getCurrentActiveSessions();

    /**
     * Gets maximum number of active sessions since last reset. Since event execution sessions are tracked only when
     * {@code atLeastOnce} configuration property is set to {@code true}, this metric will be collected
     * only in this case. When {@code atLeastOnce} is set to {@code false}, this metric will always be zero.
     *
     * @return Maximum active sessions since last reset.
     */
    @GridMBeanDescription("Maximum number of active sessions since last reset.")
    public int getMaximumActiveSessions();

    /**
     * Gets number of failures since last reset. If {@code atLeastOnce} flag is set to steamer configuration,
     * then only root node failures will be counted. Otherwise each node will count failures independently.
     *
     * @return Failures count.
     */
    @GridMBeanDescription("Number of failures since last reset.")
    public int getFailuresCount();
}
