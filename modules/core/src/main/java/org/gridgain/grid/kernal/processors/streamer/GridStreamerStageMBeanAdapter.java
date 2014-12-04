/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.gridgain.grid.streamer.*;

/**
 * Streamer stage MBean adapter.
 */
@SuppressWarnings("ConstantConditions")
public class GridStreamerStageMBeanAdapter implements GridStreamerStageMBean {
    /** Stage name. */
    private String stageName;

    /** Stage class name. */
    private String stageClsName;

    /** */
    private IgniteStreamerImpl streamer;

    /**
     * @param stageName Stage name.
     * @param stageClsName Stage class name.
     * @param streamer Streamer implementation.
     */
    public GridStreamerStageMBeanAdapter(String stageName, String stageClsName, IgniteStreamerImpl streamer) {
        this.stageName = stageName;
        this.stageClsName = stageClsName;
        this.streamer = streamer;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return stageName;
    }

    /** {@inheritDoc} */
    @Override public String getStageClassName() {
        return stageClsName;
    }

    /** {@inheritDoc} */
    @Override public long getMinimumExecutionTime() {
        return streamer.metrics().stageMetrics(stageName).minimumExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumExecutionTime() {
        return streamer.metrics().stageMetrics(stageName).maximumExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getAverageExecutionTime() {
        return streamer.metrics().stageMetrics(stageName).averageExecutionTime();
    }

    /** {@inheritDoc} */
    @Override public long getMinimumWaitingTime() {
        return streamer.metrics().stageMetrics(stageName).minimumWaitingTime();
    }

    /** {@inheritDoc} */
    @Override public long getMaximumWaitingTime() {
        return streamer.metrics().stageMetrics(stageName).maximumWaitingTime();
    }

    /** {@inheritDoc} */
    @Override public long getAverageWaitingTime() {
        return streamer.metrics().stageMetrics(stageName).averageWaitingTime();
    }

    /** {@inheritDoc} */
    @Override public long getTotalExecutionCount() {
        return streamer.metrics().stageMetrics(stageName).totalExecutionCount();
    }

    /** {@inheritDoc} */
    @Override public int getFailuresCount() {
        return streamer.metrics().stageMetrics(stageName).failuresCount();
    }

    /** {@inheritDoc} */
    @Override public boolean isExecuting() {
        return streamer.metrics().stageMetrics(stageName).executing();
    }
}
