/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Streamer stage metrics adapter.
 */
public class StreamerStageMetricsAdapter implements StreamerStageMetrics {
    /** */
    private String name;

    /** */
    private long minExecTime;

    /** */
    private long maxExecTime;

    /** */
    private long avgExecTime;

    /** */
    private long minWaitTime;

    /** */
    private long maxWaitTime;

    /** */
    private long avgWaitTime;

    /** */
    private long totalExecCnt;

    /** */
    private int failuresCnt;

    /** */
    private boolean executing;

    /**
     * Empty constructor.
     */
    public StreamerStageMetricsAdapter() {
        // No-op.
    }

    /**
     * @param metrics Metrics.
     */
    public StreamerStageMetricsAdapter(StreamerStageMetrics metrics) {
        // Preserve alphabetic order for maintenance.
        avgExecTime = metrics.averageExecutionTime();
        avgWaitTime = metrics.averageWaitingTime();
        executing = metrics.executing();
        failuresCnt = metrics.failuresCount();
        maxExecTime = metrics.maximumExecutionTime();
        maxWaitTime = metrics.maximumWaitingTime();
        minExecTime = metrics.minimumExecutionTime();
        minWaitTime = metrics.minimumWaitingTime();
        name = metrics.name();
        totalExecCnt = metrics.totalExecutionCount();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long minimumExecutionTime() {
        return minExecTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumExecutionTime() {
        return maxExecTime;
    }

    /** {@inheritDoc} */
    @Override public long averageExecutionTime() {
        return avgExecTime;
    }

    /** {@inheritDoc} */
    @Override public long totalExecutionCount() {
        return totalExecCnt;
    }

    /** {@inheritDoc} */
    @Override public long minimumWaitingTime() {
        return minWaitTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumWaitingTime() {
        return maxWaitTime;
    }

    /** {@inheritDoc} */
    @Override public long averageWaitingTime() {
        return avgWaitTime;
    }

    /** {@inheritDoc} */
    @Override public int failuresCount() {
        return failuresCnt;
    }

    /** {@inheritDoc} */
    @Override public boolean executing() {
        return executing;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerStageMetricsAdapter.class, this);
    }
}
