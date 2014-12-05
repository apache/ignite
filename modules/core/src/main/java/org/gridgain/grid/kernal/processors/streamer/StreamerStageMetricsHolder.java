/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.streamer.*;
import org.gridgain.grid.util.*;
import org.jdk8.backport.*;

/**
 * Streamer stage metrics holder.
 */
public class StreamerStageMetricsHolder implements StreamerStageMetrics {
    /** Stage name. */
    private String name;

    /** Minimum execution time. */
    private GridAtomicLong minExecTime = new GridAtomicLong(Long.MAX_VALUE);

    /** Maximum execution time. */
    private GridAtomicLong maxExecTime = new GridAtomicLong();

    /** Stage execution time sum. */
    private LongAdder sumExecTime = new LongAdder();

    /** Stage minimum waiting time. */
    private GridAtomicLong minWaitTime = new GridAtomicLong(Long.MAX_VALUE);

    /** Stage maximum waiting time. */
    private GridAtomicLong maxWaitTime = new GridAtomicLong();

    /** Stage average waiting time sum. */
    private LongAdder sumWaitTime = new LongAdder();

    /** Total number of times this stage was executed. */
    private LongAdder totalExecCnt = new LongAdder();

    /** Failures count. */
    private LongAdder failuresCnt = new LongAdder();

    /** Number of threads executing this stage. */
    private LongAdder curActive = new LongAdder();

    /**
     * @param name Stage name.
     */
    public StreamerStageMetricsHolder(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long minimumExecutionTime() {
        long min = minExecTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long maximumExecutionTime() {
        return maxExecTime.get();
    }

    /** {@inheritDoc} */
    @Override public long averageExecutionTime() {
        long execTime = sumExecTime.sum();

        long execs = totalExecCnt.sum();

        return execs == 0 ? 0 : execTime / execs;
    }

    /** {@inheritDoc} */
    @Override public long minimumWaitingTime() {
        long min = minWaitTime.get();

        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** {@inheritDoc} */
    @Override public long maximumWaitingTime() {
        return maxWaitTime.get();
    }

    /** {@inheritDoc} */
    @Override public long averageWaitingTime() {
        long waitTime = sumWaitTime.sum();

        long execs = totalExecCnt.sum();

        return execs == 0 ? 0 : waitTime / execs;
    }

    /** {@inheritDoc} */
    @Override public long totalExecutionCount() {
        return totalExecCnt.longValue();
    }

    /** {@inheritDoc} */
    @Override public int failuresCount() {
        return failuresCnt.intValue();
    }

    /** {@inheritDoc} */
    @Override public boolean executing() {
        return curActive.intValue() > 0;
    }

    /**
     * Execution started callback.
     *
     * @param waitTime Wait time.
     */
    public void onExecutionStarted(long waitTime) {
        if (waitTime < 0)
            waitTime = 0;

        curActive.increment();

        maxWaitTime.setIfGreater(waitTime);
        minWaitTime.setIfLess(waitTime);
        sumWaitTime.add(waitTime);
    }

    /**
     * Execution finished callback.
     *
     * @param execTime Stage execution time.
     */
    public void onExecutionFinished(long execTime) {
        if (execTime < 0)
            execTime = 0;

        curActive.decrement();

        maxExecTime.setIfGreater(execTime);
        minExecTime.setIfLess(execTime);
        sumExecTime.add(execTime);

        totalExecCnt.increment();
    }

    /**
     * Failure callback.
     */
    public void onFailure() {
        failuresCnt.increment();
    }
}
