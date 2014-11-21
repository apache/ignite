/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto.streamer;

import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridStreamerStageMetrics}.
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
    public static VisorStreamerStageMetrics from(GridStreamerStageMetrics m) {
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
    public static Collection<VisorStreamerStageMetrics> stages(GridStreamer streamer) {
        assert streamer != null;

        Collection<VisorStreamerStageMetrics> res = new ArrayList<>();

        for (GridStreamerStageMetrics m : streamer.metrics().stageMetrics())
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
