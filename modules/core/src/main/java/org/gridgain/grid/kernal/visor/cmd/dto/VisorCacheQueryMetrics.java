/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for cache query metrics.
 */
public class VisorCacheQueryMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Minimum execution time of query. */
    private final long minTime;

    /** Maximum execution time of query. */
    private final long maxTime;

    /** Average execution time of query. */
    private final double avgTime;

    /** Number of executions. */
    private final int execs;

    /** Total number of times a query execution failed. */
    private final int fails;

    /**
     * Create data transfer object with given parameters.
     *
     * @param minTime Minimum execution time of query.
     * @param maxTime Maximum execution time of query.
     * @param avgTime Average execution time of query.
     * @param execs Number of executions.
     * @param fails Total number of times a query execution failed.
     */
    public VisorCacheQueryMetrics(long minTime, long maxTime, double avgTime, int execs, int fails) {
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.avgTime = avgTime;
        this.execs = execs;
        this.fails = fails;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long minTime() {
        return minTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long maxTime() {
        return maxTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double avgTime() {
        return avgTime;
    }

    /**
     * @return Number of executions.
     */
    public int execs() {
        return execs;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int fails() {
        return fails;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryMetrics.class, this);
    }
}
