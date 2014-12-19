/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for cache query metrics.
 */
public class VisorCacheQueryMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Minimum execution time of query. */
    private long minTime;

    /** Maximum execution time of query. */
    private long maxTime;

    /** Average execution time of query. */
    private double avgTime;

    /** Number of executions. */
    private int execs;

    /** Total number of times a query execution failed. */
    private int fails;

    /**
     * @param m Cache query metrics.
     * @return Data transfer object for given cache metrics.
     */
    public static VisorCacheQueryMetrics from(GridCacheQueryMetrics m) {
        VisorCacheQueryMetrics qm = new VisorCacheQueryMetrics();

        qm.minTime = m.minimumTime();
        qm.maxTime = m.maximumTime();
        qm.avgTime = m.averageTime();
        qm.execs = m.executions();
        qm.fails = m.fails();

        return qm;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long minimumTime() {
        return minTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long maximumTime() {
        return maxTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double averageTime() {
        return avgTime;
    }

    /**
     * @return Number of executions.
     */
    public int executions() {
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
