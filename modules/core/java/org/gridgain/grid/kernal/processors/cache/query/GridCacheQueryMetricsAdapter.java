// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter for {@link GridCacheQueryMetrics}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueryMetricsAdapter implements GridCacheQueryMetrics, Externalizable {
    /** Query metrics key. */
    @GridToStringExclude
    private GridCacheQueryMetricsKey key;

    /** Query creation time. */
    private long createTime = U.currentTimeMillis();

    /** First run time. */
    private volatile long firstTime;

    /** Last run time. */
    private volatile long lastTime;

    /** Minimum time of execution. */
    private volatile long minTime;

    /** Maximum time of execution. */
    private volatile long maxTime;

    /** Average time of execution. */
    private volatile double avgTime;

    /** Number of hits. */
    private volatile int execs;

    /** Number of fails. */
    private volatile int fails;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheQueryMetricsAdapter() {
        /* No-op. */
    }

    /**
     *
     * @param key Query metrics key.
     */
    public GridCacheQueryMetricsAdapter(GridCacheQueryMetricsKey key) {
        assert key != null;

        this.key = key;
    }

    /**
     * @return Metrics key.
     */
    GridCacheQueryMetricsKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public long firstRunTime() {
        return firstTime;
    }

    /** {@inheritDoc} */
    @Override public long lastRunTime() {
        return lastTime;
    }

    /** {@inheritDoc} */
    @Override public long minimumTime() {
        return minTime;
    }

    /** {@inheritDoc} */
    @Override public long maximumTime() {
        return maxTime;
    }

    /** {@inheritDoc} */
    @Override public double averageTime() {
        return avgTime;
    }

    /** {@inheritDoc} */
    @Override public int executions() {
        return execs;
    }

    /** {@inheritDoc} */
    @Override public int fails() {
        return fails;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String clause() {
        return key.clause();
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryType type() {
        return key.type();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Class<?> queryClass() {
        return key.queryClass();
    }

    /**
     * Callback for query execution.
     *
     * @param startTime Start queue time.
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public void onQueryExecute(long startTime, long duration, boolean fail) {
        if (fail) {
            fails++;

            return;
        }

        synchronized (mux) {
            lastTime = startTime;

            if (firstTime == 0) {
                firstTime = lastTime;
                minTime = duration;
                maxTime = duration;
            }

            if (minTime > duration)
                minTime = duration;

            if (maxTime < duration)
                maxTime = duration;

            execs++;

            avgTime = (avgTime * (execs - 1) + duration) / execs;
        }
    }

    /**
     * Merge with given metrics.
     *
     * @return Copy.
     */
    public GridCacheQueryMetricsAdapter copy() {
        GridCacheQueryMetricsAdapter m = new GridCacheQueryMetricsAdapter(key);

        synchronized (mux) {
            m.fails = fails;
            m.firstTime = firstTime;
            m.lastTime = lastTime;
            m.minTime = minTime;
            m.maxTime = maxTime;
            m.execs = execs;
            m.avgTime = avgTime;
        }

        return m;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeLong(createTime);
        out.writeLong(firstTime);
        out.writeLong(lastTime);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeDouble(avgTime);
        out.writeInt(execs);
        out.writeInt(fails);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (GridCacheQueryMetricsKey)in.readObject();
        createTime = in.readLong();
        firstTime = in.readLong();
        lastTime = in.readLong();
        minTime = in.readLong();
        maxTime = in.readLong();
        avgTime = in.readDouble();
        execs = in.readInt();
        fails = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof GridCacheQueryMetricsAdapter))
            return false;

        GridCacheQueryMetricsAdapter oth = (GridCacheQueryMetricsAdapter)obj;

        return oth.key.equals(key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryMetricsAdapter.class, this,
            "type", key.type(), "clsName", key.queryClass().getName(), "clause", key.clause());
    }
}
