/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;

/**
 * Adapter for cache metrics.
 */
public class GridClientDataMetricsAdapter implements GridClientDataMetrics {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime = System.currentTimeMillis();

    /** Last read time. */
    private volatile long readTime = System.currentTimeMillis();

    /** Last update time. */
    private volatile long writeTime = System.currentTimeMillis();

    /** Number of reads. */
    private volatile int reads;

    /** Number of writes. */
    private volatile int writes;

    /** Number of hits. */
    private volatile int hits;

    /** Number of misses. */
    private volatile int misses;

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long writeTime() {
        return writeTime;
    }

    /** {@inheritDoc} */
    @Override public long readTime() {
        return readTime;
    }

    /** {@inheritDoc} */
    @Override public int reads() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public int writes() {
        return writes;
    }

    /** {@inheritDoc} */
    @Override public int hits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public int misses() {
        return misses;
    }

    /**
     * Sets creation time.
     *
     * @param createTime Creation time.
     */
    public void createTime(long createTime) {
        this.createTime = createTime;
    }

    /**
     * Sets read time.
     *
     * @param readTime Read time.
     */
    public void readTime(long readTime) {
        this.readTime = readTime;
    }

    /**
     * Sets write time.
     *
     * @param writeTime Write time.
     */
    public void writeTime(long writeTime) {
        this.writeTime = writeTime;
    }

    /**
     * Sets number of reads.
     *
     * @param reads Number of reads.
     */
    public void reads(int reads) {
        this.reads = reads;
    }

    /**
     * Sets number of writes.
     *
     * @param writes Number of writes.
     */
    public void writes(int writes) {
        this.writes = writes;
    }

    /**
     * Sets number of hits.
     *
     * @param hits Number of hits.
     */
    public void hits(int hits) {
        this.hits = hits;
    }

    /**
     * Sets number of misses.
     *
     * @param misses Number of misses.
     */
    public void misses(int misses) {
        this.misses = misses;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridClientDataMetricsAdapter [" +
            "createTime=" + createTime +
            ", hits=" + hits +
            ", misses=" + misses +
            ", reads=" + reads +
            ", readTime=" + readTime +
            ", writes=" + writes +
            ", writeTime=" + writeTime +
            ']';
    }
}
