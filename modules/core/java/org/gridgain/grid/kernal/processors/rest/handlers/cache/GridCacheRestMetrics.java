// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.util.*;

import java.util.*;

/**
 * Grid cache metrics for rest.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheRestMetrics {
    /** Create time. */
    private long createTime;

    /** Last read time. */
    private long readTime;

    /** Last update time. */
    private long writeTime;

    /** Number of reads. */
    private int reads;

    /** Number of writes. */
    private int writes;

    /** Number of hits. */
    private int hits;

    /** Number of misses. */
    private int misses;

    /**
     * Constructor.
     *
     * @param createTime Create time.
     * @param readTime Read time.
     * @param writeTime Write time.
     * @param reads Reads.
     * @param writes Writes.
     * @param hits Hits.
     * @param misses Misses.
     */
    public GridCacheRestMetrics(long createTime, long readTime, long writeTime, int reads, int writes, int hits,
        int misses) {
        this.createTime = createTime;
        this.readTime = readTime;
        this.writeTime = writeTime;
        this.reads = reads;
        this.writes = writes;
        this.hits = hits;
        this.misses = misses;
    }

    /**
     * Gets create time.
     *
     * @return Create time.
     */
    public long getCreateTime() {
        return createTime;
    }

    /**
     * Sets create time.
     *
     * @param createTime Create time.
     */
    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    /**
     * Gets read time.
     *
     * @return Read time.
     */
    public long getReadTime() {
        return readTime;
    }

    /**
     * Sets read time.
     *
     * @param readTime Read time.
     */
    public void setReadTime(long readTime) {
        this.readTime = readTime;
    }

    /**
     * Gets write time.
     *
     * @return Write time.
     */
    public long getWriteTime() {
        return writeTime;
    }

    /**
     * Sets write time.
     *
     * @param writeTime Write time.
     */
    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    /**
     * Gets reads.
     *
     * @return Reads.
     */
    public int getReads() {
        return reads;
    }

    /**
     * Sets reads.
     *
     * @param reads Reads.
     */
    public void setReads(int reads) {
        this.reads = reads;
    }

    /**
     * Gets writes.
     *
     * @return Writes.
     */
    public int getWrites() {
        return writes;
    }

    /**
     * Sets writes.
     *
     * @param writes Writes.
     */
    public void setWrites(int writes) {
        this.writes = writes;
    }

    /**
     * Gets hits.
     *
     * @return Hits.
     */
    public int getHits() {
        return hits;
    }

    /**
     * Sets hits.
     *
     * @param hits Hits.
     */
    public void setHits(int hits) {
        this.hits = hits;
    }

    /**
     * Gets misses.
     *
     * @return Misses.
     */
    public int getMisses() {
        return misses;
    }

    /**
     * Sets misses.
     *
     * @param misses Misses.
     */
    public void setMisses(int misses) {
        this.misses = misses;
    }

    /**
     * Creates map with strings.
     *
     * @return Map.
     */
    public Map<String, Long> map() {
        Map<String, Long> map = new GridLeanMap<>(7);

        map.put("createTime", createTime);
        map.put("readTime", readTime);
        map.put("writeTime", writeTime);
        map.put("reads", (long)reads);
        map.put("writes", (long)writes);
        map.put("hits", (long)hits);
        map.put("misses", (long)misses);

        return map;
    }
}
