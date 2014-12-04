/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link GridCacheMetrics}.
 */
public class VisorCacheMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time of the owning entity (either cache or entry). */
    private long createTm;

    /** Last write time of the owning entity (either cache or entry). */
    private long writeTm;

    /** Last read time of the owning entity (either cache or entry). */
    private long readTm;

    /** Last time transaction was committed. */
    private long commitTm;

    /** Last time transaction was rollback. */
    private long rollbackTm;

    /** Total number of reads of the owning entity (either cache or entry). */
    private int reads;

    /** Total number of writes of the owning entity (either cache or entry). */
    private int writes;

    /** Total number of hits for the owning entity (either cache or entry). */
    private int hits;

    /** Total number of misses for the owning entity (either cache or entry). */
    private int misses;

    /** Total number of transaction commits. */
    private int txCommits;

    /** Total number of transaction rollbacks. */
    private int txRollbacks;

    /** Reads per second. */
    private int readsPerSec;

    /** Writes per second. */
    private int writesPerSec;

    /** Hits per second. */
    private int hitsPerSec;

    /** Misses per second. */
    private int missesPerSec;

    /** Commits per second. */
    private int commitsPerSec;

    /** Rollbacks per second. */
    private int rollbacksPerSec;

    /** Calculate rate of metric per second. */
    private static int perSecond(int metric, long time, long createTime) {
        long seconds = (time - createTime) / 1000;

        return (seconds > 0) ? (int)(metric / seconds) : 0;
    }

    /**
     * @param m Cache metrics.
     * @return Data transfer object for given cache metrics.
     */
    public static VisorCacheMetrics from(GridCacheMetrics m) {
        // TODO gg-9141

        assert m != null;

        VisorCacheMetrics metrics = new VisorCacheMetrics();

        metrics.createTime(m.createTime());
        metrics.writeTime(m.writeTime());
        metrics.readTime(m.readTime());
//        metrics.commitTime(m.commitTime());
//        metrics.rollbackTime(m.rollbackTime());

        metrics.reads(m.reads());
        metrics.writes(m.writes());
        metrics.hits(m.hits());
        metrics.misses(m.misses());

//        metrics.txCommits(m.txCommits());
//        metrics.txRollbacks(m.txRollbacks());

        metrics.readsPerSecond(perSecond(m.reads(), m.readTime(), m.createTime()));
        metrics.writesPerSecond(perSecond(m.writes(), m.writeTime(), m.createTime()));
        metrics.hitsPerSecond(perSecond(m.hits(), m.readTime(), m.createTime()));
        metrics.missesPerSecond(perSecond(m.misses(), m.readTime(), m.createTime()));
//        metrics.commitsPerSecond(perSecond(m.txCommits(), m.commitTime(), m.createTime()));
//        metrics.rollbacksPerSecond(perSecond(m.txRollbacks(), m.rollbackTime(), m.createTime()));

        return metrics;
    }

    /**
     * @return Create time of the owning entity (either cache or entry).
     */
    public long createTime() {
        return createTm;
    }

    /**
     * @param createTm New create time of the owning entity (either cache or entry).
     */
    public void createTime(long createTm) {
        this.createTm = createTm;
    }

    /**
     * @return Last write time of the owning entity (either cache or entry).
     */
    public long writeTime() {
        return writeTm;
    }

    /**
     * @param writeTm New last write time of the owning entity (either cache or entry).
     */
    public void writeTime(long writeTm) {
        this.writeTm = writeTm;
    }

    /**
     * @return Last read time of the owning entity (either cache or entry).
     */
    public long readTime() {
        return readTm;
    }

    /**
     * @param readTm New last read time of the owning entity (either cache or entry).
     */
    public void readTime(long readTm) {
        this.readTm = readTm;
    }

    /**
     * @return Last time transaction was committed.
     */
    public long commitTime() {
        return commitTm;
    }

    /**
     * @param commitTm New last time transaction was committed.
     */
    public void commitTime(long commitTm) {
        this.commitTm = commitTm;
    }

    /**
     * @return Last time transaction was rollback.
     */
    public long rollbackTime() {
        return rollbackTm;
    }

    /**
     * @param rollbackTm New last time transaction was rollback.
     */
    public void rollbackTime(long rollbackTm) {
        this.rollbackTm = rollbackTm;
    }

    /**
     * @return Total number of reads of the owning entity (either cache or entry).
     */
    public int reads() {
        return reads;
    }

    /**
     * @param reads New total number of reads of the owning entity (either cache or entry).
     */
    public void reads(int reads) {
        this.reads = reads;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry).
     */
    public int writes() {
        return writes;
    }

    /**
     * @param writes New total number of writes of the owning entity (either cache or entry).
     */
    public void writes(int writes) {
        this.writes = writes;
    }

    /**
     * @return Total number of hits for the owning entity (either cache or entry).
     */
    public int hits() {
        return hits;
    }

    /**
     * @param hits New total number of hits for the owning entity (either cache or entry).
     */
    public void hits(int hits) {
        this.hits = hits;
    }

    /**
     * @return Total number of misses for the owning entity (either cache or entry).
     */
    public int misses() {
        return misses;
    }

    /**
     * @param misses New total number of misses for the owning entity (either cache or entry).
     */
    public void misses(int misses) {
        this.misses = misses;
    }

    /**
     * @return Total number of transaction commits.
     */
    public int txCommits() {
        return txCommits;
    }

    /**
     * @param txCommits New total number of transaction commits.
     */
    public void txCommits(int txCommits) {
        this.txCommits = txCommits;
    }

    /**
     * @return Total number of transaction rollbacks.
     */
    public int txRollbacks() {
        return txRollbacks;
    }

    /**
     * @param txRollbacks New total number of transaction rollbacks.
     */
    public void txRollbacks(int txRollbacks) {
        this.txRollbacks = txRollbacks;
    }

    /**
     * @return Reads per second.
     */
    public int readsPerSecond() {
        return readsPerSec;
    }

    /**
     * @param readsPerSec New reads per second.
     */
    public void readsPerSecond(int readsPerSec) {
        this.readsPerSec = readsPerSec;
    }

    /**
     * @return Writes per second.
     */
    public int writesPerSecond() {
        return writesPerSec;
    }

    /**
     * @param writesPerSec New writes per second.
     */
    public void writesPerSecond(int writesPerSec) {
        this.writesPerSec = writesPerSec;
    }

    /**
     * @return Hits per second.
     */
    public int hitsPerSecond() {
        return hitsPerSec;
    }

    /**
     * @param hitsPerSec New hits per second.
     */
    public void hitsPerSecond(int hitsPerSec) {
        this.hitsPerSec = hitsPerSec;
    }

    /**
     * @return Misses per second.
     */
    public int missesPerSecond() {
        return missesPerSec;
    }

    /**
     * @param missesPerSec New misses per second.
     */
    public void missesPerSecond(int missesPerSec) {
        this.missesPerSec = missesPerSec;
    }

    /**
     * @return Commits per second.
     */
    public int commitsPerSecond() {
        return commitsPerSec;
    }

    /**
     * @param commitsPerSec New commits per second.
     */
    public void commitsPerSecond(int commitsPerSec) {
        this.commitsPerSec = commitsPerSec;
    }

    /**
     * @return Rollbacks per second.
     */
    public int rollbacksPerSecond() {
        return rollbacksPerSec;
    }

    /**
     * @param rollbacksPerSec New rollbacks per second.
     */
    public void rollbacksPerSecond(int rollbacksPerSec) {
        this.rollbacksPerSec = rollbacksPerSec;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
