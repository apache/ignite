/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.eviction.random.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for eviction configuration properties.
 */
public class VisorEvictionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private String plc;

    /** Cache eviction policy max size. */
    private Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    private String filter;

    /** Synchronous eviction concurrency level. */
    private int syncConcurrencyLvl;

    /** Synchronous eviction timeout. */
    private long syncTimeout;

    /** Synchronized key buffer size. */
    private int syncKeyBufSize;

    /** Synchronous evicts flag. */
    private boolean evictSynchronized;

    /** Synchronous near evicts flag. */
    private boolean nearSynchronized;

    /** Eviction max overflow ratio. */
    private float maxOverflowRatio;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for eviction configuration properties.
     */
    public static VisorEvictionConfig from(GridCacheConfiguration ccfg) {
        VisorEvictionConfig cfg = new VisorEvictionConfig();

        Integer policyMaxSize = null;

        final GridCacheEvictionPolicy policy = ccfg.getEvictionPolicy();

        if (policy instanceof GridCacheLruEvictionPolicyMBean)
            policyMaxSize = ((GridCacheLruEvictionPolicyMBean)policy).getMaxSize();
        else if (policy instanceof GridCacheRandomEvictionPolicyMBean)
            policyMaxSize = ((GridCacheRandomEvictionPolicyMBean)policy).getMaxSize();
        else if (policy instanceof GridCacheFifoEvictionPolicyMBean)
            policyMaxSize = ((GridCacheFifoEvictionPolicyMBean)policy).getMaxSize();

        cfg.policy(compactClass(ccfg.getEvictionPolicy()));
        cfg.policyMaxSize(policyMaxSize);
        cfg.filter(compactClass(ccfg.getEvictionFilter()));
        cfg.synchronizedConcurrencyLevel(ccfg.getEvictSynchronizedConcurrencyLevel());
        cfg.synchronizedTimeout(ccfg.getEvictSynchronizedTimeout());
        cfg.synchronizedKeyBufferSize(ccfg.getEvictSynchronizedKeyBufferSize());
        cfg.evictSynchronized(ccfg.isEvictSynchronized());
        cfg.nearSynchronized(ccfg.isEvictNearSynchronized());
        cfg.maxOverflowRatio(ccfg.getEvictMaxOverflowRatio());

        return cfg;
    }

    /**
     * @return Eviction policy.
     */
    @Nullable public String policy() {
        return plc;
    }

    /**
     * @param plc New eviction policy.
     */
    public void policy(String plc) {
        this.plc = plc;
    }

    /**
     * @return Cache eviction policy max size.
     */
    @Nullable public Integer policyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @param plcMaxSize New cache eviction policy max size.
     */
    public void policyMaxSize(Integer plcMaxSize) {
        this.plcMaxSize = plcMaxSize;
    }

    /**
     * @return Eviction filter to specify which entries should not be evicted.
     */
    @Nullable public String filter() {
        return filter;
    }

    /**
     * @param filter New eviction filter to specify which entries should not be evicted.
     */
    public void filter(String filter) {
        this.filter = filter;
    }

    /**
     * @return synchronized eviction concurrency level.
     */
    public int synchronizedConcurrencyLevel() {
        return syncConcurrencyLvl;
    }

    /**
     * @param syncConcurrencyLvl New synchronized eviction concurrency level.
     */
    public void synchronizedConcurrencyLevel(int syncConcurrencyLvl) {
        this.syncConcurrencyLvl = syncConcurrencyLvl;
    }

    /**
     * @return synchronized eviction timeout.
     */
    public long synchronizedTimeout() {
        return syncTimeout;
    }

    /**
     * @param syncTimeout New synchronized eviction timeout.
     */
    public void synchronizedTimeout(long syncTimeout) {
        this.syncTimeout = syncTimeout;
    }

    /**
     * @return Synchronized key buffer size.
     */
    public int synchronizedKeyBufferSize() {
        return syncKeyBufSize;
    }

    /**
     * @param syncKeyBufSize New synchronized key buffer size.
     */
    public void synchronizedKeyBufferSize(int syncKeyBufSize) {
        this.syncKeyBufSize = syncKeyBufSize;
    }

    /**
     * @return Synchronous evicts flag.
     */
    public boolean evictSynchronized() {
        return evictSynchronized;
    }

    /**
     * @param evictSynchronized New synchronous evicts flag.
     */
    public void evictSynchronized(boolean evictSynchronized) {
        this.evictSynchronized = evictSynchronized;
    }

    /**
     * @return Synchronous near evicts flag.
     */
    public boolean nearSynchronized() {
        return nearSynchronized;
    }

    /**
     * @param nearSynchronized New synchronous near evicts flag.
     */
    public void nearSynchronized(boolean nearSynchronized) {
        this.nearSynchronized = nearSynchronized;
    }

    /**
     * @return Eviction max overflow ratio.
     */
    public float maxOverflowRatio() {
        return maxOverflowRatio;
    }

    /**
     * @param maxOverflowRatio New eviction max overflow ratio.
     */
    public void maxOverflowRatio(float maxOverflowRatio) {
        this.maxOverflowRatio = maxOverflowRatio;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorEvictionConfig.class, this);
    }
}
