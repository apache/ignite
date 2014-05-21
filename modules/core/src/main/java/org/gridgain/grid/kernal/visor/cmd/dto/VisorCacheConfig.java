/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.cache.*;

import java.io.*;

/**
 * Cache configuration data.
 */
public class VisorCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String name;
    private final GridCacheMode mode;
    private final long ttl;
    private final double refreshAheadRatio;
    private final int seqReserveSize;
    private final boolean swapEnabled;
    private final boolean txBatchUpdate;
    private final boolean invalidate;
    private final int startSize;
    private final String cloner;
    private final String txMgrLookup;

    private final VisorAffinityConfig affinity;
    private final VisorPreloadConfig preload;
    private final VisorEvictionConfig evict;
    private final VisorNearCacheConfig near;
    private final VisorDefaultCacheConfig dflt;
    private final VisorDgcConfig dgc;
    private final VisorStoreConfig store;

    public VisorCacheConfig(String name, GridCacheMode mode, long ttl, double refreshAheadRatio, int seqReserveSize,
        boolean swapEnabled, boolean txBatchUpdate, boolean invalidate, int startSize, String cloner,
        String txMgrLookup, VisorAffinityConfig affinity,
        VisorPreloadConfig preload, VisorEvictionConfig evict,
        VisorNearCacheConfig near, VisorDefaultCacheConfig dflt,
        VisorDgcConfig dgc, VisorStoreConfig store) {
        this.name = name;
        this.mode = mode;
        this.ttl = ttl;
        this.refreshAheadRatio = refreshAheadRatio;
        this.seqReserveSize = seqReserveSize;
        this.swapEnabled = swapEnabled;
        this.txBatchUpdate = txBatchUpdate;
        this.invalidate = invalidate;
        this.startSize = startSize;
        this.cloner = cloner;
        this.txMgrLookup = txMgrLookup;
        this.affinity = affinity;
        this.preload = preload;
        this.evict = evict;
        this.near = near;
        this.dflt = dflt;
        this.dgc = dgc;
        this.store = store;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Mode.
     */
    public GridCacheMode mode() {
        return mode;
    }

    /**
     * @return Ttl.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Refresh ahead ratio.
     */
    public double refreshAheadRatio() {
        return refreshAheadRatio;
    }

    /**
     * @return Sequence reserve size.
     */
    public int sequenceReserveSize() {
        return seqReserveSize;
    }

    /**
     * @return Swap enabled.
     */
    public boolean swapEnabled() {
        return swapEnabled;
    }

    /**
     * @return Tx batch update.
     */
    public boolean txBatchUpdate() {
        return txBatchUpdate;
    }

    /**
     * @return Invalidate.
     */
    public boolean invalidate() {
        return invalidate;
    }

    /**
     * @return Start size.
     */
    public int startSize() {
        return startSize;
    }

    /**
     * @return Cloner.
     */
    public String cloner() {
        return cloner;
    }

    /**
     * @return Tx manager lookup.
     */
    public String txManagerLookup() {
        return txMgrLookup;
    }

    /**
     * @return Affinity.
     */
    public VisorAffinityConfig affinity() {
        return affinity;
    }

    /**
     * @return Preload.
     */
    public VisorPreloadConfig preload() {
        return preload;
    }

    /**
     * @return Evict.
     */
    public VisorEvictionConfig evict() {
        return evict;
    }

    /**
     * @return Near.
     */
    public VisorNearCacheConfig near() {
        return near;
    }

    /**
     * @return dflt.
     */
    public VisorDefaultCacheConfig defaultConfig() {
        return dflt;
    }

    /**
     * @return Dgc.
     */
    public VisorDgcConfig dgc() {
        return dgc;
    }

    /**
     * @return Store.
     */
    public VisorStoreConfig store() {
        return store;
    }
}
