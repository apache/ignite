/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Cache configuration.
 */
public class VisorCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private final String name;

    /** Cache mode. */
    private final GridCacheMode mode;

    /** Distribution mode. */
    private final GridCacheDistributionMode distributionMode;

    /** Cache atomicity mode */
    private final GridCacheAtomicityMode atomicityMode;

    /** Cache atomic sequence reserve size */
    private final int atomicSequenceReserveSize;

    /** Cache atomicity write ordering mode.  */
    private final GridCacheAtomicWriteOrderMode atomicWriteOrderMode;

    /** TTL value. */
    private final long ttl;

    /** Eager ttl flag */
    private final boolean eagerTtl;

    /** Refresh ahead ratio. */
    private final double refreshAheadRatio;

    /** Write synchronization mode. */
    private final GridCacheWriteSynchronizationMode writeSynchronizationMode;

    /** Sequence reserve size. */
    private final int seqReserveSize;

    /** Swap enabled flag. */
    private final boolean swapEnabled;

    /** Flag indicating whether GridGain should attempt to index value and/or key instances stored in cache.*/
    private final boolean queryIndexEnabled;

    /** Tx batch update. */
    private final boolean txBatchUpdate;

    /** Invalidate. */
    private final boolean invalidate;

    /** Start size. */
    private final int startSize;

    /** Cloner. */
    @Nullable private final String cloner;

    /** Tx manager lookup. */
    @Nullable private final String txMgrLookup;

    /** Flag to enable/disable transaction serializable isolation level. */
    private final boolean txSerializableEnabled;

    /** Off-heap max memory. */
    private final long offHeapMaxMemory;

    /** Max query iterator count */
    private final int maxQueryIteratorCnt;

    /** Max concurrent async operations */
    private final int maxConcurrentAsyncOps;

    /** Pessimistic tx logger size */
    private final int pessimisticTxLogSize;

    /** Pessimistic tx logger linger. */
    private final int pessimisticTxLogLinger;

    /** Memory mode. */
    private final GridCacheMemoryMode memoryMode;

    /** Name of SPI to use for indexing. */
    private final String indexingSpiName;

    /** Cache affinity config. */
    private final VisorAffinityConfig affinity;

    /** Preload config. */
    private final VisorPreloadConfig preload;

    /** Eviction config. */
    private final VisorEvictionConfig evict;

    /** Near cache config. */
    private final VisorNearCacheConfig near;

    /** Default config */
    private final VisorDefaultConfig dflt;

    /** Dgc config */
    private final VisorDgcConfig dgc;

    /** Store config */
    private final VisorStoreConfig store;

    /** Write behind config */
    private final VisorWriteBehindConfig writeBehind;

    /** Data center replication send configuration. * */
    @Nullable private final VisorDrSenderConfig drSendConfig;

    /** Data center replication receive configuration. */
    @Nullable private final VisorDrReceiverConfig drReceiveConfig;

    public VisorCacheConfig(String name,
        GridCacheMode mode,
        GridCacheDistributionMode distributionMode,
        GridCacheAtomicityMode atomicityMode,
        int atomicSequenceReserveSize,
        GridCacheAtomicWriteOrderMode atomicWriteOrderMode,
        long ttl,
        boolean eagerTtl,
        double refreshAheadRatio,
        GridCacheWriteSynchronizationMode writeSynchronizationMode,
        int seqReserveSize,
        boolean swapEnabled,
        boolean queryIndexEnabled,
        boolean txBatchUpdate,
        boolean invalidate,
        int startSize,
        @Nullable String cloner,
        @Nullable String txMgrLookup,
        boolean txSerializableEnabled,
        long offHeapMaxMemory,
        int maxQueryIteratorCnt,
        int maxConcurrentAsyncOps,
        int pessimisticTxLogSize,
        int pessimisticTxLogLinger,
        GridCacheMemoryMode memoryMode,
        String indexingSpiName,
        VisorAffinityConfig affinity,
        VisorPreloadConfig preload,
        VisorEvictionConfig evict,
        VisorNearCacheConfig near,
        VisorDefaultConfig dflt,
        VisorDgcConfig dgc,
        VisorStoreConfig store,
        VisorWriteBehindConfig writeBehind,
        @Nullable VisorDrSenderConfig drSendConfig,
        @Nullable VisorDrReceiverConfig drReceiveConfig) {
        this.name = name;
        this.mode = mode;
        this.distributionMode = distributionMode;
        this.atomicityMode = atomicityMode;
        this.atomicSequenceReserveSize = atomicSequenceReserveSize;
        this.atomicWriteOrderMode = atomicWriteOrderMode;
        this.ttl = ttl;
        this.eagerTtl = eagerTtl;
        this.refreshAheadRatio = refreshAheadRatio;
        this.seqReserveSize = seqReserveSize;
        this.writeSynchronizationMode = writeSynchronizationMode;
        this.swapEnabled = swapEnabled;
        this.queryIndexEnabled = queryIndexEnabled;
        this.txBatchUpdate = txBatchUpdate;
        this.invalidate = invalidate;
        this.startSize = startSize;
        this.cloner = cloner;
        this.txMgrLookup = txMgrLookup;
        this.txSerializableEnabled = txSerializableEnabled;
        this.offHeapMaxMemory = offHeapMaxMemory;
        this.maxQueryIteratorCnt = maxQueryIteratorCnt;
        this.maxConcurrentAsyncOps = maxConcurrentAsyncOps;
        this.pessimisticTxLogSize = pessimisticTxLogSize;
        this.pessimisticTxLogLinger = pessimisticTxLogLinger;
        this.memoryMode = memoryMode;
        this.indexingSpiName = indexingSpiName;
        this.affinity = affinity;
        this.preload = preload;
        this.evict = evict;
        this.near = near;
        this.dflt = dflt;
        this.dgc = dgc;
        this.store = store;
        this.writeBehind = writeBehind;
        this.drSendConfig = drSendConfig;
        this.drReceiveConfig = drReceiveConfig;
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
     * @return Distribution mode.
     */
    public GridCacheDistributionMode distributionMode() {
        return distributionMode;
    }

    /**
     * @return Cache atomicity mode
     */
    public GridCacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @return Cache atomic sequence reserve size
     */
    public int atomicSequenceReserveSize() {
        return atomicSequenceReserveSize;
    }

    /**
     * @return Cache atomicity write ordering mode.
     */
    public GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return atomicWriteOrderMode;
    }

    /**
     * @return Ttl.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Eager ttl flag.
     */
    public boolean eagerTtl() {
        return eagerTtl;
    }

    /**
     * @return Refresh ahead ratio.
     */
    public double refreshAheadRatio() {
        return refreshAheadRatio;
    }

    /**
     * @return Write synchronization mode.
     */
    public GridCacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSynchronizationMode;
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
     * @return Query index enabled.
     */
    public boolean queryIndexEnabled() {
        return queryIndexEnabled;
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
    @Nullable public String cloner() {
        return cloner;
    }

    /**
     * @return Tx manager lookup.
     */
    @Nullable public String txManagerLookup() {
        return txMgrLookup;
    }

    /**
     * @return Tx serializable enabled.
     */
    public boolean txSerializableEnabled() {
        return txSerializableEnabled;
    }

    /**
     * @return Affinity config.
     */
    public VisorAffinityConfig affinity() {
        return affinity;
    }

    /**
     * @return Preload config.
     */
    public VisorPreloadConfig preload() {
        return preload;
    }

    /**
     * @return Evict config.
     */
    public VisorEvictionConfig evict() {
        return evict;
    }

    /**
     * @return Near cache config.
     */
    public VisorNearCacheConfig near() {
        return near;
    }

    /**
     * @return Default config.
     */
    public VisorDefaultConfig defaultConfig() {
        return dflt;
    }

    /**
     * @return Dgc config.
     */
    public VisorDgcConfig dgc() {
        return dgc;
    }

    /**
     * @return Store config.
     */
    public VisorStoreConfig store() {
        return store;
    }

    /**
     * @return Data center replication send configuration. *
     */
    public VisorDrSenderConfig drSendConfig() {
        return drSendConfig;
    }

    /**
     * @return Data center replication receive configuration.
     */
    public VisorDrReceiverConfig drReceiveConfig() {
        return drReceiveConfig;
    }

    /**
     * @return Off-heap max memory.
     */
    public long offsetHeapMaxMemory() {
        return offHeapMaxMemory;
    }

    /**
     * @return Max query iterator count.
     */
    public int maxQueryIteratorCount() {
        return maxQueryIteratorCnt;
    }

    /**
     * @return Max concurrent async operations.
     */
    public int maxConcurrentAsyncOperations() {
        return maxConcurrentAsyncOps;
    }

    /**
     * @return Write behind config.
     */
    public VisorWriteBehindConfig writeBehind() {
        return writeBehind;
    }

    /**
     * @return Pessimistic tx logger size.
     */
    public int pessimisticTxLoggerSize() {
        return pessimisticTxLogSize;
    }

    /**
     * @return Pessimistic tx logger linger.
     */
    public int pessimisticTxLoggerLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * @return Memory mode.
     */
    public GridCacheMemoryMode memoryMode() {
        return memoryMode;
    }

    /**
     * @return Indexing spi name.
     */
    public String indexingSpiName() {
        return indexingSpiName;
    }
}
