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
 * Cache configuration data.
 */
public class VisorCacheConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String name;

    /** */
    private final GridCacheMode mode;

    private final GridCacheDistributionMode distributionMode;
    /** Cache atomicity mode */
    private final GridCacheAtomicityMode atomicityMode;

    /** Cache atomic sequence reserve size */
    private final int atomicSequenceReserveSize;

    /** Cache atomicity write ordering mode.  */
    private final GridCacheAtomicWriteOrderMode atomicWriteOrderMode;

    /**  */
    private final long ttl;

    /** Eager ttl flag */
    private final boolean eagerTtl;

    /**  */
    private final double refreshAheadRatio;

    private final GridCacheWriteSynchronizationMode writeSynchronizationMode;

    /**  */
    private final int seqReserveSize;

    /**  */
    private final boolean swapEnabled;

    private final boolean queryIndexEnabled;
    /**  */
    private final boolean txBatchUpdate;

    /**  */
    private final boolean invalidate;

    /**  */
    private final int startSize;

    /**  */
    private final String cloner;

    /**  */
    private final String txMgrLookup;

    /**  */
    private final boolean txSerializableEnabled;

    /**  */
    private final long offHeapMaxMemory;

    /**  */
    private final int maxQueryIteratorCnt;

    /**  */
    private final int maxConcurrentAsyncOps;

    /**  */
    private final int pessimisticTxLogSize;

    /**  */
    private final int pessimisticTxLogLinger;

    private final GridCacheMemoryMode memoryMode;

    private final String indexingSpiName;
    /**  */
    private final VisorAffinityConfig affinity;

    /**  */
    private final VisorPreloadConfig preload;

    /**  */
    private final VisorEvictionConfig evict;

    /**  */
    private final VisorNearCacheConfig near;

    /**  */
    private final VisorDefaultConfig dflt;

    /**  */
    private final VisorDgcConfig dgc;

    /**  */
    private final VisorStoreConfig store;

    /**  */
    private final VisorWriteBehindConfig writeBehind;

    /** Data center replication send configuration. * */
    @Nullable private final VisorDrSenderConfig drSendConfig;

    /** Data center replication receive configuration. */
    @Nullable private final VisorDrReceiverConfig drReceiveConfig;

    public VisorCacheConfig(String name, GridCacheMode mode, GridCacheDistributionMode distributionMode,
        GridCacheAtomicityMode atomicityMode,
        int atomicSequenceReserveSize, GridCacheAtomicWriteOrderMode atomicWriteOrderMode,
        long ttl, boolean eagerTtl, double refreshAheadRatio,
        GridCacheWriteSynchronizationMode writeSynchronizationMode,
        int seqReserveSize, boolean swapEnabled, boolean queryIndexEnabled, boolean txBatchUpdate, boolean invalidate,
        int startSize, String cloner,
        String txMgrLookup, boolean txSerializableEnabled,
        long offHeapMaxMemory, int maxQueryIteratorCnt, int maxConcurrentAsyncOps, int pessimisticTxLogSize,
        int pessimisticTxLogLinger,
        GridCacheMemoryMode memoryMode, String indexingSpiName, VisorAffinityConfig affinity,
        VisorPreloadConfig preload, VisorEvictionConfig evict,
        VisorNearCacheConfig near, VisorDefaultConfig dflt,
        VisorDgcConfig dgc, VisorStoreConfig store, VisorWriteBehindConfig writeBehind,
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
     * @return Tx serializable enabled.
     */
    public boolean txSerializableEnabled() {
        return txSerializableEnabled;
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
    public VisorDefaultConfig defaultConfig() {
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
     * @return Offset heap max memory.
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
     * @return Write behind.
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
