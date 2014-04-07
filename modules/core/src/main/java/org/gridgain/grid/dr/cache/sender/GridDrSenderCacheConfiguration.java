/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.sender;

import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.dr.cache.sender.GridDrSenderCacheMode.*;

/**
 * Data center replication sender cache configuration.
 */
public class GridDrSenderCacheConfiguration {
    /** Default data center replication sender cache mode. */
    public static final GridDrSenderCacheMode DFLT_MODE = DR_ASYNC;

    /** Default data center replication sender cache batch send frequency. */
    public static final long DFLT_BATCH_SND_FREQUENCY = 2000L;

    /** Default data center replication sender cache batch send size. */
    public static final int DFLT_BATCH_SND_SIZE = 2048;

    /** Default data center replication sender cache maximum amount of pending batches. */
    public static final int DFLT_MAX_BATCHES = 32;

    /** Default data center replication sender cache sender hub load balancing mode. */
    public static final GridDrSenderHubLoadBalancingMode DFLT_SND_HUB_LOAD_BALANCING_MODE = GridDrSenderHubLoadBalancingMode.DR_RANDOM;

    /** Default data center replication sender cache amount of worker threads responsible for full state transfer. */
    public static final int DFLT_STATE_TRANSFER_THREADS_CNT = 2;

    /** Default data center replication sender cache state transfer throttle. */
    public static final long DFLT_STATE_TRANSFER_THROTTLE = 0;

    /** Mode. */
    private GridDrSenderCacheMode mode = DFLT_MODE;

    /** Batch send size. */
    private int batchSndSize = DFLT_BATCH_SND_SIZE;

    /** Batch send frequency. */
    private long batchSndFreq = DFLT_BATCH_SND_FREQUENCY;

    /** Maximum amount of pending entries awaiting for replication to happen. */
    private int maxBatches = DFLT_MAX_BATCHES;

    /** Entry filter. */
    private GridDrSenderCacheEntryFilter entryFilter;

    /** Sender hub load balancing mode. */
    private GridDrSenderHubLoadBalancingMode sndHubLoadBalancingMode = DFLT_SND_HUB_LOAD_BALANCING_MODE;

    /** Time in milliseconds to wait between full state transfer batches creation. */
    @SuppressWarnings("RedundantFieldInitialization")
    private long stateTransferThrottle = DFLT_STATE_TRANSFER_THROTTLE;

    /** Amount of worker threads responsible for full state transfer. */
    private int stateTransferThreadsCnt = DFLT_STATE_TRANSFER_THREADS_CNT;

    /**
     * Default constructor.
     */
    public GridDrSenderCacheConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridDrSenderCacheConfiguration(GridDrSenderCacheConfiguration cfg) {
        assert cfg != null;

        batchSndFreq = cfg.getBatchSendFrequency();
        batchSndSize = cfg.getBatchSendSize();
        entryFilter = cfg.getEntryFilter();
        maxBatches = cfg.getMaxBatches();
        mode = cfg.getMode();
        sndHubLoadBalancingMode = cfg.getSenderHubLoadBalancingMode();
        stateTransferThreadsCnt = cfg.getStateTransferThreadsCount();
        stateTransferThrottle = cfg.getStateTransferThrottle();
    }

    /**
     * Gets mode. See {@link GridDrSenderCacheMode} for more info.
     * <p>
     * Defaults to {@link #DFLT_MODE}.
     *
     * @return Mode.
     */
    @Nullable public GridDrSenderCacheMode getMode() {
        return mode;
    }

    /**
     * Sets mode. See {@link #getMode()} for more information.
     *
     * @param mode Mode.
     */
    public void setMode(GridDrSenderCacheMode mode) {
        this.mode = mode;
    }

    /**
     * Gets batch send size. When size of a batch is greater than the given value or it's lifetime is greater than
     * {@link #getBatchSendFrequency()} batch will be sent to a sender hub.
     * <p>
     * When set to non-positive value, batch will be sent only in case it's lifetime is greater than
     * {@code getBatchSendFrequency()}.
     * <p>
     * Defaults to {@link #DFLT_BATCH_SND_SIZE}.
     *
     * @return Batch send size.
     */
    public int getBatchSendSize() {
        return batchSndSize;
    }

    /**
     * Sets batch send size. See {@link #getBatchSendSize()} for more information.
     *
     * @param batchSndSize Batch send size.
     */
    public void setBatchSendSize(int batchSndSize) {
        this.batchSndSize = batchSndSize;
    }

    /**
     * Gets batch send frequency. When size of a batch is greater than {@link #getBatchSendSize()} or it's lifetime
     * is greater than the given value batch will be sent to a sender hub.
     * <p>
     * When set to non-positive value, batch will be sent only in case it's size is greater than
     * {@code #getBatchSendSize()}.
     * <p>
     * Defaults to {@link #DFLT_BATCH_SND_FREQUENCY}.
     *
     * @return Batch send frequency.
     */
    public long getBatchSendFrequency() {
        return batchSndFreq;
    }

    /**
     * Sets batch send frequency. See {@link #getBatchSendFrequency()} for more
     * information.
     *
     * @param batchSndFreq Batch send frequency.
     */
    public void setBatchSendFrequency(long batchSndFreq) {
        this.batchSndFreq = batchSndFreq;
    }

    /**
     * Gets maximum amount of batches awaiting for sender hub acknowledge. When amount of batches awaiting
     * sender hub delivery acknowledge reaches this value, all further cache updates will be suspended until
     * it is below the limit again.
     * <p>
     * When set to non-positive value, maximum amount of batches is unlimited.
     * <p>
     * Defaults to {@link #DFLT_MAX_BATCHES}.
     *
     * @return Maximum amount of batches awaiting for sender hub acknowledge.
     */
    public int getMaxBatches() {
        return maxBatches;
    }

    /**
     * Sets maximum amount of batches awaiting for sender hub acknowledge. See {@link #getMaxBatches()} for more
     * information.
     *
     * @param maxBatches Maximum amount of batches awaiting for sender hub acknowledge.
     */
    public void setMaxBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

    /**
     * Gets entry filter. When set it will be applied to all entries which are candidates for data center replication.
     * If the filter is not passed, data center replication will not be performed on that entry.*
     * <p>
     * Defaults to {@code null}.
     *
     * @return Entry filter.
     */
    public GridDrSenderCacheEntryFilter getEntryFilter() {
        return entryFilter;
    }

    /**
     * Set entry filter. See {@link #getEntryFilter()} for more information.
     *
     * @param entryFilter Entry filter.
     */
    public void setEntryFilter(@Nullable GridDrSenderCacheEntryFilter entryFilter) {
        this.entryFilter = entryFilter;
    }

    /**
     * Gets sender hub load balancing mode. When batch is ready to be sent, this parameter defines how to select the
     * next sender hub this batch will be sent to.
     * <p>
     * Defaults to {@link #DFLT_SND_HUB_LOAD_BALANCING_MODE}.
     *
     * @return Sender hub load balancing mode.
     */
    public GridDrSenderHubLoadBalancingMode getSenderHubLoadBalancingMode() {
        return sndHubLoadBalancingMode;
    }

    /**
     * Sets sender hub load balancing mode. See {@link #getSenderHubLoadBalancingMode()} for more information.
     *
     * @param sndHubLoadBalancingMode Sender hub load balancing mode.
     */
    public void setSenderHubLoadBalancingMode(GridDrSenderHubLoadBalancingMode sndHubLoadBalancingMode) {
        this.sndHubLoadBalancingMode = sndHubLoadBalancingMode;
    }

    /**
     * Gets state transfer throttle. Defines delay in milliseconds to wait between full state transfer batches
     * creation to avoid CPU or network overload.
     * <p>
     * When set to {@code 0} throttling is disabled.
     * <p>
     * Defaults to {@link #DFLT_STATE_TRANSFER_THROTTLE}.
     *
     * @return State transfer throttle.
     */
    public long getStateTransferThrottle() {
        return stateTransferThrottle;
    }

    /**
     * Sets state transfer throttle. See {@link #getStateTransferThrottle()} for more information.
     *
     * @param stateTransferThrottle State transfer throttle.
     */
    public void setStateTransferThrottle(long stateTransferThrottle) {
        this.stateTransferThrottle = stateTransferThrottle;
    }

    /**
     * Gets state transfer threads count. Defines amount of threads which will iterate over cache and force
     * entries data center replication.
     * <p>
     * Defaults to {@link #DFLT_STATE_TRANSFER_THREADS_CNT}.
     *
     * @return State transfer threads count
     */
    public int getStateTransferThreadsCount() {
        return stateTransferThreadsCnt;
    }

    /**
     * Sets state transfer threads count. See {@link #getStateTransferThreadsCount()} for more information.
     *
     * @param stateTransferThreadsCnt State transfer threads count
     */
    public void setStateTransferThreadsCount(int stateTransferThreadsCnt) {
        this.stateTransferThreadsCnt = stateTransferThreadsCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrSenderCacheConfiguration.class, this);
    }
}
