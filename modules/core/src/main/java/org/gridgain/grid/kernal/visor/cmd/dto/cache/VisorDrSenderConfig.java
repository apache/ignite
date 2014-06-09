/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for DR sender cache configuration properties.
 */
public class VisorDrSenderConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data center replication mode. */
    private GridDrSenderCacheMode mode;

    /** Data center replication batch send size. */
    private int batchSendSize;

    /** Data center replication batch send frequency. */
    private long batchSendFrequency;

    /** Maximum amount of pending entries waiting to be sent to the sender hub. */
    private int maxBatches;

    /** Data center replication sender hub load balancing mode. */
    private GridDrSenderHubLoadBalancingMode senderHubLoadBalancingMode;

    /** Time in milliseconds to wait between full state transfer batches creation. */
    private long stateTransferThrottle;

    /** Amount of worker threads responsible for full state transfer. */
    private int stateTransferThreadsCount;

    /**
     * @param sndCfg Data center replication sender cache configuration.
     * @return Data transfer object for DR sender cache configuration properties.
     */
    public static VisorDrSenderConfig from(GridDrSenderCacheConfiguration sndCfg) {
        VisorDrSenderConfig cfg = new VisorDrSenderConfig();

        cfg.mode(sndCfg.getMode());
        cfg.batchSendSize(sndCfg.getBatchSendSize());
        cfg.batchSendFrequency(sndCfg.getBatchSendFrequency());
        cfg.maxBatches(sndCfg.getMaxBatches());
        cfg.senderHubLoadBalancingMode(sndCfg.getSenderHubLoadBalancingMode());
        cfg.stateTransferThrottle(sndCfg.getStateTransferThrottle());
        cfg.stateTransferThreadsCount(sndCfg.getStateTransferThreadsCount());

        return cfg;
    }

    /**
     * @return Data center replication mode.
     */
    public GridDrSenderCacheMode mode() {
        return mode;
    }

    /**
     * @param mode New data center replication mode.
     */
    public void mode(GridDrSenderCacheMode mode) {
        this.mode = mode;
    }

    /**
     * @return Data center replication batch send size.
     */
    public int batchSendSize() {
        return batchSendSize;
    }

    /**
     * @param batchSndSize New data center replication batch send size.
     */
    public void batchSendSize(int batchSndSize) {
        batchSendSize = batchSndSize;
    }

    /**
     * @return Data center replication batch send frequency.
     */
    public long batchSendFrequency() {
        return batchSendFrequency;
    }

    /**
     * @param batchSndFreq New data center replication batch send frequency.
     */
    public void batchSendFrequency(long batchSndFreq) {
        batchSendFrequency = batchSndFreq;
    }

    /**
     * @return Maximum amount of pending entries waiting to be sent to the sender hub.
     */
    public int maxBatches() {
        return maxBatches;
    }

    /**
     * @param maxBatches New maximum amount of pending entries waiting to be sent to the sender hub.
     */
    public void maxBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

    /**
     * @return Data center replication sender hub load balancing mode.
     */
    public GridDrSenderHubLoadBalancingMode senderHubLoadBalancingMode() {
        return senderHubLoadBalancingMode;
    }

    /**
     * @param sndHubLoadBalancingMode New data center replication sender hub load balancing mode.
     */
    public void senderHubLoadBalancingMode(GridDrSenderHubLoadBalancingMode sndHubLoadBalancingMode) {
        senderHubLoadBalancingMode = sndHubLoadBalancingMode;
    }

    /**
     * @return Time in milliseconds to wait between full state transfer batches creation.
     */
    public long stateTransferThrottle() {
        return stateTransferThrottle;
    }

    /**
     * @param stateTransferThrottle New time in milliseconds to wait between full state transfer batches creation.
     */
    public void stateTransferThrottle(long stateTransferThrottle) {
        this.stateTransferThrottle = stateTransferThrottle;
    }

    /**
     * @return Amount of worker threads responsible for full state transfer.
     */
    public int stateTransferThreadsCount() {
        return stateTransferThreadsCount;
    }

    /**
     * @param stateTransferThreadsCnt New amount of worker threads responsible for full state transfer.
     */
    public void stateTransferThreadsCount(int stateTransferThreadsCnt) {
        stateTransferThreadsCount = stateTransferThreadsCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderConfig.class, this);
    }
}
