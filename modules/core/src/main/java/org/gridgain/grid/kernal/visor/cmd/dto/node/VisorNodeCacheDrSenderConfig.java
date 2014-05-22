/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.dr.hub.sender.*;

import java.io.*;

/**
 * Visor counterpart for {@link GridDrSenderCacheConfiguration}.
 */
public class VisorNodeCacheDrSenderConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data center replication mode. */
    private final GridDrSenderCacheMode mode;

    /** Data center replication batch send size. */
    private final int batchSendSize;

    /** Data center replication batch send frequency. */
    private final long batchSendFrequency;

    /** Maximum amount of pending entries waiting to be sent to the sender hub. */
    private final int maxBatches;

    /** Data center replication sender hub load balancing mode. */
    private final GridDrSenderHubLoadBalancingMode senderHubLoadBalancingMode;

    /** Time in milliseconds to wait between full state transfer batches creation. */
    private final long stateTransferThrottle;

    /** Amount of worker threads responsible for full state transfer. */
    private final int stateTransferThreadsCount;

    public VisorNodeCacheDrSenderConfig(GridDrSenderCacheMode mode, int batchSendSize, long batchSendFrequency,
        int maxBatches,
        GridDrSenderHubLoadBalancingMode senderHubLoadBalancingMode, long stateTransferThrottle,
        int stateTransferThreadsCount) {
        this.mode = mode;
        this.batchSendSize = batchSendSize;
        this.batchSendFrequency = batchSendFrequency;
        this.maxBatches = maxBatches;
        this.senderHubLoadBalancingMode = senderHubLoadBalancingMode;
        this.stateTransferThrottle = stateTransferThrottle;
        this.stateTransferThreadsCount = stateTransferThreadsCount;
    }

    /**
     * @return Data center replication mode.
     */
    public GridDrSenderCacheMode mode() {
        return mode;
    }

    /**
     * @return Data center replication batch send size.
     */
    public int batchSendSize() {
        return batchSendSize;
    }

    /**
     * @return Data center replication batch send frequency.
     */
    public long batchSendFrequency() {
        return batchSendFrequency;
    }

    /**
     * @return Maximum amount of pending entries waiting to be sent to the sender hub.
     */
    public int maxBatches() {
        return maxBatches;
    }

    /**
     * @return Data center replication sender hub load balancing mode.
     */
    public GridDrSenderHubLoadBalancingMode senderHubLoadBalancingMode() {
        return senderHubLoadBalancingMode;
    }

    /**
     * @return Time in milliseconds to wait between full state transfer batches creation.
     */
    public long stateTransferThrottle() {
        return stateTransferThrottle;
    }

    /**
     * @return Amount of worker threads responsible for full state transfer.
     */
    public int stateTransferThreadsCount() {
        return stateTransferThreadsCount;
    }
}
