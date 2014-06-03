/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.dr.hub.receiver.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data transfer object for DR sender hub connection configuration properties.
 */
public class VisorDrSenderHubConnectionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of remote data center this replica works with. */
    private final Byte dataCenterId;

    /** Addresses of remote receiver hubs. */
    private final String[] receiverHubAddresses;

    /** Local network interface name to which this replica hub will be bound to. */
    @Nullable private final String localOutboundHost;

    /** Replica hub selection strategy. */
    private final GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode;

    /** IDs of data centers updates from which will not be replicated to this remote data center. */
    private final byte[] ignoredDataCenterIds;

    /** Create data transfer object with given parameters. */
    public VisorDrSenderHubConnectionConfig(
        Byte dataCenterId,
        String[] receiverHubAddresses,
        @Nullable String localOutboundHost,
        GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode,
        byte[] ignoredDataCenterIds
    ) {
        this.dataCenterId = dataCenterId;
        this.receiverHubAddresses = receiverHubAddresses;
        this.localOutboundHost = localOutboundHost;
        this.receiverHubLoadBalancingMode = receiverHubLoadBalancingMode;
        this.ignoredDataCenterIds = ignoredDataCenterIds;
    }

    /**
     * @return ID of remote data center this replica works with.
     */
    public Byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @return Addresses of remote receiver hubs.
     */
    public String[] receiverHubAddresses() {
        return receiverHubAddresses;
    }

    /**
     * @return Local network interface name to which this replica hub will be bound to.
     */
    public String localOutboundHost() {
        return localOutboundHost;
    }

    /**
     * @return Replica hub selection strategy.
     */
    public GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode() {
        return receiverHubLoadBalancingMode;
    }

    /**
     * @return IDs of data centers updates from which will not be replicated to this remote data center.
     */
    public byte[] ignoredDataCenterIds() {
        return ignoredDataCenterIds;
    }
}
