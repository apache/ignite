/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto.node;

import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data transfer object for DR sender hub connection configuration properties.
 */
public class VisorDrSenderHubConnectionConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of remote data center this replica works with. */
    private Byte dataCenterId;

    /** Addresses of remote receiver hubs. */
    private String[] receiverHubAddresses;

    /** Local network interface name to which this replica hub will be bound to. */
    private String localOutboundHost;

    /** Replica hub selection strategy. */
    private GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode;

    /** IDs of data centers updates from which will not be replicated to this remote data center. */
    private byte[] ignoredDataCenterIds;

    /**
     * @param rmtCfg Data center replication sender hub connection configuration.
     * @return Data transfer object for DR sender hub connection configuration properties.
     */
    public static VisorDrSenderHubConnectionConfig from(GridDrSenderHubConnectionConfiguration rmtCfg) {
        if (rmtCfg == null)
            return null;

        VisorDrSenderHubConnectionConfig cfg = new VisorDrSenderHubConnectionConfig();

        cfg.dataCenterId(rmtCfg.getDataCenterId());
        cfg.receiverHubAddresses(rmtCfg.getReceiverHubAddresses());
        cfg.localOutboundHost(rmtCfg.getLocalOutboundHost());
        cfg.receiverHubLoadBalancingMode(rmtCfg.getReceiverHubLoadBalancingMode());
        cfg.ignoredDataCenterIds(rmtCfg.getIgnoredDataCenterIds());

        return cfg;
    }

    /**
     * @return ID of remote data center this replica works with.
     */
    public Byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @param dataCenterId New iD of remote data center this replica works with.
     */
    public void dataCenterId(Byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /**
     * @return Addresses of remote receiver hubs.
     */
    public String[] receiverHubAddresses() {
        return receiverHubAddresses;
    }

    /**
     * @param receiverHubAddrs New addresses of remote receiver hubs.
     */
    public void receiverHubAddresses(String[] receiverHubAddrs) {
        receiverHubAddresses = receiverHubAddrs;
    }

    /**
     * @return Local network interface name to which this replica hub will be bound to.
     */
    @Nullable public String localOutboundHost() {
        return localOutboundHost;
    }

    /**
     * @param locOutboundHost New local network interface name to which this replica hub will be bound to.
     */
    public void localOutboundHost(@Nullable String locOutboundHost) {
        localOutboundHost = locOutboundHost;
    }

    /**
     * @return Replica hub selection strategy.
     */
    public GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode() {
        return receiverHubLoadBalancingMode;
    }

    /**
     * @param receiverHubLoadBalancingMode New replica hub selection strategy.
     */
    public void receiverHubLoadBalancingMode(GridDrReceiverHubLoadBalancingMode receiverHubLoadBalancingMode) {
        this.receiverHubLoadBalancingMode = receiverHubLoadBalancingMode;
    }

    /**
     * @return IDs of data centers updates from which will not be replicated to this remote data center.
     */
    public byte[] ignoredDataCenterIds() {
        return ignoredDataCenterIds;
    }

    /**
     * @param ignoredDataCenterIds New iDs of data centers updates from which will not be replicated to
     * this remote data  center.
     */
    public void ignoredDataCenterIds(byte[] ignoredDataCenterIds) {
        this.ignoredDataCenterIds = ignoredDataCenterIds;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderHubConnectionConfig.class, this);
    }
}
