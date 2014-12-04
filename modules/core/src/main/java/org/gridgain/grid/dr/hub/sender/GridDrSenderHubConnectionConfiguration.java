/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender;

import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.dr.hub.receiver.GridDrReceiverHubLoadBalancingMode.*;

/**
 * Data center replication sender hub connection configuration.
 */
public class GridDrSenderHubConnectionConfiguration {
    /** Default receiver hub load balancing policy. */
    public static final GridDrReceiverHubLoadBalancingMode DFLT_RCV_HUB_LOAD_BALANCING_MODE = DR_RANDOM;

    /** Default await acknowledge flag. */
    public static final boolean DFLT_AWAIT_ACK = true;

    /** Data center ID.*/
    private byte dataCenterId;

    /** Addresses. */
    private String[] rcvHubAddrs;

    /** Local host. */
    private String locOutboundHost;

    /** Receiver hub load balancing policy. */
    private GridDrReceiverHubLoadBalancingMode rcvHubLoadBalancingMode = DFLT_RCV_HUB_LOAD_BALANCING_MODE;

    /** Ignored data center IDs. */
    private byte[] ignoreDataCenterIds;

    /** Await acknowledge flag. */
    private boolean awaitAck = DFLT_AWAIT_ACK;

    /**
     * Constructor.
     */
    public GridDrSenderHubConnectionConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridDrSenderHubConnectionConfiguration(GridDrSenderHubConnectionConfiguration cfg) {
        rcvHubAddrs = cfg.getReceiverHubAddresses();
        dataCenterId = cfg.getDataCenterId();
        ignoreDataCenterIds = cfg.getIgnoredDataCenterIds();
        locOutboundHost = cfg.getLocalOutboundHost();
        rcvHubLoadBalancingMode = cfg.getReceiverHubLoadBalancingMode();
        awaitAck = cfg.isAwaitAcknowledge();
    }

    /**
     * Gets ID of remote data center.
     *
     * @return ID of remote data center.
     */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /**
     * Sets ID of remote data center. See {@link #getDataCenterId()} for more information.
     *
     * @param dataCenterId ID of remote data center.
     */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /**
     * Gets addresses of remote receiver hubs.
     *
     * @return Addresses of remote receiver hubs.
     */
    public String[] getReceiverHubAddresses() {
        return rcvHubAddrs;
    }

    /**
     * Sets addresses of remote receiver hubs. See {@link #getReceiverHubAddresses()} for more information.
     *
     * @param rcvHubAddrs Addresses of remote receiver hubs.
     */
    public void setReceiverHubAddresses(String... rcvHubAddrs) {
        this.rcvHubAddrs = rcvHubAddrs;
    }

    /**
     * Gets local network interface name form with remote data center is reachable.
     * <p>
     * By default is set to {@code null} meaning that sender hub will be bound to the same interface as the grid itself
     * (see {@link org.gridgain.grid.IgniteConfiguration#getLocalHost()})
     *
     * @return Local network interface name form with remote data center is reachable.
     */
    public String getLocalOutboundHost() {
        return locOutboundHost;
    }

    /**
     * Sets local network interface name form with remote data center is reachable. See {@link #getLocalOutboundHost()}
     * for more information.
     *
     * @param locOutboundHost Local network interface name form with remote data center is reachable.
     */
    public void setLocalOutboundHost(String locOutboundHost) {
        this.locOutboundHost = locOutboundHost;
    }

    /**
     * Gets remote receiver hub load balancing policy. This policy provides balancing mechanism in case remote data
     * center has several receiver hubs.
     * <p>
     * Defaults to {@link #DFLT_RCV_HUB_LOAD_BALANCING_MODE}.
     *
     * @return Remote receiver hub load balancing policy.
     */
    public GridDrReceiverHubLoadBalancingMode getReceiverHubLoadBalancingMode() {
        return rcvHubLoadBalancingMode;
    }

    /**
     * Sets remote receiver hub load balancing policy. See {@link #getReceiverHubLoadBalancingMode()}
     * for more information.
     *
     * @param rcvHubLoadBalancingMode Remote receiver hub load balancing policy.
     */
    public void setReceiverHubLoadBalancingMode(GridDrReceiverHubLoadBalancingMode rcvHubLoadBalancingMode) {
        this.rcvHubLoadBalancingMode = rcvHubLoadBalancingMode;
    }

    /**
     * Gets IDs of data centers updates from which will not be replicated to this remote data center.
     * Use this parameter to avoid cycles in replication. For example, if you have setup your replication
     * in a way that {@code A} replicates to {@code B}, {@code B} replicates to {@code C}, and {@code C}
     * replicates back to {@code A}, then on {@code C} you should specify that updates from {@code A}
     * should be ignored to avoid a cycle.
     *
     * @return Ignored data center IDs.
     */
    public byte[] getIgnoredDataCenterIds() {
        return ignoreDataCenterIds;
    }

    /**
     * Sets ignored data center IDs. See {@link #getIgnoredDataCenterIds()}
     *
     * @param ignoreDataCenterIds Ignored data center IDs.
     */
    public void setIgnoredDataCenterIds(byte... ignoreDataCenterIds) {
        this.ignoreDataCenterIds = ignoreDataCenterIds;
    }

    /**
     * Gets await acknowledge flag. When set to {@code true} sender hub will await acknowledge for each replication
     * batch sent to a receiver hub thus ensuring that data is stored in remote topology. If set to {@code false},
     * sender hub will not wait for that acknowledge. This will increase throughput at a cost of reliability.
     * <p>
     * Defaults to {@link #DFLT_AWAIT_ACK}.
     *
     * @return Await acknowledge flag.
     */
    public boolean isAwaitAcknowledge() {
        return awaitAck;
    }

    /**
     * Sets await acknowledge flag. See {@link #isAwaitAcknowledge()}
     *
     * @param awaitAck Await acknowledge flag.
     */
    public void setAwaitAcknowledge(boolean awaitAck) {
        this.awaitAck = awaitAck;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrSenderHubConnectionConfiguration.class, this);
    }
}
