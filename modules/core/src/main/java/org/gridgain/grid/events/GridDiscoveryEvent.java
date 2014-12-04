/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Grid discovery event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link GridEvents#remoteQuery(GridPredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link GridEvents#localQuery(GridPredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link GridEvents#localListen(GridPredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link GridEvents#waitForLocal(GridPredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * @see GridEventType#EVT_NODE_METRICS_UPDATED
 * @see GridEventType#EVT_NODE_FAILED
 * @see GridEventType#EVT_NODE_JOINED
 * @see GridEventType#EVT_NODE_LEFT
 * @see GridEventType#EVT_NODE_SEGMENTED
 * @see GridEventType#EVTS_DISCOVERY_ALL
 * @see GridEventType#EVTS_DISCOVERY
 */
public class GridDiscoveryEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ClusterNode evtNode;

    /** Topology version. */
    private long topVer;

    /** Collection of nodes corresponding to topology version. */
    private Collection<ClusterNode> topSnapshot;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": id8=" + U.id8(evtNode.id()) + ", ip=" + F.first(evtNode.addresses());
    }

    /**
     * No-arg constructor.
     */
    public GridDiscoveryEvent() {
        // No-op.
    }

    /**
     * Creates new discovery event with given parameters.
     *
     * @param node Local node.
     * @param msg Optional event message.
     * @param type Event type.
     * @param evtNode Node that caused this event to be generated.
     */
    public GridDiscoveryEvent(ClusterNode node, String msg, int type, ClusterNode evtNode) {
        super(node, msg, type);

        this.evtNode = evtNode;
    }

    /**
     * Sets node this event is referring to.
     *
     * @param evtNode Event node.
     */
    public void eventNode(ClusterNode evtNode) {
        this.evtNode = evtNode;
    }

    /**
     * Gets node that caused this event to be generated. It is potentially different from the node
     * on which this event was recorded. For example, node {@code A} locally recorded the event that a remote node
     * {@code B} joined the topology. In this case this method will return ID of {@code B}.
     *
     * @return Event node ID.
     */
    public ClusterNode eventNode() {
        return evtNode;
    }

    /**
     * Gets topology version if this event is raised on
     * topology change and configured discovery SPI implementation
     * supports topology versioning.
     *
     * @return Topology version or {@code 0} if configured discovery SPI implementation
     *      does not support versioning.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * Gets topology nodes from topology snapshot. If SPI implementation does not support
     * versioning, the best effort snapshot will be captured.
     *
     * @return Topology snapshot.
     */
    public Collection<ClusterNode> topologyNodes() {
        return topSnapshot;
    }

    /**
     * Sets the topology snapshot.
     *
     * @param topVer Topology version.
     * @param topSnapshot Topology snapshot.
     */
    public void topologySnapshot(long topVer, Collection<ClusterNode> topSnapshot) {
        this.topVer = topVer;
        this.topSnapshot = topSnapshot;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
