// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Grid discovery event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link GridEvents#queryRemote(org.gridgain.grid.lang.GridPredicate, long)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link GridEvents#queryLocal(org.gridgain.grid.lang.GridPredicate[])} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link GridEvents#addLocalListener(GridLocalEventListener, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link GridEvents#waitForLocal(org.gridgain.grid.lang.GridPredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link GridConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 *
 * @author @java.author
 * @version @java.version
 * @see GridEventType#EVT_NODE_METRICS_UPDATED
 * @see GridEventType#EVT_NODE_FAILED
 * @see GridEventType#EVT_NODE_JOINED
 * @see GridEventType#EVT_NODE_LEFT
 * @see GridEventType#EVT_NODE_RECONNECTED
 * @see GridEventType#EVT_NODE_SEGMENTED
 * @see GridEventType#EVTS_DISCOVERY_ALL
 * @see GridEventType#EVTS_DISCOVERY
 */
public class GridDiscoveryEvent extends GridEventAdapter {
    /** */
    private UUID evtNodeId;

    /** */
    private GridNodeShadow shadow;

    /** Topology version. */
    private long topVer;

    /** Collection of node shadows corresponding to topology version. */
    private Collection<GridNodeShadow> topSnapshot;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": id8=" + U.id8(evtNodeId) +
            (shadow != null ? ", ip=" + F.first(shadow.addresses()) : "");
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
     * @param nodeId Local node ID.
     * @param msg Optional event message.
     * @param type Event type.
     * @param evtNodeId ID of the node that caused this event to be generated.
     */
    public GridDiscoveryEvent(UUID nodeId, String msg, int type, UUID evtNodeId) {
        super(nodeId, msg, type);

        this.evtNodeId = evtNodeId;
    }

    /**
     * Creates new discovery event with given parameters.
     *
     * @param nodeId Local node ID.
     * @param msg Optional event message.
     * @param type Event type.
     */
    public GridDiscoveryEvent(UUID nodeId, String msg, int type) {
        super(nodeId, msg, type);
    }

    /**
     * Sets ID of the node this event is referring to.
     *
     * @param evtNodeId Event node ID. Note that event node ID is different from node ID
     *      available via {@link #nodeId()} method.
     */
    public void eventNodeId(UUID evtNodeId) {
        this.evtNodeId = evtNodeId;
    }

    /**
     * Gets ID of the node that caused this event to be generated. It is potentially different from the node
     * on which this event was recorded. For example, node {@code A} locally recorded the event that a remote node
     * {@code B} joined the topology. In this case this method will return ID of {@code B} and
     * method {@link #nodeId()} will return ID of {@code A}
     *
     * @return Event node ID.
     */
    public UUID eventNodeId() {
        return evtNodeId;
    }

    /**
     * Sets node shadow.
     *
     * @param shadow Node shadow to set.
     */
    public void shadow(GridNodeShadow shadow) {
        this.shadow = shadow;
    }

    /**
     * Gets node shadow.
     *
     * @return Node shadow or {@code null} if one wasn't set.
     */
    @Nullable public GridNodeShadow shadow() {
        return shadow;
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
    public Collection<GridNodeShadow> topologyNodes() {
        return topSnapshot;
    }

    /**
     * Sets the topology snapshot.
     *
     * @param topSnapshot Topology snapshot.
     */
    public void topologySnapshot(long topVer, Collection<GridNodeShadow> topSnapshot) {
        this.topVer = topVer;
        this.topSnapshot = topSnapshot;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryEvent.class, this,
            "nodeId8", U.id8(nodeId()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
