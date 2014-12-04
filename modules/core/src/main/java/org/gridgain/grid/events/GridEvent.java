/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link GridEvents#remoteQuery(GridPredicate , long, int...)} - querying
 *          events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link GridEvents#localQuery(GridPredicate, int...)} - querying only local
 *          events stored on this local node.
 *      </li>
 *      <li>
 *          {@link GridEvents#localListen(GridPredicate, int...)} - listening
 *          to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using either {@link org.gridgain.grid.IgniteConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * <h1 class="header">Internal and Hidden Events</h1>
 * Also note that some events are considered to be internally used or hidden.
 * <p>
 * Internally used events are always "recordable" for notification purposes (regardless of whether they were
 * enabled or disabled). But won't be sent down to SPI level if user specifically excluded them.
 * <p>
 * All discovery events are internal:
 * <ul>
 *     <li>{@link GridEventType#EVT_NODE_FAILED}</li>
 *     <li>{@link GridEventType#EVT_NODE_LEFT}</li>
 *     <li>{@link GridEventType#EVT_NODE_JOINED}</li>
 *     <li>{@link GridEventType#EVT_NODE_METRICS_UPDATED}</li>
 *     <li>{@link GridEventType#EVT_NODE_SEGMENTED}</li>
 * </ul>
 * <p>
 * Hidden events are NEVER sent to SPI level. They serve purpose of local
 * notification for the local node.
 * <p>
 * Hidden events:
 * <ul>
 *     <li>{@link GridEventType#EVT_NODE_METRICS_UPDATED}</li>
 * </ul>
 * @see GridJobEvent
 * @see GridCacheEvent
 * @see GridCachePreloadingEvent
 * @see GridSwapSpaceEvent
 * @see GridCheckpointEvent
 * @see GridDeploymentEvent
 * @see GridDiscoveryEvent
 * @see GridTaskEvent
 * @see GridEvents#waitForLocal(GridPredicate, int...)
 */
public interface GridEvent extends Comparable<GridEvent>, Serializable {
    /**
     * Gets globally unique ID of this event.
     *
     * @return Globally unique ID of this event.
     * @see #localOrder()
     */
    public GridUuid id();

    /**
     * Gets locally unique ID that is atomically incremented for each event. Unlike
     * global {@link #id} this local ID can be used for ordering events on this node.
     * <p>
     * Note that for performance considerations GridGain doesn't order events globally.
     *
     * @return Locally unique ID that is atomically incremented for each new event.
     * @see #id()
     */
    public long localOrder();

    /**
     * Node where event occurred and was recorded
     *
     * @return node where event occurred and was recorded.
     */
    public ClusterNode node();

    /**
     * Gets optional message for this event.
     *
     * @return Optional (can be {@code null}) message for this event.
     */
    @Nullable public String message();

    /**
     * Gets type of this event. All system event types are defined in
     * {@link GridEventType}.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal GridGain events and should not be used by user-defined events.
     *
     * @return Event's type.
     * @see GridEventType
     */
    public int type();

    /**
     * Gets name of this event. All events are defined in {@link GridEventType} class.
     *
     * @return Name of this event.
     */
    public String name();

    /**
     * Gets event timestamp. Timestamp is local to the node on which this
     * event was produced. Note that more than one event can be generated
     * with the same timestamp. For ordering purposes use {@link #localOrder()} instead.
     *
     * @return Event timestamp.
     */
    public long timestamp();

    /**
     * Gets a shortened version of {@code toString()} result. Suitable for humans to read.
     *
     * @return Shortened version of {@code toString()} result.
     */
    public String shortDisplay();
}
