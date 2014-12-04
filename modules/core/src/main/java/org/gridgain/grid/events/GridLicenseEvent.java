/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Grid license event.
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
 * by using {@link GridConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * @see GridEventType#EVT_LIC_CLEARED
 * @see GridEventType#EVT_LIC_GRACE_EXPIRED
 * @see GridEventType#EVT_LIC_VIOLATION
 */
public class GridLicenseEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** License ID. */
    private UUID licId;

    /**
     * No-arg constructor.
     */
    public GridLicenseEvent() {
        // No-op.
    }

    /**
     * Creates license event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridLicenseEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
    }

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": licId8=" + U.id8(licId) + ", msg=" + message();
    }

    /**
     * Gets license ID.
     *
     * @return License ID.
     */
    public UUID licenseId() {
        return licId;
    }

    /**
     * Sets license ID.
     *
     * @param licId License ID to set.
     */
    public void licenseId(UUID licId) {
        this.licId = licId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridLicenseEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
