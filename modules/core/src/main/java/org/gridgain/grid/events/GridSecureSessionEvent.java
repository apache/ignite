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
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Grid secure session validation event.
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
 * @see GridEventType#EVT_SECURE_SESSION_VALIDATION_FAILED
 * @see GridEventType#EVT_SECURE_SESSION_VALIDATION_SUCCEEDED
 */
public class GridSecureSessionEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /**  Subject type. */
    private GridSecuritySubjectType subjType;

    /** Subject ID. */
    private UUID subjId;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": subjType=" + subjType;
    }

    /**
     * No-arg constructor.
     */
    public GridSecureSessionEvent() {
        // No-op.
    }

    /**
     * Creates secure session event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridSecureSessionEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
    }

    /**
     * Creates secure session event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     * @param subjType Subject type.
     * @param subjId Subject ID.
     */
    public GridSecureSessionEvent(ClusterNode node, String msg, int type, GridSecuritySubjectType subjType,
        UUID subjId) {
        super(node, msg, type);

        this.subjType = subjType;
        this.subjId = subjId;
    }

    /**
     * Gets subject type that triggered the event.
     *
     * @return Subject type that triggered the event.
     */
    public GridSecuritySubjectType subjectType() {
        return subjType;
    }

    /**
     * Gets subject ID that triggered the event.
     *
     * @return Subject ID that triggered the event.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * Sets subject type that triggered the event.
     *
     * @param subjType Subject type to set.
     */
    public void subjectType(GridSecuritySubjectType subjType) {
        this.subjType = subjType;
    }

    /**
     * Sets subject ID that triggered the event.
     *
     * @param subjId Subject ID to set.
     */
    public void subjectId(UUID subjId) {
        this.subjId = subjId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSecureSessionEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
