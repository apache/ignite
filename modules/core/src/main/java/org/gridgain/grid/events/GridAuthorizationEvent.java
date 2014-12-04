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
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid authorization event.
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
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link GridConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * @see GridEventType#EVT_AUTHORIZATION_FAILED
 * @see GridEventType#EVT_AUTHORIZATION_SUCCEEDED
 */
public class GridAuthorizationEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Requested operation. */
    private GridSecurityPermission op;

    /** Authenticated subject authorized to perform operation. */
    private GridSecuritySubject subj;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": op=" + op;
    }

    /**
     * No-arg constructor.
     */
    public GridAuthorizationEvent() {
        // No-op.
    }

    /**
     * Creates authorization event with given parameters.
     *
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridAuthorizationEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
    }

    /**
     * Creates authorization event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     * @param op Requested operation.
     * @param subj Authenticated subject.
     */
    public GridAuthorizationEvent(ClusterNode node, String msg, int type, GridSecurityPermission op,
        GridSecuritySubject subj) {
        super(node, msg, type);

        this.op = op;
        this.subj = subj;
    }

    /**
     * Gets requested operation.
     *
     * @return Requested operation.
     */
    public GridSecurityPermission operation() {
        return op;
    }

    /**
     * Sets requested operation.
     *
     * @param op Requested operation.
     */
    public void operation(GridSecurityPermission op) {
        this.op = op;
    }

    /**
     * Gets authenticated subject.
     *
     * @return Authenticated subject.
     */
    public GridSecuritySubject subject() {
        return subj;
    }

    /**
     * Sets authenticated subject.
     *
     * @param subj Authenticated subject.
     */
    public void subject(GridSecuritySubject subj) {
        this.subj = subj;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAuthorizationEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
