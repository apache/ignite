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
 * Grid task event.
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
 * @see GridEventType#EVT_TASK_FAILED
 * @see GridEventType#EVT_TASK_FINISHED
 * @see GridEventType#EVT_TASK_REDUCED
 * @see GridEventType#EVT_TASK_STARTED
 * @see GridEventType#EVT_TASK_SESSION_ATTR_SET
 * @see GridEventType#EVT_TASK_TIMEDOUT
 * @see GridEventType#EVTS_TASK_EXECUTION
 */
public class GridTaskEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String taskName;

    /** */
    private String taskClsName;

    /** */
    private GridUuid sesId;

    /** */
    private boolean internal;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": taskName=" + taskName;
    }

    /**
     * No-arg constructor.
     */
    public GridTaskEvent() {
        // No-op.
    }

    /**
     * Creates task event with given parameters.
     *
     * @param nodeId Node ID.
     * @param msg Optional message.
     * @param type Event type.
     * @param sesId Task session ID.
     * @param taskName Task name.
     */
    public GridTaskEvent(UUID nodeId, String msg, int type, GridUuid sesId, String taskName) {
        super(nodeId, msg, type);

        this.sesId = sesId;
        this.taskName = taskName;
    }

    /**
     * Creates task event with given parameters.
     *
     * @param nodeId Node ID.
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridTaskEvent(UUID nodeId, String msg, int type) {
        super(nodeId, msg, type);
    }

    /**
     * Gets name of the task that triggered the event.
     *
     * @return Name of the task that triggered the event.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * Gets name of task class that triggered this event.
     *
     * @return Name of task class that triggered the event.
     */
    public String taskClassName() {
        return taskClsName;
    }

    /**
     * Gets session ID of the task that triggered the event.
     *
     * @return Session ID of the task that triggered the event.
     */
    public GridUuid taskSessionId() {
        return sesId;
    }

    /**
     * Sets the task name.
     *
     * @param taskName Task name to set.
     */
    public void taskName(String taskName) {
        assert taskName != null;

        this.taskName = taskName;
    }

    /**
     * Sets name of the task class that triggered this event.
     *
     * @param taskClsName Task class name to set.
     */
    public void taskClassName(String taskClsName) {
        this.taskClsName = taskClsName;
    }

    /**
     * Sets task session ID.
     *
     * @param sesId Task session ID to set.
     */
    public void taskSessionId(GridUuid sesId) {
        assert sesId != null;

        this.sesId = sesId;
    }

    /**
     * Set to {@code true} if task is created by GridGain and is used for system needs.
     *
     * @param internal {@code True} if task is created by GridGain and is used for system needs.
     */
    public void internal(boolean internal) {
        this.internal = internal;
    }

    /**
     * Returns {@code true} if task is created by GridGain and is used for system needs.
     *
     * @return {@code True} if task is created by GridGain and is used for system needs.
     */
    public boolean internal() {
        return internal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskEvent.class, this,
            "nodeId8", U.id8(nodeId()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
