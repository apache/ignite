/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.events;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Grid task event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
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
    private final String taskName;

    /** */
    private final String taskClsName;

    /** */
    private final IgniteUuid sesId;

    /** */
    private final boolean internal;

    /**  */
    private final UUID subjId;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": taskName=" + taskName;
    }

    /**
     * Creates task event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     * @param sesId Task session ID.
     * @param taskName Task name.
     * @param subjId Subject ID.
     */
    public GridTaskEvent(ClusterNode node, String msg, int type, IgniteUuid sesId, String taskName, String taskClsName,
        boolean internal, @Nullable UUID subjId) {
        super(node, msg, type);

        this.sesId = sesId;
        this.taskName = taskName;
        this.taskClsName = taskClsName;
        this.internal = internal;
        this.subjId = subjId;
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
    public IgniteUuid taskSessionId() {
        return sesId;
    }

    /**
     * Returns {@code true} if task is created by GridGain and is used for system needs.
     *
     * @return {@code True} if task is created by GridGain and is used for system needs.
     */
    public boolean internal() {
        return internal;
    }

    /**
     * Gets security subject ID initiated this task event, if available. This property
     * is not available for GridEventType#EVT_TASK_SESSION_ATTR_SET task event.
     * <p>
     * Subject ID will be set either to node ID or client ID initiated
     * task execution.
     *
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
