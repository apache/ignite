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
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Grid job event.
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
 * @see GridEventType#EVT_JOB_CANCELLED
 * @see GridEventType#EVT_JOB_FAILED
 * @see GridEventType#EVT_JOB_FAILED_OVER
 * @see GridEventType#EVT_JOB_FINISHED
 * @see GridEventType#EVT_JOB_MAPPED
 * @see GridEventType#EVT_JOB_QUEUED
 * @see GridEventType#EVT_JOB_REJECTED
 * @see GridEventType#EVT_JOB_RESULTED
 * @see GridEventType#EVT_JOB_STARTED
 * @see GridEventType#EVT_JOB_TIMEDOUT
 * @see GridEventType#EVTS_JOB_EXECUTION
 */
public class GridJobEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String taskName;

    /** */
    private String taskClsName;

    /** */
    private GridUuid sesId;

    /** */
    private GridUuid jobId;

    /** */
    private ClusterNode taskNode;

    /** */
    private UUID taskSubjId;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": taskName=" + taskName;
    }

    /**
     * No-arg constructor.
     */
    public GridJobEvent() {
        // No-op.
    }

    /**
     * Creates job event with given parameters.
     *
     * @param node Node.
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridJobEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
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
     * Gets task session ID of the task that triggered this event.
     *
     * @return Task session ID of the task that triggered the event.
     */
    public GridUuid taskSessionId() {
        return sesId;
    }

    /**
     * Gets job ID.
     *
     * @return Job ID.
     */
    public GridUuid jobId() {
        return jobId;
    }

    /**
     * Sets name of the task that triggered this event.
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
     * Sets task session ID of the task that triggered this event.
     *
     * @param sesId Task session ID to set.
     */
    public void taskSessionId(GridUuid sesId) {
        assert sesId != null;

        this.sesId = sesId;
    }

    /**
     * Sets job ID.
     *
     * @param jobId Job ID to set.
     */
    public void jobId(GridUuid jobId) {
        assert jobId != null;

        this.jobId = jobId;
    }

    /**
     * Get node where parent task of the job has originated.
     *
     * @return Node where parent task of the job has originated.
     */
    public ClusterNode taskNode() {
        return taskNode;
    }

    /**
     * Sets node where parent task of the job has originated.
     *
     * @param taskNode Node where parent task of the job has originated.
     */
    public void taskNode(ClusterNode taskNode) {
        this.taskNode = taskNode;
    }

    /**
     * Gets task subject ID.
     *
     * @return Task subject ID.
     */
    public UUID taskSubjectId() {
        return taskSubjId;
    }

    /**
     * Sets task subject ID.
     *
     * @param taskSubjId Task subject ID.
     */
    public void taskSubjectId(UUID taskSubjId) {
        this.taskSubjId = taskSubjId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
