/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * The helper to work with values of the job statistics counter {@link GridHadoopStatCounter}.
 *
 * The class is not serializable. The events are serialized manually in counter.
 */
public class GridHadoopJobStatistics {
    /** Events collections. */
    private final Collection<T2<String,Long>> evts;

    /** Node id to insert into the event info. */
    private UUID nodeId;

    /**
     * Default constructor.
     */
    public GridHadoopJobStatistics() {
        evts = new ArrayList<>();
    }

    /**
     * Constructor.
     *
     * @param evts Collection of statistics events.
     */
    public GridHadoopJobStatistics(Collection<T2<String,Long>> evts) {
        this.evts = evts;
    }

    public GridHadoopJobStatistics(UUID nodeId) {
        this();

        this.nodeId = nodeId;
    }

    /**
     * Gets collection of statistic events.
     *
     * @return Collection of statistic events.
     */
    public Collection<T2<String,Long>> evts() {
        return evts;
    }

    /**
     * Generate name that consists of some event information.
     *
     * @param info Task info.
     * @param evtType The type of the event.
     * @return String contains necessary event information.
     */
    private String eventName(GridHadoopTaskInfo info, String evtType) {
        return info.type() + " " + info.taskNumber() + " " + evtType + " " + nodeId;
    }

    /**
     * Adds event of the task submission (task instance creation).
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskSubmit(GridHadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "submit"), ts));
    }

    /**
     * Adds event of the task preparation.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskPrepare(GridHadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "prepare"), ts));
    }

    /**
     * Adds event of the task finish.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskFinish(GridHadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "finish"), ts));
    }

    /**
     * Adds event of the task run.
     *
     * @param info Task info.
     * @param ts Timestamp of the event.
     */
    public void onTaskRun(GridHadoopTaskInfo info, long ts) {
        evts.add(new T2<>(eventName(info, "run"), ts));
    }

    /**
     * Adds event of the job preparation.
     *
     * @param ts Timestamp of the event.
     */
    public void onJobPrepare(long ts) {
        evts.add(new T2<>("JOB prepare " + nodeId, ts));
    }

    /**
     * Adds event of the job start.
     *
     * @param ts Timestamp of the event.
     */
    public void onJobStart(long ts) {
        evts.add(new T2<>("JOB start " + nodeId, ts));
    }

    /**
     * Adds event of the job finish.
     *
     * @param ts Timestamp of the event.
     */
    public void onJobFinish(long ts) {
        evts.add(new T2<>("JOB finish " + nodeId, ts));
    }

    /**
     * Adds client submission events from job info.
     *
     * @param info Job info.
     */
    public void clientSubmissionEvents(GridHadoopJobInfo info) {
        try {
            evts.add(new T2<>("JOB requestId " + nodeId,
                Long.parseLong(info.property(REQ_NEW_JOBID_TS_PROPERTY))));
        }
        catch (NumberFormatException ignore) {
        }

        try {
            evts.add(new T2<>("JOB responseId " + nodeId,
                Long.parseLong(info.property(RESPONSE_NEW_JOBID_TS_PROPERTY))));
        }
        catch (NumberFormatException ignore) {
        }

        try {
            evts.add(new T2<>("JOB submit " + nodeId,
                Long.parseLong(info.property(JOB_SUBMISSION_START_TS_PROPERTY))));
        }
        catch (NumberFormatException ignore) {
        }
    }
}
