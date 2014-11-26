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
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Counter for the job statistics accumulation.
 */
public class GridHadoopStatCounter extends GridHadoopCounterAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** The group name for this counter. */
    public static final String GROUP_NAME = "SYSTEM";

    /** The counter name for this counter. */
    public static final String COUNTER_NAME = "STATISTICS";

    /** Events collections. */
    private Collection<T2<String,Long>> evts = new ArrayList<>();

    /** Node id to insert into the event info. */
    private UUID nodeId;

    /** */
    private int reducerNum;

    /** */
    private Long firstShuffleMsg;

    /** */
    private volatile Long lastShuffleMsg;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopStatCounter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param grp Group name.
     * @param name Counter name.
     */
    public GridHadoopStatCounter(String grp, String name) {
        super(grp, name);
    }

    /**
     * Constructor to create instance to use this as helper.
     *
     * @param nodeId Id of the work node.
     */
    public GridHadoopStatCounter(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        U.writeCollection(out, evts);
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        try {
            evts = U.readCollection(in);
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void merge(GridHadoopCounter cntr) {
        if (lastShuffleMsg != null) {
            evts.add(new T2<>("SHUFFLE " + reducerNum + " start", firstShuffleMsg));
            evts.add(new T2<>("SHUFFLE " + reducerNum + " finish", lastShuffleMsg));

            lastShuffleMsg = null;
        }

        evts.addAll(((GridHadoopStatCounter) cntr).evts);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounter copy() {
        GridHadoopStatCounter cp = new GridHadoopStatCounter(group(), name());

        cp.merge(this);

        return cp;
    }

    public Collection<T2<String, Long>> evts() {
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

    /**
     * Registers shuffle message event.
     *
     * @param reducerNum Number of reducer that receives the data.
     * @param ts Timestamp of the event.
     */
    public void onShuffleMessage(int reducerNum, long ts) {
        this.reducerNum = reducerNum;

        if (firstShuffleMsg == null)
            firstShuffleMsg =ts;

        lastShuffleMsg = ts;
    }
}
