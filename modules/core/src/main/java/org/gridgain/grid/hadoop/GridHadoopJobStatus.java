/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop job status.
 */
public class GridHadoopJobStatus implements Externalizable {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job state. */
    private GridHadoopJobState jobState;

    /** Job name. */
    private String jobName;

    /** User. */
    private String usr;

    /** Pending split count. */
    private int pendingSplitCnt;

    /** Pending reducer count. */
    private int pendingReducerCnt;

    /** Total split count. */
    private int totalSplitCnt;

    /** Total reducer count. */
    private int totalReducerCnt;

    /** Phase. */
    private GridHadoopJobPhase jobPhase;

    /**
     * {@link Externalizable}  support.
     */
    public GridHadoopJobStatus() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param jobId Job ID.
     * @param jobState Job state.
     * @param jobName Job name.
     * @param usr User.
     * @param pendingSplitCnt Pending split count.
     * @param pendingReducerCnt Pending reducer count.
     * @param totalSplitCnt Total split count.
     * @param totalReducerCnt Total reducer count.
     * @param jobPhase Job phase.
     */
    public GridHadoopJobStatus(GridHadoopJobId jobId, GridHadoopJobState jobState, String jobName, String usr,
        int pendingSplitCnt, int pendingReducerCnt, int totalSplitCnt, int totalReducerCnt,
        GridHadoopJobPhase jobPhase) {
        this.jobId = jobId;
        this.jobState = jobState;
        this.jobName = jobName;
        this.usr = usr;
        this.pendingSplitCnt = pendingSplitCnt;
        this.pendingReducerCnt = pendingReducerCnt;
        this.totalSplitCnt = totalSplitCnt;
        this.totalReducerCnt = totalReducerCnt;
        this.jobPhase = jobPhase;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job state.
     */
    public GridHadoopJobState jobState() {
        return jobState;
    }

    /**
     * @return Job name.
     */
    public String jobName() {
        return jobName;
    }

    /**
     * @return User.
     */
    public String user() {
        return usr;
    }

    /**
     * @return Pending split count.
     */
    public int pendingSplitCnt() {
        return pendingSplitCnt;
    }

    /**
     * @return Pending reducer count.
     */
    public int pendingReducerCnt() {
        return pendingReducerCnt;
    }

    /**
     * @return Total split count.
     */
    public int totalSplitCnt() {
        return totalSplitCnt;
    }

    /**
     * @return Total reducer count.
     */
    public int totalReducerCnt() {
        return totalReducerCnt;
    }

    /**
     * @return Split progress.
     */
    public float splitProgress() {
        return totalSplitCnt == 0 ? 1.0f : (float)(totalSplitCnt - pendingSplitCnt) / totalSplitCnt;
    }

    /**
     * @return Reducer progress.
     */
    public float reducerProgress() {
        return totalReducerCnt == 0 ? 1.0f : (float)(totalReducerCnt - pendingReducerCnt) / totalReducerCnt;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopJobStatus.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        out.writeObject(jobState);
        U.writeString(out, jobName);
        U.writeString(out, usr);
        out.writeInt(pendingSplitCnt);
        out.writeInt(pendingReducerCnt);
        out.writeInt(totalSplitCnt);
        out.writeInt(totalReducerCnt);
        out.writeObject(jobPhase);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobState = (GridHadoopJobState)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
        pendingSplitCnt = in.readInt();
        pendingReducerCnt = in.readInt();
        totalSplitCnt = in.readInt();
        totalReducerCnt = in.readInt();
        jobPhase = (GridHadoopJobPhase)in.readObject();
    }
}
