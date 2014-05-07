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

    /** Pending block count. */
    private int pendingBlockCnt;

    /** Pending reducer count. */
    private int pendingReducerCnt;

    /** Total block count. */
    private int totalBlockCnt;

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
     * @param pendingBlockCnt Pending block count.
     * @param pendingReducerCnt Pending reducer count.
     * @param totalBlockCnt Total block count.
     * @param totalReducerCnt Total reducer count.
     * @param jobPhase Job phase.
     */
    public GridHadoopJobStatus(GridHadoopJobId jobId, GridHadoopJobState jobState, String jobName, String usr,
        int pendingBlockCnt, int pendingReducerCnt, int totalBlockCnt, int totalReducerCnt,
        GridHadoopJobPhase jobPhase) {
        this.jobId = jobId;
        this.jobState = jobState;
        this.jobName = jobName;
        this.usr = usr;
        this.pendingBlockCnt = pendingBlockCnt;
        this.pendingReducerCnt = pendingReducerCnt;
        this.totalBlockCnt = totalBlockCnt;
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
     * @return Pending block count.
     */
    public int pendingBlockCnt() {
        return pendingBlockCnt;
    }

    /**
     * @return Pending reducer count.
     */
    public int pendingReducerCnt() {
        return pendingReducerCnt;
    }

    /**
     * @return Total block count.
     */
    public int totalBlockCnt() {
        return totalBlockCnt;
    }

    /**
     * @return Total reducer count.
     */
    public int totalReducerCnt() {
        return totalReducerCnt;
    }

    /**
     * @return Block progress.
     */
    public float blockProgress() {
        return totalBlockCnt == 0 ? 1.0f : (float)(totalBlockCnt - pendingBlockCnt) / totalBlockCnt;
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
        out.writeInt(pendingBlockCnt);
        out.writeInt(pendingReducerCnt);
        out.writeInt(totalBlockCnt);
        out.writeInt(totalReducerCnt);
        out.writeObject(jobPhase);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobState = (GridHadoopJobState)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
        pendingBlockCnt = in.readInt();
        pendingReducerCnt = in.readInt();
        totalBlockCnt = in.readInt();
        totalReducerCnt = in.readInt();
        jobPhase = (GridHadoopJobPhase)in.readObject();
    }
}
