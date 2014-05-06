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
     */
    public GridHadoopJobStatus(GridHadoopJobId jobId, GridHadoopJobState jobState, String jobName, String usr) {
        this.jobId = jobId;
        this.jobState = jobState;
        this.jobName = jobName;
        this.usr = usr;
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
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobState = (GridHadoopJobState)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
    }
}
