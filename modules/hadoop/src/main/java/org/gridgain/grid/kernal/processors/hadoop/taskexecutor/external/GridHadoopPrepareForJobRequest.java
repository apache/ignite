/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Child process initialization request.
 */
public class GridHadoopPrepareForJobRequest implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private GridHadoopJobId jobId;

    /** Job info. */
    @GridToStringInclude
    private GridHadoopJobInfo jobInfo;

    /** Has mappers flag. */
    @GridToStringInclude
    private boolean hasMappers;

    /** Total amount of reducers in job. */
    @GridToStringInclude
    private int reducerCnt;

    /** Reducers to be executed on current node. */
    private int[] reducers;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public GridHadoopPrepareForJobRequest() {
        // No-op.
    }

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     * @param hasMappers Has mappers flag.
     * @param reducerCnt Number of reducers in job.
     * @param reducers Reducers to be executed on current node.
     */
    public GridHadoopPrepareForJobRequest(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo, boolean hasMappers,
        int reducerCnt, int[] reducers) {
        assert jobId != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.hasMappers = hasMappers;
        this.reducerCnt = reducerCnt;
        this.reducers = reducers;
    }

    /**
     * @return Job info.
     */
    public GridHadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Has mappers flag.
     */
    public boolean hasMappers() {
        return hasMappers;
    }

    /**
     * @return Reducers to be executed on current node.
     */
    public int[] reducers() {
        return reducers;
    }

    /**
     * @return Number of reducers in job.
     */
    public int reducerCount() {
        return reducerCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobInfo);
        out.writeBoolean(hasMappers);
        out.writeInt(reducerCnt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new GridHadoopJobId();
        jobId.readExternal(in);

        jobInfo = (GridHadoopJobInfo)in.readObject();
        hasMappers = in.readBoolean();
        reducerCnt = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopPrepareForJobRequest.class, this);
    }
}
