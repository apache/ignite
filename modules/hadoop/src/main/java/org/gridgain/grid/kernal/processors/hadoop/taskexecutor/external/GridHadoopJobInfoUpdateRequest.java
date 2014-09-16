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
 * Job info update request.
 */
public class GridHadoopJobInfoUpdateRequest implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private GridHadoopJobId jobId;

    /** Job phase. */
    @GridToStringInclude
    private GridHadoopJobPhase jobPhase;

    /** Reducers addresses. */
    @GridToStringInclude
    private GridHadoopProcessDescriptor[] reducersAddrs;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public GridHadoopJobInfoUpdateRequest() {
        // No-op.
    }

    /**
     * @param jobId Job ID.
     * @param jobPhase Job phase.
     * @param reducersAddrs Reducers addresses.
     */
    public GridHadoopJobInfoUpdateRequest(GridHadoopJobId jobId, GridHadoopJobPhase jobPhase,
        GridHadoopProcessDescriptor[] reducersAddrs) {
        assert jobId != null;

        this.jobId = jobId;
        this.jobPhase = jobPhase;
        this.reducersAddrs = reducersAddrs;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /**
     * @return Reducers addresses.
     */
    public GridHadoopProcessDescriptor[] reducersAddresses() {
        return reducersAddrs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobPhase);
        U.writeArray(out, reducersAddrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new GridHadoopJobId();
        jobId.readExternal(in);

        jobPhase = (GridHadoopJobPhase)in.readObject();
        reducersAddrs = (GridHadoopProcessDescriptor[])U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopJobInfoUpdateRequest.class, this);
    }
}
