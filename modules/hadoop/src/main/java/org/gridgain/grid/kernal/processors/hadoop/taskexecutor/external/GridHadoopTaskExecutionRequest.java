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
import java.util.*;

/**
 * Message sent from node to child process to start task(s) execution.
 */
public class GridHadoopTaskExecutionRequest implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private GridHadoopJobId jobId;

    /** Job info. */
    @GridToStringInclude
    private GridHadoopJobInfo jobInfo;

    /** Mappers. */
    @GridToStringInclude
    private Collection<GridHadoopTaskInfo> tasks;

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param jobId Job ID.
     */
    public void jobId(GridHadoopJobId jobId) {
        this.jobId = jobId;
    }

    /**
     * @return Jon info.
     */
    public GridHadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * @param jobInfo Job info.
     */
    public void jobInfo(GridHadoopJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    /**
     * @return Tasks.
     */
    public Collection<GridHadoopTaskInfo> tasks() {
        return tasks;
    }

    /**
     * @param tasks Tasks.
     */
    public void tasks(Collection<GridHadoopTaskInfo> tasks) {
        this.tasks = tasks;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskExecutionRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobInfo);
        U.writeCollection(out, tasks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new GridHadoopJobId();
        jobId.readExternal(in);

        jobInfo = (GridHadoopJobInfo)in.readObject();
        tasks = U.readCollection(in);
    }
}
