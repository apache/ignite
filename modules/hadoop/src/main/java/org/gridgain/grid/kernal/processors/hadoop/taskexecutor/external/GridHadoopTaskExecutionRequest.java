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
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Message sent from node to child process to start task(s) execution.
 */
public class GridHadoopTaskExecutionRequest extends GridHadoopMessage {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    private GridHadoopJobInfo jobInfo;

    /** Mappers. */
    private Collection<GridHadoopTaskInfo> tasks;

    /** Number of concurrently running mappers. */
    private int concurrentMappers;

    /** Number of concurrently running reducers. */
    private int concurrentReducers;

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

    /**
     * @return Number of concurrently running mappers.
     */
    public int concurrentMappers() {
        return concurrentMappers;
    }

    /**
     * @param concurrentMappers Number of concurrently running mappers.
     */
    public void concurrentMappers(int concurrentMappers) {
        this.concurrentMappers = concurrentMappers;
    }

    /**
     * @return Number of concurrently running reducers.
     */
    public int concurrentReducers() {
        return concurrentReducers;
    }

    /**
     * @param concurrentReducers Number of concurrently running reducers.
     */
    public void concurrentReducers(int concurrentReducers) {
        this.concurrentReducers = concurrentReducers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopTaskExecutionRequest.class, this);
    }
}
