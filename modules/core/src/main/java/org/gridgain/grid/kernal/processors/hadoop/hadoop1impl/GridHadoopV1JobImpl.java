/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.util.Collection;

/**
 * Hadoop job implementation for v1 API.
 */
public class GridHadoopV1JobImpl implements GridHadoopJob {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopDefaultJobInfo jobInfo;

    /** Hadoop job context. */
    private JobContext ctx;

    public GridHadoopV1JobImpl(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;

        ctx = new JobContextImpl((JobConf)jobInfo.configuration(), new JobID(jobId.globalId().toString(), jobId.localId()));
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId id() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobInfo info() {
        return jobInfo;
    }

    @Override
    public Collection<GridHadoopFileBlock> input() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return ctx.getNumReduceTasks();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
        switch (taskInfo.type()) {
            case MAP:
                return new GridHadoopV1MapTask(taskInfo);

            case REDUCE:
                return new GridHadoopV1ReduceTask(taskInfo);

            case COMBINE:
                return new GridHadoopV1CombineTask(taskInfo);

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return combinerClass() != null;
    }

    /**
     * Gets combiner class.
     *
     * @return Combiner class or {@code null} if combiner is not specified.
     */
    private Class<? extends org.apache.hadoop.mapreduce.Reducer<?,?,?,?>> combinerClass() {
        try {
            return ctx.getCombinerClass();
        }
        catch (ClassNotFoundException e) {
            // TODO check combiner class at initialization and throw meaningful exception.
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization serialization() throws GridException {
        // TODO implement.
        return null;
    }

    /** Hadoop native job context. */
    public JobContext hadoopJobContext() {
        return ctx;
    }
}
