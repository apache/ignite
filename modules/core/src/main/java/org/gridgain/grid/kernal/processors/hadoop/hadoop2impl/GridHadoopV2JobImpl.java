/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Hadoop job implementation for v2 API.
 */
public class GridHadoopV2JobImpl implements GridHadoopJob {
    /** Hadoop job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    private GridHadoopJobInfoImpl jobInfoImpl;

    /**
     * @param jobId Job ID.
     * @param jobInfoImpl Job info.
     */
    public GridHadoopV2JobImpl(GridHadoopJobId jobId, GridHadoopJobInfoImpl jobInfoImpl) {
        this.jobId = jobId;
        this.jobInfoImpl = jobInfoImpl;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId id() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobInfo info() {
        return jobInfoImpl;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopFileBlock> input() throws GridException {
        return GridHadoopV2Splitter.splitJob(jobId, jobInfoImpl);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        Class<? extends Partitioner> partCls = (Class<? extends Partitioner>)jobInfoImpl.configuration().getClass(
            MRJobConfig.PARTITIONER_CLASS_ATTR, HashPartitioner.class);

        return new GridHadoopV2PartitionerAdapter((Partitioner<Object, Object>)U.newInstance(partCls));
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
