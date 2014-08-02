/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Context for task execution.
 */
public class GridHadoopV2TaskContext extends GridHadoopTaskContext {
    /** */
    private static final boolean COMBINE_KEY_GROUPING_SUPPORTED;

    /**
     * Check for combiner grouping support (available since Hadoop 2.3).
     */
    static {
        boolean ok;

        try {
            JobContext.class.getDeclaredMethod("getCombinerKeyGroupingComparator");

            ok = true;
        }
        catch (NoSuchMethodException ignore) {
            ok = false;
        }

        COMBINE_KEY_GROUPING_SUPPORTED = ok;
    }

    /** */
    private final JobContextImpl jobCtx;

    /**
     * @param taskInfo Task info.
     * @param job Job.
     * @param jobCtx Job context.
     */
    public GridHadoopV2TaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job, JobContextImpl jobCtx) {
        super(taskInfo, job);

        this.jobCtx = jobCtx;
    }

    /**
     * Gets job configuration of the task.
     *
     * @return Job configuration.
     */
    public JobConf jobConf() {
        return jobCtx.getJobConf();
    }

    /**
     * Gets job context of the task.
     *
     * @return Job context.
     */
    public JobContextImpl jobContext() {
        return jobCtx;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        Class<?> partClsOld = jobConf().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new GridHadoopV1Partitioner(jobConf().getPartitionerClass(), jobConf());

        try {
            return new GridHadoopV2Partitioner(jobCtx.getPartitionerClass(), jobConf());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /**
     * Gets serializer for specified class.
     *
     * @param cls Class.
     * @param jobConf Job configuration.
     * @return Appropriate serializer.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopSerialization getSerialization(Class<?> cls, Configuration jobConf) throws GridException {
        A.notNull(cls, "cls");

        SerializationFactory factory = new SerializationFactory(jobConf);

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization == null)
            throw new GridException("Failed to find serialization for: " + cls.getName());

        if (serialization.getClass() == WritableSerialization.class)
            return new GridHadoopWritableSerialization((Class<? extends Writable>)cls);

        return new GridHadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization keySerialization() throws GridException {
        return getSerialization(jobCtx.getMapOutputKeyClass(), jobConf());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization valueSerialization() throws GridException {
        return getSerialization(jobCtx.getMapOutputValueClass(), jobConf());
    }

    /** {@inheritDoc} */
    @Override public Comparator<Object> sortComparator() {
        return (Comparator<Object>)jobCtx.getSortComparator();
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment() throws GridException {
        job().prepareTaskEnvironment(taskInfo());

        Thread.currentThread().setContextClassLoader(jobConf().getClassLoader());
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment() throws GridException {
        Thread.currentThread().setContextClassLoader(null);

        job().cleanupTaskEnvironment(taskInfo());
    }

    /** {@inheritDoc} */
    @Override public Comparator<Object> groupComparator() {
        Comparator<?> res;

        switch (taskInfo().type()) {
            case COMBINE:
                res = COMBINE_KEY_GROUPING_SUPPORTED ?
                    jobContext().getCombinerKeyGroupingComparator() : jobContext().getGroupingComparator();

                break;

            case REDUCE:
                res = jobContext().getGroupingComparator();

                break;

            default:
                return null;
        }

        if (res != null && res.getClass() != sortComparator().getClass())
            return (Comparator<Object>)res;

        return null;
    }

    /**
     * @param split Split.
     * @return Native Hadoop split.
     * @throws GridException if failed.
     */
    @SuppressWarnings("unchecked")
    public Object getNativeSplit(GridHadoopInputSplit split) throws GridException {
        if (split instanceof GridHadoopExternalSplit)
            return readExternalSplit((GridHadoopExternalSplit)split);

        if (split instanceof GridHadoopSplitWrapper)
            return ((GridHadoopSplitWrapper)split).innerSplit();

        throw new IllegalStateException("Unknown split: " + split);
    }

    /**
     * @param split External split.
     * @return Native input split.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object readExternalSplit(GridHadoopExternalSplit split) throws GridException {
        Path jobDir = new Path(jobConf().get(MRJobConfig.MAPREDUCE_JOB_DIR));

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf());
            FSDataInputStream in = fs.open(JobSubmissionFiles.getJobSplitFile(jobDir))) {

            in.seek(split.offset());

            String clsName = Text.readString(in);

            Class<?> cls = jobConf().getClassByName(clsName);

            assert cls != null;

            Serialization serialization = new SerializationFactory(jobConf()).getSerialization(cls);

            Deserializer deserializer = serialization.getDeserializer(cls);

            deserializer.open(in);

            Object res = deserializer.deserialize(null);

            deserializer.close();

            assert res != null;

            return res;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopV2Job job() {
        return (GridHadoopV2Job)super.job();
    }
}
