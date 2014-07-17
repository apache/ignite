package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.gridgain.grid.GridException;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Partitioner;
import org.gridgain.grid.util.typedef.internal.A;

import java.io.IOException;
import java.util.Comparator;

public class GridHadoopV2TaskContext extends GridHadoopTaskContext {
    /** */
    private static final boolean COMBINE_KEY_GROUPING_SUPPORTED;

    /**
     * Check for combiner grouping support (available since Hadoop 2.3).
     */
    static {
        boolean ok;

        try {
            org.apache.hadoop.mapreduce.JobContext.class.getDeclaredMethod("getCombinerKeyGroupingComparator");

            ok = true;
        }
        catch (NoSuchMethodException ignore) {
            ok = false;
        }

        COMBINE_KEY_GROUPING_SUPPORTED = ok;
    }

    /** */
    private JobContextImpl jobCtx;

    /**
     * @param taskInfo Task info.
     * @param job      Job.
     */
    public GridHadoopV2TaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job,
        JobContextImpl jobCtx) {
        super(taskInfo, job);
        this.jobCtx = jobCtx;
    }

    public JobConf jobConf() {
        return jobCtx.getJobConf();
    }

    public JobContextImpl jobContext() {
        return jobCtx;
    }

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

    @Override public Comparator<?> combineGroupComparator() {
        return COMBINE_KEY_GROUPING_SUPPORTED ? jobContext().getCombinerKeyGroupingComparator() : null;
    }

    @Override public Comparator<?> reduceGroupComparator() {
        return jobContext().getGroupingComparator();
    }

    /**
     * Gets serializer for specified class.
     * @param cls Class.
     * @param jobConf
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
    @Override public Comparator<?> sortComparator() {
        return jobCtx.getSortComparator();
    }

    /**
     * @param split Split.
     * @return Native Hadoop split.
     * @throws GridException if failed.
     */
    @SuppressWarnings("unchecked")
    public Object getNativeSplit(GridHadoopInputSplit split) throws GridException {
        if (split instanceof GridHadoopExternalSplit)
            return readExternalSplit((GridHadoopExternalSplit)split, jobConf());

        if (split instanceof GridHadoopSplitWrapper)
            return ((GridHadoopSplitWrapper)split).innerSplit();

        throw new IllegalStateException("Unknown split: " + split);
    }

    /**
     * @param split External split.
     * @param jobConf
     * @return Native input split.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object readExternalSplit(GridHadoopExternalSplit split, Configuration jobConf) throws GridException {
        Path jobDir = new Path(jobConf.get(MRJobConfig.MAPREDUCE_JOB_DIR));

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf);
             FSDataInputStream in = fs.open(JobSubmissionFiles.getJobSplitFile(jobDir))) {

            Class<?> cls = GridHadoopExternalSplit.readSplitClass(in, split.offset(), jobConf);

            assert cls != null;

            Serialization serialization = new SerializationFactory(jobConf).getSerialization(cls);

            Deserializer deserializer = serialization.getDeserializer(cls);

            deserializer.open(in);

            Object res = deserializer.deserialize(null);

            deserializer.close();

            assert res != null;

            return res;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
