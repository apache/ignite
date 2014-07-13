/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.io.*;

/**
 * Hadoop map task implementation for v1 API.
 */
public class GridHadoopV1MapTask extends GridHadoopV1Task {
    /** */
    private static final String[] EMPTY_HOSTS = new String[0];

    /** {@inheritDoc} */
    public GridHadoopV1MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void run(final GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.getTaskConf());

        InputFormat inFormat = jobConf.getInputFormat();

        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), EMPTY_HOSTS);
        }
        else {
            nativeSplit = (InputSplit) jobImpl.getNativeSplit(split);

            try {
                Class<?> splitCls = Class.forName(nativeSplit.getClass().getName(), true, jobConf.getClassLoader());

                if (!splitCls.isAssignableFrom(nativeSplit.getClass())) {
                    ByteArrayOutputStream buf = new ByteArrayOutputStream();

                    nativeSplit.write(new DataOutputStream(buf));

                    nativeSplit = (InputSplit) ReflectionUtils.newInstance(splitCls, jobConf);

                    nativeSplit.readFields(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));
                }
            }
            catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }

        assert nativeSplit != null;

        Reporter reporter = Reporter.NULL;

        GridHadoopV1OutputCollector collector = null;

        try {
            collector = collector(jobConf, taskCtx, !jobImpl.info().hasCombiner() && !jobImpl.info().hasReducer(),
                fileName(), jobImpl.attemptId(info()));

            RecordReader reader = inFormat.getRecordReader(nativeSplit, jobConf, reporter);

            Mapper mapper = ReflectionUtils.newInstance(jobConf.getMapperClass(), jobConf);

            Object key = reader.createKey();
            Object val = reader.createValue();

            assert mapper != null;

            boolean mapperClosed = false;

            try {
                while (reader.next(key, val)) {
                    if (isCancelled())
                        throw new GridHadoopTaskCancelledException("Map task cancelled.");

                    mapper.map(key, val, collector, reporter);
                }

                mapper.close();

                mapperClosed = true;
            }
            finally {
                if (!mapperClosed) {
                    try {
                        mapper.close();
                    }
                    catch (Throwable ignore){}
                }

                collector.closeWriter();
            }

            collector.commit();
        }
        catch (Exception e) {
            if (collector != null)
                collector.abort();

            throw new GridException(e);
        }
    }
}
