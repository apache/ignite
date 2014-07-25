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
    @SuppressWarnings("unchecked")
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        GridHadoopV2TaskContext ctx = (GridHadoopV2TaskContext)taskCtx;

        JobConf jobConf = ctx.jobConf();

        InputFormat inFormat = jobConf.getInputFormat();

        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), EMPTY_HOSTS);
        }
        else
            nativeSplit = (InputSplit)ctx.getNativeSplit(split);

        assert nativeSplit != null;

        Reporter reporter = new GridHadoopV1Reporter(taskCtx);

        GridHadoopV1OutputCollector collector = null;

        try {
            collector = collector(jobConf, ctx, !jobImpl.info().hasCombiner() && !jobImpl.info().hasReducer(),
                fileName(), jobImpl.attemptId(info()));

            RecordReader reader = inFormat.getRecordReader(nativeSplit, jobConf, reporter);

            Mapper mapper = ReflectionUtils.newInstance(jobConf.getMapperClass(), jobConf);

            Object key = reader.createKey();
            Object val = reader.createValue();

            assert mapper != null;

            try {
                try {
                    while (reader.next(key, val)) {
                        if (isCancelled())
                            throw new GridHadoopTaskCancelledException("Map task cancelled.");

                        mapper.map(key, val, collector, reporter);
                    }
                }
                finally {
                    mapper.close();
                }
            }
            finally {
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
