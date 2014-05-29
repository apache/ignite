/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop map task implementation for v2 API.
 */
public class GridHadoopV2MapTask extends GridHadoopV2Task {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job)taskCtx.job();

        JobContext jobCtx = jobImpl.hadoopJobContext();

        Mapper mapper;
        InputFormat inFormat;

        try {
            mapper = U.newInstance(jobCtx.getMapperClass());
            inFormat = U.newInstance(jobCtx.getInputFormatClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        context(new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx, jobImpl.attemptId(info())));

        GridHadoopV2Context hadoopCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx,
            jobImpl.attemptId(info()));

        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), null);
        }
        else if (split instanceof GridHadoopExternalSplit)
            nativeSplit = jobImpl.readExternalSplit((GridHadoopExternalSplit)split);
        else
            nativeSplit = split.innerSplit();

        assert nativeSplit != null;

        try {
            RecordReader reader = inFormat.createRecordReader(nativeSplit, hadoopCtx);

            reader.initialize(nativeSplit, hadoopCtx);

            hadoopCtx.reader(reader);

            OutputFormat outputFormat = jobImpl.hasCombinerOrReducer() ? null : prepareWriter(jobCtx);

            try {
                mapper.run(new WrappedMapper().getMapContext(hadoopCtx));
            }
            finally {
                closeWriter();
            }

            commit(outputFormat);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }

}
