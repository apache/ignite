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
    @Override public void run0(GridHadoopV2Job jobImpl, JobContext jobCtx, GridHadoopTaskContext taskCtx)
        throws GridException {
        Mapper mapper;
        InputFormat inFormat;

        try {
            mapper = U.newInstance(jobCtx.getMapperClass());
            inFormat = U.newInstance(jobCtx.getInputFormatClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), null);
        }
        else
            nativeSplit = (InputSplit)jobImpl.getNativeSplit(split);

        assert nativeSplit != null;

        OutputFormat outputFormat = null;
        Exception err = null;

        try {
            RecordReader reader = inFormat.createRecordReader(nativeSplit, hadoopContext());

            reader.initialize(nativeSplit, hadoopContext());

            hadoopContext().reader(reader);

            outputFormat = jobImpl.hasCombinerOrReducer() ? null : prepareWriter(jobCtx);

            try {
                mapper.run(new WrappedMapper().getMapContext(hadoopContext()));
            }
            finally {
                closeWriter();
            }

            commit(outputFormat);
        }
        catch (InterruptedException e) {
            err = e;

            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
        catch (Exception e) {
            err = e;

            throw new GridException(e);
        }
        finally {
            if (err != null)
                abort(outputFormat);
        }
    }
}
