/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;

/**
 * Hadoop map task implementation for v2 API.
 */
public class GridHadoopV2MapTask extends GridHadoopV2TaskImpl {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV2JobImpl jobImpl = (GridHadoopV2JobImpl)taskCtx.job();

        JobContext jobCtx = jobImpl.ctx;

        Mapper mapper;
        InputFormat inFormat;

        try {
            mapper = U.newInstance(jobCtx.getMapperClass());
            inFormat = U.newInstance(jobCtx.getInputFormatClass());
        } catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        GridHadoopV2Context hadoopCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx, jobImpl.attemptId(info()));

        GridHadoopFileBlock block = info().fileBlock();

        FileSplit split = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), null);

        try {
            RecordReader reader = inFormat.createRecordReader(split, hadoopCtx);
            reader.initialize(split, hadoopCtx);

            hadoopCtx.reader(reader);

            mapper.run(new WrappedMapper().getMapContext(hadoopCtx));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            throw new GridInterruptedException(e);
        }
    }
}
