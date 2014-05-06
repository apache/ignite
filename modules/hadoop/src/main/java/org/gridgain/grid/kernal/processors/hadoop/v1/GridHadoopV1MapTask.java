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
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Job;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop map task implementation for v1 API.
 */
public class GridHadoopV1MapTask extends GridHadoopTask {
    /** {@inheritDoc} */
    public GridHadoopV1MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(final GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Mapper mapper = U.newInstance(jobConf.getMapperClass());

        InputFormat inFormat = jobConf.getInputFormat();

        GridHadoopFileBlock block = info().fileBlock();

        InputSplit split = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), block.hosts());

        OutputCollector collector = new OutputCollector() {
            @Override public void collect(Object key, Object val) throws IOException {
                try {
                    taskCtx.output().write(key, val);
                }
                catch (GridException e) {
                    throw new IOException(e);
                }
            }
        };

        Reporter reporter = Reporter.NULL;

        try {
            RecordReader reader = inFormat.getRecordReader(split, jobConf, reporter);

            Object key = reader.createKey();
            Object val = reader.createValue();

            mapper.configure(jobConf);

            while (reader.next(key, val))
                mapper.map(key, val, collector, reporter);

            mapper.close();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
