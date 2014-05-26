/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Extended Hadoop task.
 */
public abstract class GridHadoopTaskEx extends GridHadoopTask {
    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     */
    public GridHadoopTaskEx(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /**
     * Put write into Hadoop context and return associated output format instance.
     *
     * @param hadoopCtx Hadoop context.
     * @param jobCtx Job context.
     * @return Output format.
     * @throws GridException In case of Grid exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected static OutputFormat putWriter(GridHadoopV2Context hadoopCtx, JobContext jobCtx)
        throws GridException, InterruptedException {
        try {
            OutputFormat outputFormat = U.newInstance(jobCtx.getOutputFormatClass());

            assert outputFormat != null;

            RecordWriter writer = outputFormat.getRecordWriter(hadoopCtx);

            hadoopCtx.writer(writer);

            return outputFormat;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /**
     * Commit
     *
     * @param hadoopCtx Hadoop context.
     * @param outputFormat Output format.
     * @throws GridException In case of Grid exception.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected static void commit(GridHadoopV2Context hadoopCtx, @Nullable OutputFormat outputFormat)
        throws GridException, IOException, InterruptedException {
        RecordWriter writer = hadoopCtx.writer();

        if (writer != null) {
            assert outputFormat != null;

            writer.close(hadoopCtx);

            OutputCommitter outputCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            outputCommitter.commitTask(hadoopCtx);
        }
        else
            assert outputFormat == null;
    }
}
