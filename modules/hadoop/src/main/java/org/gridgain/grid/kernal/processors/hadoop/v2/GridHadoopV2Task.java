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
 * Extended Hadoop v2 task.
 */
public abstract class GridHadoopV2Task extends GridHadoopTask {
    /** Hadoop context. */
    private GridHadoopV2Context hadoopCtx;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     */
    public GridHadoopV2Task(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /**
     * Set hadoop context.
     * @param ctx Hadoop context.
     */
    protected void context(GridHadoopV2Context ctx) {
        hadoopCtx = ctx;
    }

    /**
     * @return hadoop context.
     */
    protected ReduceContext hadoopCtx() {
        return hadoopCtx;
    }

    /**
     * Put write into Hadoop context and return associated output format instance.
     *
     * @param jobCtx Job context.
     * @return Output format.
     * @throws GridException In case of Grid exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected OutputFormat prepareWriter(JobContext jobCtx)
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
     * Close writer.
     *
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void closeWriter() throws IOException, InterruptedException {
        RecordWriter writer = hadoopCtx.writer();

        if (writer != null)
            writer.close(hadoopCtx);
    }

    /**
     * Commit data.
     *
     * @param outputFormat Output format.
     * @throws GridException In case of Grid exception.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void commit(@Nullable OutputFormat outputFormat)
        throws GridException, IOException, InterruptedException {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            OutputCommitter outputCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            outputCommitter.commitTask(hadoopCtx);
        }
        else
            assert outputFormat == null;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        if (hadoopCtx != null)
            hadoopCtx.cancel();
    }
}
