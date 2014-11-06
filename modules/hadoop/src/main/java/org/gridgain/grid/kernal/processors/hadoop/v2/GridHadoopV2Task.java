/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
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
    protected GridHadoopV2Task(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2TaskContext ctx = (GridHadoopV2TaskContext)taskCtx;

        hadoopCtx = new GridHadoopV2Context(ctx);

        run0(ctx);
    }

    /**
     * Internal task routine.
     *
     * @param taskCtx Task context.
     * @throws GridException
     */
    protected abstract void run0(GridHadoopV2TaskContext taskCtx) throws GridException;

    /**
     * @return hadoop context.
     */
    protected GridHadoopV2Context hadoopContext() {
        return hadoopCtx;
    }

    /**
     * Create and configure an OutputFormat instance.
     *
     * @param jobCtx Job context.
     * @return Instance of OutputFormat is specified in job configuration.
     * @throws ClassNotFoundException If specified class not found.
     */
    protected OutputFormat getOutputFormat(JobContext jobCtx) throws ClassNotFoundException {
        return ReflectionUtils.newInstance(jobCtx.getOutputFormatClass(), hadoopContext().getConfiguration());
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
            OutputFormat outputFormat = getOutputFormat(jobCtx);

            assert outputFormat != null;

            OutputCommitter outCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            if (outCommitter != null)
                outCommitter.setupTask(hadoopCtx);

            RecordWriter writer = outputFormat.getRecordWriter(hadoopCtx);

            hadoopCtx.writer(writer);

            return outputFormat;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /**
     * Closes writer.
     *
     * @throws Exception If fails and logger hasn't been specified.
     */
    protected void closeWriter() throws Exception {
        RecordWriter writer = hadoopCtx.writer();

        if (writer != null)
            writer.close(hadoopCtx);
    }

    /**
     * Setup task.
     *
     * @param outputFormat Output format.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void setup(@Nullable OutputFormat outputFormat) throws IOException, InterruptedException {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            outputFormat.getOutputCommitter(hadoopCtx).setupTask(hadoopCtx);
        }
    }

    /**
     * Commit task.
     *
     * @param outputFormat Output format.
     * @throws GridException In case of Grid exception.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void commit(@Nullable OutputFormat outputFormat) throws GridException, IOException, InterruptedException {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            OutputCommitter outputCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            if (outputCommitter.needsTaskCommit(hadoopCtx))
                outputCommitter.commitTask(hadoopCtx);
        }
    }

    /**
     * Abort task.
     *
     * @param outputFormat Output format.
     */
    protected void abort(@Nullable OutputFormat outputFormat) {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            try {
                outputFormat.getOutputCommitter(hadoopCtx).abortTask(hadoopCtx);
            }
            catch (IOException ignore) {
                // Ignore.
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        hadoopCtx.cancel();
    }
}
