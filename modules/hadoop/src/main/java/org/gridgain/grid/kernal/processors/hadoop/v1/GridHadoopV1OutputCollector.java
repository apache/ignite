/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Hadoop output collector.
 */
public class GridHadoopV1OutputCollector implements OutputCollector {
    /** Job configuration. */
    private final JobConf jobConf;

    /** Task context. */
    private final GridHadoopTaskContext taskCtx;

    /** Optional direct writer. */
    private final RecordWriter writer;

    /** Task attempt. */
    private final TaskAttemptID attempt;

    /**
     * @param jobConf Job configuration.
     * @param taskCtx Task context.
     * @param directWrite Direct write flag.
     * @param fileName File name.
     * @throws IOException In case of IO exception.
     */
    GridHadoopV1OutputCollector(JobConf jobConf, GridHadoopTaskContext taskCtx, boolean directWrite,
        @Nullable String fileName, TaskAttemptID attempt) throws IOException {
        this.jobConf = jobConf;
        this.taskCtx = taskCtx;
        this.attempt = attempt;

        if (directWrite) {
            jobConf.set("mapreduce.task.attempt.id", attempt.toString());

            OutputFormat outFormat = jobConf.getOutputFormat();

            writer = outFormat.getRecordWriter(null, jobConf, fileName, Reporter.NULL);
        }
        else
            writer = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void collect(Object key, Object val) throws IOException {
        if (writer != null)
            writer.write(key, val);
        else {
            try {
                taskCtx.output().write(key, val);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Close writer.
     *
     * @throws IOException In case of IO exception.
     */
    public void closeWriter() throws IOException {
        if (writer != null)
            writer.close(Reporter.NULL);
    }

    /**
     * Setup task.
     *
     * @throws IOException If failed.
     */
    public void setup() throws IOException {
        if (writer != null)
            jobConf.getOutputCommitter().setupTask(new TaskAttemptContextImpl(jobConf, attempt));
    }

    /**
     * Commit task.
     *
     * @throws IOException In failed.
     */
    public void commit() throws IOException {
        if (writer != null) {
            OutputCommitter outputCommitter = jobConf.getOutputCommitter();

            TaskAttemptContext taskCtx = new TaskAttemptContextImpl(jobConf, attempt);

            if (outputCommitter.needsTaskCommit(taskCtx))
                outputCommitter.commitTask(taskCtx);
        }
    }

    /**
     * Abort task.
     */
    public void abort() {
        try {
            if (writer != null)
                jobConf.getOutputCommitter().abortTask(new TaskAttemptContextImpl(jobConf, attempt));
        }
        catch (IOException ignore) {
            // No-op.
        }
    }
}
