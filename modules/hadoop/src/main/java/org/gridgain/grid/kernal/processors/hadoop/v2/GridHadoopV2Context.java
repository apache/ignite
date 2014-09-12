/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.task.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop context implementation for v2 API. It provides IO operations for hadoop tasks.
 */
public class GridHadoopV2Context extends JobContextImpl implements MapContext, ReduceContext {
    /** Input reader to overriding of GridHadoopTaskContext input. */
    private RecordReader reader;

    /** Output writer to overriding of GridHadoopTaskContext output. */
    private RecordWriter writer;

    /** Output is provided by executor environment. */
    private final GridHadoopTaskOutput output;

    /** Input is provided by executor environment. */
    private final GridHadoopTaskInput input;

    /** Unique identifier for a task attempt. */
    private final TaskAttemptID taskAttemptID;

    /** Indicates that this task is to be cancelled. */
    private volatile boolean cancelled;

    /** Input split. */
    private InputSplit inputSplit;

    /** */
    private final GridHadoopTaskContext ctx;

    /** */
    private String status;

    /**
     * @param ctx Context for IO operations.
     */
    public GridHadoopV2Context(GridHadoopV2TaskContext ctx) {
        super(ctx.jobConf(), ctx.jobContext().getJobID());

        taskAttemptID = ctx.attemptId();

        conf.set("mapreduce.job.id", taskAttemptID.getJobID().toString());
        conf.set("mapreduce.task.id", taskAttemptID.getTaskID().toString());

        output = ctx.output();
        input = ctx.input();

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() {
        if (inputSplit == null) {
            GridHadoopInputSplit split = ctx.taskInfo().inputSplit();

            if (split == null)
                return null;

            if (split instanceof GridHadoopFileBlock) {
                GridHadoopFileBlock fileBlock = (GridHadoopFileBlock)split;

                inputSplit = new FileSplit(new Path(fileBlock.file()), fileBlock.start(), fileBlock.length(), null);
            }
            else if (split instanceof GridHadoopExternalSplit) {
                throw new UnsupportedOperationException(); // TODO
            }
            else if (split instanceof GridHadoopSplitWrapper) {
                inputSplit = (InputSplit)((GridHadoopSplitWrapper)split).innerSplit();
            }
            else
                throw new IllegalStateException();
        }

        return inputSplit;
    }

    /** {@inheritDoc} */
    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        if (cancelled)
            throw new GridHadoopTaskCancelledException("Task cancelled.");

        return reader.nextKeyValue();
    }

    /** {@inheritDoc} */
    @Override public Object getCurrentKey() throws IOException, InterruptedException {
        if (reader != null) {
            return reader.getCurrentKey();
        }

        return input.key();
    }

    /** {@inheritDoc} */
    @Override public Object getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void write(Object key, Object val) throws IOException, InterruptedException {
        if (cancelled)
            throw new GridHadoopTaskCancelledException("Task cancelled.");

        if (writer != null)
            writer.write(key, val);
        else {
            try {
                output.write(key, val);
            }
            catch (GridException e) {
                throw new IOException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public OutputCommitter getOutputCommitter() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public TaskAttemptID getTaskAttemptID() {
        return taskAttemptID;
    }

    /** {@inheritDoc} */
    @Override public void setStatus(String msg) {
        status = msg;
    }

    /** {@inheritDoc} */
    @Override public String getStatus() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        return 0.5f; // TODO
    }

    /** {@inheritDoc} */
    @Override public Counter getCounter(Enum<?> cntrName) {
        return getCounter(cntrName.getDeclaringClass().getName(), cntrName.name());
    }

    /** {@inheritDoc} */
    @Override public Counter getCounter(String grpName, String cntrName) {
        return new GridHadoopV2Counter(ctx.counter(grpName, cntrName));
    }

    /** {@inheritDoc} */
    @Override public void progress() {
        // No-op.
    }

    /**
     * Overrides default input data reader.
     *
     * @param reader New reader.
     */
    public void reader(RecordReader reader) {
        this.reader = reader;
    }

    /** {@inheritDoc} */
    @Override public boolean nextKey() throws IOException, InterruptedException {
        if (cancelled)
            throw new GridHadoopTaskCancelledException("Task cancelled.");

        return input.next();
    }

    /** {@inheritDoc} */
    @Override public Iterable getValues() throws IOException, InterruptedException {
        return new Iterable() {
            @Override public Iterator iterator() {
                return input.values();
            }
        };
    }

    /**
     * @return Overridden output data writer.
     */
    public RecordWriter writer() {
        return writer;
    }

    /**
     * Overrides default output data writer.
     *
     * @param writer New writer.
     */
    public void writer(RecordWriter writer) {
        this.writer = writer;
    }

    /**
     * Cancels the task by stop the IO.
     */
    public void cancel() {
        cancelled = true;
    }
}
