/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopLongCounter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Hadoop context implementation for v2 API. It provides IO operations for hadoop tasks.
 */
public class HadoopV2Context extends JobContextImpl implements MapContext, ReduceContext {
    /** Input reader to overriding of HadoopTaskContext input. */
    private RecordReader reader;

    /** Output writer to overriding of HadoopTaskContext output. */
    private RecordWriter writer;

    /** Output is provided by executor environment. */
    private final HadoopTaskOutput output;

    /** Input is provided by executor environment. */
    private final HadoopTaskInput input;

    /** Unique identifier for a task attempt. */
    private final TaskAttemptID taskAttemptID;

    /** Indicates that this task is to be cancelled. */
    private volatile boolean cancelled;

    /** Input split. */
    private InputSplit inputSplit;

    /** */
    private final HadoopTaskContext ctx;

    /** */
    private String status;

    /**
     * @param ctx Context for IO operations.
     */
    public HadoopV2Context(HadoopV2TaskContext ctx) {
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
            HadoopInputSplit split = ctx.taskInfo().inputSplit();

            if (split == null)
                return null;

            if (split instanceof HadoopFileBlock) {
                HadoopFileBlock fileBlock = (HadoopFileBlock)split;

                inputSplit = new FileSplit(new Path(fileBlock.file()), fileBlock.start(), fileBlock.length(), null);
            }
            else
            {
                try {
                    inputSplit = (InputSplit) ((HadoopV2TaskContext)ctx).getNativeSplit(split);
                } catch (IgniteCheckedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        return inputSplit;
    }

    /** {@inheritDoc} */
    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        if (cancelled)
            throw new HadoopTaskCancelledException("Task cancelled.");

        return reader.nextKeyValue();
    }

    /** {@inheritDoc} */
    @Override public Object getCurrentKey() throws IOException, InterruptedException {
        if (reader != null)
            return reader.getCurrentKey();

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
            throw new HadoopTaskCancelledException("Task cancelled.");

        if (writer != null)
            writer.write(key, val);
        else {
            try {
                output.write(key, val);
            }
            catch (IgniteCheckedException e) {
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
        return new HadoopV2Counter(ctx.counter(grpName, cntrName, HadoopLongCounter.class));
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
            throw new HadoopTaskCancelledException("Task cancelled.");

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
