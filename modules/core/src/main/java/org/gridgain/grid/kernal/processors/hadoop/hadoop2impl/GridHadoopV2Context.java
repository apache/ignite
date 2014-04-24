/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.*;

import org.gridgain.grid.hadoop.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Hadoop context implementation for v2 API. It provides IO operations for hadoop tasks.
 */
public class GridHadoopV2Context implements MapContext, ReduceContext {
    /** Input reader to overriding of GridHadoopTaskContext input. */
    private RecordReader reader;

    /** Output writer to overriding of GridHadoopTaskContext output. */
    private RecordWriter writer;

    /** Hadoop configuration of the job. */
    private final Configuration cfg;

    /** Output is provided by executor environment. */
    private final GridHadoopTaskOutput output;

    /** Input is provided by executor environment. */
    private final GridHadoopTaskInput input;

    /** Unique identifier for a task attempt. */
    private final TaskAttemptID taskAttemptID;

    /**
     * @param cfg Hadoop configuration of the job.
     * @param ctx Context for IO operations.
     * @param taskAttemptID Task execution id.
     */
    public GridHadoopV2Context(Configuration cfg, GridHadoopTaskContext ctx, TaskAttemptID taskAttemptID) {
        this.cfg = cfg;
        this.taskAttemptID = taskAttemptID;

        output = ctx.output();
        input = ctx.input();
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
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
    @Override public void write(Object key, Object val) throws IOException, InterruptedException {
        if (writer != null)
            writer.write(key, val);
        else
            output.write(key, val);
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
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getStatus() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Counter getCounter(Enum<?> cntrName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Counter getCounter(String grpName, String cntrName) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Configuration getConfiguration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public Credentials getCredentials() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public JobID getJobID() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getNumReduceTasks() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getOutputKeyClass() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getOutputValueClass() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getMapOutputKeyClass() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getMapOutputValueClass() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getJobName() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RawComparator<?> getSortComparator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getJar() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RawComparator<?> getGroupingComparator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean getJobSetupCleanupNeeded() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean getTaskCleanupNeeded() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean getProfileEnabled() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getProfileParams() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getUser() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean getSymlink() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Path[] getArchiveClassPaths() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public URI[] getCacheArchives() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public URI[] getCacheFiles() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Path[] getLocalCacheArchives() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Path[] getLocalCacheFiles() throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Path[] getFileClassPaths() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String[] getArchiveTimestamps() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String[] getFileTimestamps() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getMaxMapAttempts() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getMaxReduceAttempts() {
        throw new UnsupportedOperationException();
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
     * Overrides default output data writer.
     *
     * @param writer New writer.
     */
    public void writer(RecordWriter writer) {
        this.writer = writer;
    }
}
