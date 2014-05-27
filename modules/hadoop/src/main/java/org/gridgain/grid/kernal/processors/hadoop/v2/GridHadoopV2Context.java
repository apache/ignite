/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.*;
import org.apache.hadoop.security.*;
import org.gridgain.grid.*;
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
        this.cfg = new Configuration(cfg);
        this.taskAttemptID = taskAttemptID;

        this.cfg.set("mapreduce.job.id", taskAttemptID.getJobID().toString());
        this.cfg.set("mapreduce.task.id", taskAttemptID.getTaskID().toString());

        output = ctx.output();
        input = ctx.input();
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();

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
        return new GenericCounter(cntrName.name(), cntrName.name());
    }

    /** {@inheritDoc} */
    @Override public Counter getCounter(String grpName, String cntrName) {
        return new GenericCounter(cntrName, cntrName);
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
        return taskAttemptID.getJobID();
    }

    /** {@inheritDoc} */
    @Override public int getNumReduceTasks() {
        return cfg.getInt(org.apache.hadoop.mapred.JobContext.NUM_REDUCES, 1);
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() throws IOException {
        String name = cfg.get(JobContext.WORKING_DIR);

        if (name != null)
            return new Path(name);
        else {
            try {
                Path dir = FileSystem.get(cfg).getWorkingDirectory();

                cfg.set(JobContext.WORKING_DIR, dir.toString());

                return dir;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Class<?> getOutputKeyClass() {
        return cfg.getClass(JobContext.OUTPUT_KEY_CLASS, LongWritable.class, Object.class);
    }

    /** {@inheritDoc} */
    @Override public Class<?> getOutputValueClass() {
        return cfg.getClass(JobContext.OUTPUT_VALUE_CLASS, Text.class, Object.class);
    }

    /** {@inheritDoc} */
    @Override public Class<?> getMapOutputKeyClass() {
        Class<?> res = cfg.getClass(JobContext.MAP_OUTPUT_KEY_CLASS, null, Object.class);

        if (res == null)
            res = getOutputKeyClass();

        return res;
    }

    /** {@inheritDoc} */
    @Override public Class<?> getMapOutputValueClass() {
        Class<?> res = cfg.getClass(JobContext.MAP_OUTPUT_VALUE_CLASS, null, Object.class);

        if (res == null)
            res = getOutputValueClass();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String getJobName() {
        return cfg.get(JobContext.JOB_NAME, "");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        return (Class<? extends InputFormat<?,?>>)cfg.getClass(INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        return (Class<? extends Mapper<?,?,?,?>>)cfg.getClass(MAP_CLASS_ATTR, Mapper.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        return (Class<? extends Reducer<?,?,?,?>>)cfg.getClass(COMBINE_CLASS_ATTR, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        return (Class<? extends Reducer<?,?,?,?>>)cfg.getClass(REDUCE_CLASS_ATTR, Reducer.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        return (Class<? extends OutputFormat<?,?>>)cfg.getClass(OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        return (Class<? extends Partitioner<?,?>>)cfg.getClass(PARTITIONER_CLASS_ATTR, HashPartitioner.class);
    }

    /** {@inheritDoc} */
    @Override public RawComparator<?> getSortComparator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String getJar() {
        return cfg.get(JobContext.JAR);
    }

    /** {@inheritDoc} */
    @Override public RawComparator<?> getGroupingComparator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean getJobSetupCleanupNeeded() {
        return cfg.getBoolean(JobContext.SETUP_CLEANUP_NEEDED, false);
    }

    /** {@inheritDoc} */
    @Override public boolean getTaskCleanupNeeded() {
        return cfg.getBoolean(JobContext.TASK_CLEANUP_NEEDED, false);
    }

    /** {@inheritDoc} */
    @Override public boolean getProfileEnabled() {
        return cfg.getBoolean(JobContext.TASK_PROFILE, false);
    }

    /** {@inheritDoc} */
    @Override public String getProfileParams() {
        return cfg.get(JobContext.TASK_PROFILE_PARAMS,
            "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s");
    }

    /** {@inheritDoc} */
    @Override public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        return new Configuration.IntegerRanges(cfg.get(isMap ? JobContext.NUM_MAP_PROFILES :
            JobContext.NUM_REDUCE_PROFILES, "0-2"));
    }

    /** {@inheritDoc} */
    @Override public String getUser() {
        return cfg.get(JobContext.USER_NAME);
    }

    /** {@inheritDoc} */
    @Override public boolean getSymlink() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Path[] getArchiveClassPaths() {
        ArrayList<String> list = (ArrayList<String>)cfg.getStringCollection(MRJobConfig.CLASSPATH_ARCHIVES);

        if (list.size() == 0)
            return null;

        Path[] paths = new Path[list.size()];

        for (int i = 0; i < list.size(); i++)
            paths[i] = new Path(list.get(i));

        return paths;
    }

    /** {@inheritDoc} */
    @Override public URI[] getCacheArchives() throws IOException {
        return stringsToURIs(cfg.getStrings(MRJobConfig.CACHE_ARCHIVES));
    }

    /** {@inheritDoc} */
    @Override public URI[] getCacheFiles() throws IOException {
        return stringsToURIs(cfg.getStrings(MRJobConfig.CACHE_FILES));
    }

    /** {@inheritDoc} */
    @Override public Path[] getLocalCacheArchives() throws IOException {
        return stringsToPaths(cfg.getStrings(MRJobConfig.CACHE_LOCALARCHIVES));
    }

    /** {@inheritDoc} */
    @Override public Path[] getLocalCacheFiles() throws IOException {
        return stringsToPaths(cfg.getStrings(MRJobConfig.CACHE_LOCALFILES));
    }

    /** {@inheritDoc} */
    @Override public Path[] getFileClassPaths() {
        ArrayList<String> list = (ArrayList<String>)cfg.getStringCollection(MRJobConfig.CLASSPATH_FILES);

        if (list.size() == 0)
            return null;

        Path[] paths = new Path[list.size()];

        for (int i = 0; i < list.size(); i++)
            paths[i] = new Path(list.get(i));

        return paths;
    }

    /** {@inheritDoc} */
    @Override public String[] getArchiveTimestamps() {
        return cfg.getStrings(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS);
    }

    /** {@inheritDoc} */
    @Override public String[] getFileTimestamps() {
        return cfg.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS);
    }

    /** {@inheritDoc} */
    @Override public int getMaxMapAttempts() {
        return cfg.getInt(JobContext.MAP_MAX_ATTEMPTS, 4);
    }

    /** {@inheritDoc} */
    @Override public int getMaxReduceAttempts() {
        return cfg.getInt(JobContext.REDUCE_MAX_ATTEMPTS, 4);
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
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();

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
     * Convert strings to URIs.
     *
     * @param strs Strings.
     * @return URIs.
     */
    public static URI[] stringsToURIs(String[] strs){
        if (strs == null)
            return null;

        URI[] uris = new URI[strs.length];

        for (int i = 0; i < strs.length;i++){

            try{
                uris[i] = new URI(strs[i]);
            }
            catch(URISyntaxException e){
                throw new IllegalArgumentException("Failed to create URI for string:" + strs[i], e);
            }
        }
        return uris;
    }

    /**
     * Convert strings to paths.
     *
     * @param strs Strings.
     * @return Paths.
     */
    public static Path[] stringsToPaths(String[] strs){
        if (strs == null)
            return null;

        Path[] paths = new Path[strs.length];

        for (int i = 0; i < strs.length;i++)
            paths[i] = new Path(strs[i]);

        return paths;
    }
}
