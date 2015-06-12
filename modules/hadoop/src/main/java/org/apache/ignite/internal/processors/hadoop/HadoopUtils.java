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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.*;
import org.apache.ignite.*;
import org.apache.ignite.hadoop.fs.v1.*;
import org.apache.ignite.internal.processors.hadoop.fs.*;
import org.apache.ignite.internal.processors.hadoop.v2.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Hadoop utility methods.
 */
public class HadoopUtils {
    /** Property to store timestamp of new job id request. */
    public static final String REQ_NEW_JOBID_TS_PROPERTY = "ignite.job.requestNewIdTs";

    /** Property to store timestamp of response of new job id request. */
    public static final String RESPONSE_NEW_JOBID_TS_PROPERTY = "ignite.job.responseNewIdTs";

    /** Property to store timestamp of job submission. */
    public static final String JOB_SUBMISSION_START_TS_PROPERTY = "ignite.job.submissionStartTs";

    /** Property to set custom writer of job statistics. */
    public static final String JOB_COUNTER_WRITER_PROPERTY = "ignite.counters.writer";

    /** Staging constant. */
    private static final String STAGING_CONSTANT = ".staging";

    /** Old mapper class attribute. */
    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";

    /** Old reducer class attribute. */
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    /** Lazy per-user cache for the file systems. It is cleared and nulled in #close() method. */
    private static final HadoopLazyConcurrentMap<FsCacheKey, FileSystem> fileSysLazyMap = new HadoopLazyConcurrentMap<>(
        new HadoopLazyConcurrentMap.ValueFactory<FsCacheKey, FileSystem>() {
            @Override public FileSystem createValue(FsCacheKey key) {
                try {
                    assert key != null;

                    // Explicitly disable FileSystem caching:
                    URI uri = key.uri();

                    String scheme = uri.getScheme();

                    // Copy the configuration to avoid altering the external object.
                    Configuration cfg = new Configuration(key.configuration());

                    String prop = HadoopUtils.disableFsCachePropertyName(scheme);

                    cfg.setBoolean(prop, true);

                    return FileSystem.get(uri, cfg, key.user());
                }
                catch (IOException | InterruptedException ioe) {
                    throw new IgniteException(ioe);
                }
            }
        }
    );

    /**
     * Constructor.
     */
    private HadoopUtils() {
        // No-op.
    }

    /**
     * Wraps native split.
     *
     * @param id Split ID.
     * @param split Split.
     * @param hosts Hosts.
     * @throws IOException If failed.
     */
    public static HadoopSplitWrapper wrapSplit(int id, Object split, String[] hosts) throws IOException {
        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(arr);

        assert split instanceof Writable;

        ((Writable)split).write(out);

        out.flush();

        return new HadoopSplitWrapper(id, split.getClass().getName(), arr.toByteArray(), hosts);
    }

    /**
     * Unwraps native split.
     *
     * @param o Wrapper.
     * @return Split.
     */
    public static Object unwrapSplit(HadoopSplitWrapper o) {
        try {
            Writable w = (Writable)HadoopUtils.class.getClassLoader().loadClass(o.className()).newInstance();

            w.readFields(new ObjectInputStream(new ByteArrayInputStream(o.bytes())));

            return w;
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Convert Ignite job status to Hadoop job status.
     *
     * @param status Ignite job status.
     * @return Hadoop job status.
     */
    public static JobStatus status(HadoopJobStatus status, Configuration conf) {
        JobID jobId = new JobID(status.jobId().globalId().toString(), status.jobId().localId());

        float setupProgress = 0;
        float mapProgress = 0;
        float reduceProgress = 0;
        float cleanupProgress = 0;

        JobStatus.State state = JobStatus.State.RUNNING;

        switch (status.jobPhase()) {
            case PHASE_SETUP:
                setupProgress = 0.42f;

                break;

            case PHASE_MAP:
                setupProgress = 1;
                mapProgress = 1f - status.pendingMapperCnt() / (float)status.totalMapperCnt();

                break;

            case PHASE_REDUCE:
                setupProgress = 1;
                mapProgress = 1;

                if (status.totalReducerCnt() > 0)
                    reduceProgress = 1f - status.pendingReducerCnt() / (float)status.totalReducerCnt();
                else
                    reduceProgress = 1f;

                break;

            case PHASE_CANCELLING:
            case PHASE_COMPLETE:
                if (!status.isFailed()) {
                    setupProgress = 1;
                    mapProgress = 1;
                    reduceProgress = 1;
                    cleanupProgress = 1;

                    state = JobStatus.State.SUCCEEDED;
                }
                else
                    state = JobStatus.State.FAILED;

                break;

            default:
                assert false;
        }

        return new JobStatus(jobId, setupProgress, mapProgress, reduceProgress, cleanupProgress, state,
            JobPriority.NORMAL, status.user(), status.jobName(), jobFile(conf, status.user(), jobId).toString(), "N/A");
    }

    /**
     * Gets staging area directory.
     *
     * @param conf Configuration.
     * @param usr User.
     * @return Staging area directory.
     */
    public static Path stagingAreaDir(Configuration conf, String usr) {
        return new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR, MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
            + Path.SEPARATOR + usr + Path.SEPARATOR + STAGING_CONSTANT);
    }

    /**
     * Gets job file.
     *
     * @param conf Configuration.
     * @param usr User.
     * @param jobId Job ID.
     * @return Job file.
     */
    public static Path jobFile(Configuration conf, String usr, JobID jobId) {
        return new Path(stagingAreaDir(conf, usr), jobId.toString() + Path.SEPARATOR + MRJobConfig.JOB_CONF_FILE);
    }

    /**
     * Checks the attribute in configuration is not set.
     *
     * @param attr Attribute name.
     * @param msg Message for creation of exception.
     * @throws IgniteCheckedException If attribute is set.
     */
    public static void ensureNotSet(Configuration cfg, String attr, String msg) throws IgniteCheckedException {
        if (cfg.get(attr) != null)
            throw new IgniteCheckedException(attr + " is incompatible with " + msg + " mode.");
    }

    /**
     * Creates JobInfo from hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     * @return Job info.
     * @throws IgniteCheckedException If failed.
     */
    public static HadoopDefaultJobInfo createJobInfo(Configuration cfg) throws IgniteCheckedException {
        JobConf jobConf = new JobConf(cfg);

        boolean hasCombiner = jobConf.get("mapred.combiner.class") != null
                || jobConf.get(MRJobConfig.COMBINE_CLASS_ATTR) != null;

        int numReduces = jobConf.getNumReduceTasks();

        jobConf.setBooleanIfUnset("mapred.mapper.new-api", jobConf.get(OLD_MAP_CLASS_ATTR) == null);

        if (jobConf.getUseNewMapper()) {
            String mode = "new map API";

            ensureNotSet(jobConf, "mapred.input.format.class", mode);
            ensureNotSet(jobConf, OLD_MAP_CLASS_ATTR, mode);

            if (numReduces != 0)
                ensureNotSet(jobConf, "mapred.partitioner.class", mode);
            else
                ensureNotSet(jobConf, "mapred.output.format.class", mode);
        }
        else {
            String mode = "map compatibility";

            ensureNotSet(jobConf, MRJobConfig.INPUT_FORMAT_CLASS_ATTR, mode);
            ensureNotSet(jobConf, MRJobConfig.MAP_CLASS_ATTR, mode);

            if (numReduces != 0)
                ensureNotSet(jobConf, MRJobConfig.PARTITIONER_CLASS_ATTR, mode);
            else
                ensureNotSet(jobConf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
        }

        if (numReduces != 0) {
            jobConf.setBooleanIfUnset("mapred.reducer.new-api", jobConf.get(OLD_REDUCE_CLASS_ATTR) == null);

            if (jobConf.getUseNewReducer()) {
                String mode = "new reduce API";

                ensureNotSet(jobConf, "mapred.output.format.class", mode);
                ensureNotSet(jobConf, OLD_REDUCE_CLASS_ATTR, mode);
            }
            else {
                String mode = "reduce compatibility";

                ensureNotSet(jobConf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
                ensureNotSet(jobConf, MRJobConfig.REDUCE_CLASS_ATTR, mode);
            }
        }

        Map<String, String> props = new HashMap<>();

        for (Map.Entry<String, String> entry : jobConf)
            props.put(entry.getKey(), entry.getValue());

        return new HadoopDefaultJobInfo(jobConf.getJobName(), jobConf.getUser(), hasCombiner, numReduces, props);
    }

    /**
     * Throws new {@link IgniteCheckedException} with original exception is serialized into string.
     * This is needed to transfer error outside the current class loader.
     *
     * @param e Original exception.
     * @return IgniteCheckedException New exception.
     */
    public static IgniteCheckedException transformException(Throwable e) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        e.printStackTrace(new PrintStream(os, true));

        return new IgniteCheckedException(os.toString());
    }

    /**
     * Returns work directory for job execution.
     *
     * @param locNodeId Local node ID.
     * @param jobId Job ID.
     * @return Working directory for job.
     * @throws IgniteCheckedException If Failed.
     */
    public static File jobLocalDir(UUID locNodeId, HadoopJobId jobId) throws IgniteCheckedException {
        return new File(new File(U.resolveWorkDirectory("hadoop", false), "node-" + locNodeId), "job_" + jobId);
    }

    /**
     * Returns subdirectory of job working directory for task execution.
     *
     * @param locNodeId Local node ID.
     * @param info Task info.
     * @return Working directory for task.
     * @throws IgniteCheckedException If Failed.
     */
    public static File taskLocalDir(UUID locNodeId, HadoopTaskInfo info) throws IgniteCheckedException {
        File jobLocDir = jobLocalDir(locNodeId, info.jobId());

        return new File(jobLocDir, info.type() + "_" + info.taskNumber() + "_" + info.attempt());
    }

    /**
     * Creates {@link Configuration} in a correct class loader context to avoid caching
     * of inappropriate class loader in the Configuration object.
     * @return New instance of {@link Configuration}.
     */
    public static Configuration safeCreateConfiguration() {
        final ClassLoader cl0 = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(Configuration.class.getClassLoader());

        try {
            return new Configuration();
        }
        finally {
            Thread.currentThread().setContextClassLoader(cl0);
        }
    }

    /**
     * Creates {@link JobConf} in a correct class loader context to avoid caching
     * of inappropriate class loader in the Configuration object.
     * @return New instance of {@link JobConf}.
     */
    public static JobConf safeCreateJobConf() {
        final ClassLoader cl0 = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(JobConf.class.getClassLoader());

        try {
            return new JobConf();
        }
        finally {
            Thread.currentThread().setContextClassLoader(cl0);
        }
    }

    /**
     * Gets non-null user name as per the Hadoop viewpoint.
     * @param cfg the Hadoop job configuration, may be null.
     * @return the user name, never null.
     */
    private static String getMrHadoopUser(Configuration cfg) throws IOException {
        String user = cfg.get(MRJobConfig.USER_NAME);

        if (user == null)
            user = IgniteHadoopFileSystem.getFsHadoopUser();

        return user;
    }

    /**
     * Common method to get the V1 file system in MapRed engine.
     * It creates the filesystem for the user specified in the
     * configuration with {@link MRJobConfig#USER_NAME} property.
     * @param uri the file system uri.
     * @param cfg the configuration.
     * @return the file system
     * @throws IOException
     */
    public static FileSystem fileSystemForMrUser(@Nullable URI uri, Configuration cfg, boolean doCacheFs) throws IOException {
        final String usr = getMrHadoopUser(cfg);

        assert usr != null;

        if (uri == null)
            uri = FileSystem.getDefaultUri(cfg);

        final FileSystem fs;

        if (doCacheFs) {
            try {
                fs = getWithCaching(uri, cfg, usr);
            }
            catch (IgniteException ie) {
                throw new IOException(ie);
            }
        }
        else {
            try {
                fs = FileSystem.get(uri, cfg, usr);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                throw new IOException(ie);
            }
        }

        assert fs != null;
        assert !(fs instanceof IgniteHadoopFileSystem) || F.eq(usr, ((IgniteHadoopFileSystem)fs).user());

        return fs;
    }

    /**
     * Note that configuration is not a part of the key.
     * It is used solely to initialize the first instance
     * that is created for the key.
     */
    public static final class FsCacheKey {
        /** */
        private final URI uri;

        /** */
        private final String usr;

        /** */
        private final String equalityKey;

        /** */
        private final Configuration cfg;

        /**
         * Constructor
         */
        public FsCacheKey(URI uri, String usr, Configuration cfg) {
            assert uri != null;
            assert usr != null;
            assert cfg != null;

            this.uri = fixUri(uri, cfg);
            this.usr = usr;
            this.cfg = cfg;

            this.equalityKey = createEqualityKey();
        }

        /**
         * Creates String key used for equality and hashing.
         */
        private String createEqualityKey() {
            GridStringBuilder sb = new GridStringBuilder("(").a(usr).a(")@");

            if (uri.getScheme() != null)
                sb.a(uri.getScheme().toLowerCase());

            sb.a("://");

            if (uri.getAuthority() != null)
                sb.a(uri.getAuthority().toLowerCase());

            return sb.toString();
        }

        /**
         * The URI.
         */
        public URI uri() {
            return uri;
        }

        /**
         * The User.
         */
        public String user() {
            return usr;
        }

        /**
         * The Configuration.
         */
        public Configuration configuration() {
            return cfg;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            return equalityKey.equals(((FsCacheKey)obj).equalityKey);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return equalityKey.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return equalityKey;
        }
    }

    /**
     * Gets FileSystem caching it in static Ignite cache. The cache is a singleton
     * for each class loader.
     *
     * <p/>Note that the file systems in the cache are keyed by a triplet {scheme, authority, user}.
     * The Configuration is not a part of the key. This means that for the given key file system is
     * initialized only once with the Configuration passed in upon the file system creation.
     *
     * @param uri The file system URI.
     * @param cfg The configuration.
     * @param usr The user to create file system for.
     * @return The file system: either created, or taken from the cache.
     */
    private static FileSystem getWithCaching(URI uri, Configuration cfg, String usr) {
        FsCacheKey key = new FsCacheKey(uri, usr, cfg);

        return fileSysLazyMap.getOrCreate(key);
    }

    /**
     * Gets the property name to disable file system cache.
     * @param scheme The file system URI scheme.
     * @return The property name. If scheme is null,
     * returns "fs.null.impl.disable.cache".
     */
    public static String disableFsCachePropertyName(@Nullable String scheme) {
        return String.format("fs.%s.impl.disable.cache", scheme);
    }

    /**
     * Takes Fs URI using logic similar to that used in FileSystem#get(1,2,3).
     * @param uri0 The uri.
     * @param cfg The cfg.
     * @return Correct URI.
     */
    public static URI fixUri(URI uri0, Configuration cfg) {
        if (uri0 == null)
            return FileSystem.getDefaultUri(cfg);

        String scheme = uri0.getScheme();
        String authority = uri0.getAuthority();

        if (authority == null) {
            URI dfltUri = FileSystem.getDefaultUri(cfg);

            if (scheme == null || (scheme.equals(dfltUri.getScheme()) && dfltUri.getAuthority() != null))
                return dfltUri;
        }

        return uri0;
    }
}
