/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop utility methods.
 */
public class GridHadoopUtils {
    /** Staging constant. */
    private static final String STAGING_CONSTANT = ".staging";

    /** Old mapper class attribute. */
    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";

    /** Old reducer class attribute. */
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    /**
     * Wraps native split.
     *
     * @param id Split ID.
     * @param split Split.
     * @param hosts Hosts.
     * @throws IOException If failed.
     */
    public static GridHadoopSplitWrapper wrapSplit(int id, Object split, String[] hosts) throws IOException {
        ByteArrayOutputStream arr = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(arr);

        assert split instanceof Writable;

        ((Writable)split).write(out);

        out.flush();

        return new GridHadoopSplitWrapper(id, split.getClass().getName(), arr.toByteArray(), hosts);
    }

    /**
     * Unwraps native split.
     *
     * @param o Wrapper.
     * @return Split.
     */
    public static Object unwrapSplit(GridHadoopSplitWrapper o) {
        try {
            Writable w = (Writable)GridHadoopUtils.class.getClassLoader().loadClass(o.className()).newInstance();

            w.readFields(new ObjectInputStream(new ByteArrayInputStream(o.bytes())));

            return w;
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Convert GG job status to Hadoop job status.
     *
     * @param status GG job status.
     * @return Hadoop job status.
     */
    public static JobStatus status(GridHadoopJobStatus status, Configuration conf) {
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
                assert status.totalReducerCnt() > 0;

                setupProgress = 1;
                mapProgress = 1;
                reduceProgress = 1f - status.pendingReducerCnt() / (float)status.totalReducerCnt();

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
     * @throws org.gridgain.grid.GridException If attribute is set.
     */
    public static void ensureNotSet(Configuration cfg, String attr, String msg) throws GridException {
        if (cfg.get(attr) != null)
            throw new GridException(attr + " is incompatible with " + msg + " mode.");
    }

    /**
     * Creates JobInfo from hadoop configuration.
     *
     * @param cfg Hadoop configuration.
     * @return Job info.
     * @throws GridException If failed.
     */
    public static GridHadoopDefaultJobInfo createJobInfo(Configuration cfg) throws GridException {
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

        return new GridHadoopDefaultJobInfo(jobConf.getJobName(), jobConf.getUser(), hasCombiner, numReduces, props);
    }

    /**
     * Throws new {@link GridException} with original exception is serialized into string.
     * This is needed to transfer error outside the current class loader.
     *
     * @param e Original exception.
     * @return GridException New exception.
     */
    public static GridException transformException(Throwable e) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        e.printStackTrace(new PrintStream(os, true));

        return new GridException(os.toString());
    }

    /**
     * Returns work directory for job execution.
     *
     * @param locNodeId Local node ID.
     * @param jobId Job ID.
     * @return Working directory for job.
     * @throws GridException If Failed.
     */
    public static File jobLocalDir(UUID locNodeId, GridHadoopJobId jobId) throws GridException {
        return new File(new File(U.resolveWorkDirectory("hadoop", false), "node-" + locNodeId), "job_" + jobId);
    }

    /**
     * Returns subdirectory of job working directory for task execution.
     *
     * @param locNodeId Local node ID.
     * @param info Task info.
     * @return Working directory for task.
     * @throws GridException If Failed.
     */
    public static File taskLocalDir(UUID locNodeId, GridHadoopTaskInfo info) throws GridException {
        File jobLocDir = jobLocalDir(locNodeId, info.jobId());

        return new File(jobLocDir, info.type() + "_" + info.taskNumber() + "_" + info.attempt());
    }

    /**
     * Constructor.
     */
    private GridHadoopUtils() {
        // No-op.
    }
}
