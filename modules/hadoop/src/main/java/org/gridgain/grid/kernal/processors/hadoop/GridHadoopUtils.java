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
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;
import static org.gridgain.grid.hadoop.GridHadoopJobState.*;

/**
 * Hadoop utility methods.
 */
public class GridHadoopUtils {
    /** Staging constant. */
    private static final String STAGING_CONSTANT = ".staging";

    /**
     * Convert Hadoop job metadata to job status.
     *
     * @param meta Metadata.
     * @return Status.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public static GridHadoopJobStatus status(GridHadoopJobMetadata meta) {
        GridHadoopDefaultJobInfo jobInfo = (GridHadoopDefaultJobInfo)meta.jobInfo();

        return new GridHadoopJobStatus(
            meta.jobId(),
            meta.phase() == PHASE_COMPLETE ? meta.failCause() == null ? STATE_SUCCEEDED : STATE_FAILED : STATE_RUNNING,
            jobInfo.configuration().getJobName(),
            jobInfo.configuration().getUser(),
            meta.pendingSplits() != null ? meta.pendingSplits().size() : 0,
            meta.pendingReducers() != null ? meta.pendingReducers().size() : 0,
            meta.totalSplitCount(),
            meta.totalReducerCount(),
            meta.phase(),
            meta.version()
        );
    }

    /**
     * Convert GG job status to Hadoop job status.
     *
     * @param status GG job status.
     * @return Hadoop job status.
     */
    public static JobStatus status(GridHadoopJobStatus status, Configuration conf) {
        JobID jobId = new JobID(status.jobId().globalId().toString(), status.jobId().localId());

        JobStatus.State state;

        switch (status.jobState()) {
            case STATE_RUNNING:
                state = JobStatus.State.RUNNING;

                break;

            case STATE_SUCCEEDED:
                state = JobStatus.State.SUCCEEDED;

                break;

            case STATE_FAILED:
                state = JobStatus.State.FAILED;

                break;

            default:
                assert status.jobState() == STATE_KILLED;

                state = JobStatus.State.KILLED;
        }

        float mapProgress;
        float reduceProgress;
        float cleanupProgress;

        switch (status.jobPhase()) {
            case PHASE_MAP:
                mapProgress = status.mapProgress();
                reduceProgress = 0.0f;
                cleanupProgress = 0.0f;

                break;

            case PHASE_REDUCE:
                mapProgress = 1.0f;
                reduceProgress = status.reducerProgress();
                cleanupProgress = 0.0f;

                break;

            case PHASE_CANCELLING:
                // Do not know where cancel occurred, hence calculate map/reduce progress.
                mapProgress = status.mapProgress();
                reduceProgress = status.reducerProgress();
                cleanupProgress = 0.0f;

                break;

            default:
                assert status.jobPhase() == PHASE_COMPLETE;

                // Do not know whether this is complete on success or failure, hence calculate map/reduce progress.
                mapProgress = status.mapProgress();
                reduceProgress = status.reducerProgress();
                cleanupProgress = 1.0f;
        }

        return new JobStatus(jobId, 1.0f, mapProgress, reduceProgress, cleanupProgress, state, JobPriority.NORMAL,
            status.user(), status.jobName(), jobFile(conf, status.user(), jobId).toString(), "N/A");
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
     * Constructor.
     */
    private GridHadoopUtils() {
        // No-op.
    }
}
