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
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;
import static org.gridgain.grid.hadoop.GridHadoopJobState.*;

/**
 * Hadoop utility methods.
 */
public class GridHadoopUtils {
    /** Staging constant. */
    private static final String STAGING_CONSTANT = ".staging";

    /** Step span. */
    private static final long STEP_SPAN = 1000L;

    /** Minimum doubling interval. */
    private static final long MIN_DOUBLE_INTERVAL = 5000L;

    /** Doubling interval delta. */
    private static final long DOUBLE_INTERVAL_DELTA = 45000L;

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
            meta.mapReducePlan().mappers(),
            meta.mapReducePlan().reducers(),
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
     * Calculate progress.
     *
     * @param totalTasks Total tasks.
     * @param completedTasks Completed tasks.
     * @param maxConcurrentTasks Maximum possible number of concurrent tasks.
     * @param startTime Start time.
     * @return Progress.
     */
    private static float progress(int totalTasks, int completedTasks, int maxConcurrentTasks, long startTime) {
        assert maxConcurrentTasks >= totalTasks;

        int concurrentTasks = Math.min(totalTasks - completedTasks, maxConcurrentTasks);

        long dur = U.currentTimeMillis() - startTime;

        float speculativeProgress = speculativeProgress(totalTasks, maxConcurrentTasks, dur) * concurrentTasks;

        return ((float)completedTasks + speculativeProgress) / totalTasks;
    }

    public static void main(String[] args) {
//        speculativeProgress(800, 8, 8, 100 * UPDATE_INTERVAL + 1500);
    }

    /**
     * Calculate speculative progress.
     *
     * @param totalTasks Total tasks.
     * @param maxConcurrentTasks Maximum possible number of concurrent tasks.
     * @param dur Duration.
     * @return Speculative progress.
     */
    private static float speculativeProgress(int totalTasks, int maxConcurrentTasks, long dur) {
        // Determine doubling interval based on maximum possible concurrent tasks and total tasks.
        float doubleRatio = (float)maxConcurrentTasks / totalTasks;

        U.debug("Double ratio: " + doubleRatio);

        long doubleInterval = ((long)(DOUBLE_INTERVAL_DELTA * doubleRatio) + MIN_DOUBLE_INTERVAL);

        doubleInterval = doubleInterval - doubleInterval % STEP_SPAN;

        U.debug("Double interval: " + doubleInterval);

        // Determine amount of full double interval and amount of remaining update intervals.
        long fullDoubles = dur / doubleInterval;

        U.debug("Big updates: " + fullDoubles);

        long partialUpdates = (dur - fullDoubles * doubleInterval) / STEP_SPAN;

        U.debug("Small updates: " + partialUpdates);

        // Now, as we have intervals count, calculate speculative progress for a single task.
        float power2 = (float)Math.pow(0.5, fullDoubles);

        float progress = /** Sum of geom. progression 1/2 + 1/4 ... */ 1 - power2 +
            /** Next member of geom. progression. */ power2 / 2 *
            /** Relative progress of the next doubling update. */ partialUpdates * STEP_SPAN / doubleInterval;

        U.debug("Single progress: " + progress);

        return progress;
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
