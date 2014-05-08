/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.v2.util.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;
import static org.gridgain.grid.hadoop.GridHadoopJobState.*;

/**
 * Hadoop utility methods.
 */
public class GridHadoopUtils {
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
            meta.pendingBlocks() != null ? meta.pendingBlocks().size() : 0,
            meta.pendingReducers() != null ? meta.pendingReducers().size() : 0,
            meta.totalBlockCount(),
            meta.totalReducerCount(),
            meta.phase()
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
                mapProgress = status.blockProgress();
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
                mapProgress = status.blockProgress();
                reduceProgress = status.reducerProgress();
                cleanupProgress = 0.0f;

                break;

            default:
                assert status.jobPhase() == PHASE_COMPLETE;

                // Do not know whether this is complete on success or failure, hence calculate map/reduce progress.
                mapProgress = status.blockProgress();
                reduceProgress = status.reducerProgress();
                cleanupProgress = 1.0f;
        }

        return new JobStatus(jobId, 1.0f, mapProgress, reduceProgress, cleanupProgress, state,
            JobPriority.NORMAL, status.user(), status.jobName(), MRApps.getJobFile(conf, status.user(), jobId), "N/A");
    }

    /**
     * Constructor.
     */
    private GridHadoopUtils() {
        // No-op.
    }
}
