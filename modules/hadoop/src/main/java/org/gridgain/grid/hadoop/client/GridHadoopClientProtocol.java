// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop.client;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.protocol.*;
import org.apache.hadoop.mapreduce.security.token.delegation.*;
import org.apache.hadoop.mapreduce.v2.*;
import org.apache.hadoop.mapreduce.v2.jobhistory.*;
import org.apache.hadoop.mapreduce.v2.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.authorize.*;
import org.apache.hadoop.security.token.*;
import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.proto.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Hadoop client protocol.
 */
public class GridHadoopClientProtocol implements ClientProtocol {
    /** GridGain framework name. */
    public static final String PROP_FRAMEWORK_NAME = "gg.framework.name";

    /** GridGain server host. */
    public static final String PROP_SRV_HOST = "gg.server.host";

    /** GridGain server port. */
    public static final String PROP_SRV_PORT = "gg.server.port";

    /** Default server port. */
    public static final int DFLT_SRV_PORT = 6666;

    /** Protocol version. */
    public static final long PROTO_VER = 1L;

    /** Configuration. */
    private final Configuration conf;

    /** GG client. */
    private final GridClient cli;

    /** Current status worker. */
    private final AtomicReference<StatusWorker> statusWorkerRef = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param conf Configuration.
     * @param cli GG client.
     */
    GridHadoopClientProtocol(Configuration conf, GridClient cli) {
        this.conf = conf;
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public JobID getNewJobID() throws IOException, InterruptedException {
        try {
            GridHadoopJobId jobID = cli.compute().execute(GridHadoopProtocolNextTaskIdTask.class.getName(), null);

            return new JobID(jobID.globalId().toString(), jobID.localId());
        }
        catch (GridClientException e) {
            throw new IOException("Failed to get new job ID.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts) throws IOException,
        InterruptedException {
        try {
            GridHadoopJobStatus status = cli.compute().execute(GridHadoopProtocolSubmitJobTask.class.getName(),
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId(), conf));

            // TODO GG-8035: Job status.
            return new JobStatus();
        }
        catch (GridClientException e) {
            throw new IOException("Failed to submit job.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics getClusterMetrics() throws IOException, InterruptedException {
        // TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public Cluster.JobTrackerStatus getJobTrackerStatus() throws IOException, InterruptedException {
        return Cluster.JobTrackerStatus.RUNNING;
    }

    /** {@inheritDoc} */
    @Override public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public AccessControlList getQueueAdmins(String queueName) throws IOException {
        return new AccessControlList("*");
    }

    /** {@inheritDoc} */
    @Override public void killJob(JobID jobid) throws IOException, InterruptedException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void setJobPriority(JobID jobid, String priority) throws IOException, InterruptedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException,
        InterruptedException {
        // TODO

        return false;
    }

    /** {@inheritDoc} */
    @Override public JobStatus getJobStatus(JobID jobId) throws IOException, InterruptedException {
        try {
            StatusFuture fut;

            for (;;) {
                // First get worker.
                StatusWorker worker = statusWorkerRef.get();

                if (worker == null) {
                    StatusWorker newWorker = new StatusWorker(jobId);

                    if (statusWorkerRef.compareAndSet(null, newWorker)) {
                        fut = newWorker.fut;

                        new Thread(newWorker).start();

                        break;
                    }
                }
                else {
                    fut = worker.submit(jobId);

                    if (fut != null)
                        break;
                }
            }

            return fut.get();
        }
        catch (GridException e) {
            throw new IOException("Failed to get job status: " + jobId, e);
        }
    }

    /**
     * Internal job status routine.
     *
     * @param jobId Job ID.
     * @return Job status.
     * @throws GridClientException If failed.
     */
    private JobStatus jobStatus(JobID jobId) throws GridClientException {
        GridHadoopJobStatus jobStatus = cli.compute().execute(GridHadoopProtocolJobStatusTask.class.getName(),
            new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId()));

        // TODO: Convert.
        return null;
    }

    /** {@inheritDoc} */
    @Override public Counters getJobCounters(JobID jobid) throws IOException, InterruptedException {
        // TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public TaskReport[] getTaskReports(JobID jobid, TaskType type) throws IOException, InterruptedException {
        // TODO

        return new TaskReport[0];
    }

    /** {@inheritDoc} */
    @Override public String getFilesystemName() throws IOException, InterruptedException {
        return FileSystem.get(conf).getUri().toString();
    }

    /** {@inheritDoc} */
    @Override public JobStatus[] getAllJobs() throws IOException, InterruptedException {
        // TODO

        return new JobStatus[0];
    }

    /** {@inheritDoc} */
    @Override public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId, int maxEvents)
        throws IOException, InterruptedException {
        // TODO

        return new TaskCompletionEvent[0];
    }

    /** {@inheritDoc} */
    @Override public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException, InterruptedException {
        // TODO

        return new String[0];
    }

    /** {@inheritDoc} */
    @Override public TaskTrackerInfo[] getActiveTrackers() throws IOException, InterruptedException {
        // TODO

        return new TaskTrackerInfo[0];
    }

    /** {@inheritDoc} */
    @Override public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException, InterruptedException {
        return new TaskTrackerInfo[0];
    }

    /** {@inheritDoc} */
    @Override public String getSystemDir() throws IOException, InterruptedException {
        Path sysDir = new Path(MRJobConfig.JOB_SUBMIT_DIR);

        return sysDir.toString();
    }

    /** {@inheritDoc} */
    @Override public String getStagingAreaDir() throws IOException, InterruptedException {
        String usr = UserGroupInformation.getCurrentUser().getShortUserName();

        Path path = MRApps.getStagingAreaDir(conf, usr);

        return path.toString();
    }

    /** {@inheritDoc} */
    @Override public String getJobHistoryDir() throws IOException, InterruptedException {
        return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getQueues() throws IOException, InterruptedException {
        // TODO

        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo getQueue(String queueName) throws IOException, InterruptedException {
        // TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException, InterruptedException {
        // TODO

        return new QueueAclsInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
        // TODO

        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getChildQueues(String queueName) throws IOException, InterruptedException {
        // TODO

        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException,
        InterruptedException {
        // TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException,
        InterruptedException {
        // TODO

        return 0;
    }

    /** {@inheritDoc} */
    @Override public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException,
        InterruptedException {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID) throws IOException,
        InterruptedException {
        //TODO

        return null;
    }

    /** {@inheritDoc} */
    @Override public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return PROTO_VER;
    }

    /** {@inheritDoc} */
    @Override public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
        throws IOException {
        return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
    }

    /**
     * Closes protocol.
     */
    void close() {
        cli.close();
    }

    /**
     * Status future.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private static class StatusFuture extends GridFutureAdapter<JobStatus> {
        private final JobID jobId;

        private StatusFuture(JobID jobId) {
            this.jobId = jobId;
        }
    }

    /**
     * Status worker.
     */
    private class StatusWorker implements Runnable {
        /** Status future. */
        private volatile StatusFuture fut;

        /** Stopping flag. */
        private volatile boolean stopping;

        /**
         * Constructor.
         *
         * @param jobId Job ID.
         */
        private StatusWorker(JobID jobId) {
            this.fut = new StatusFuture(jobId);
        }

        /**
         * Submit new status task to worker.
         *
         * @param jobId Job ID.
         * @return Future or {@code null} is worker is stopping.
         */
        @Nullable private StatusFuture submit(JobID jobId) {
            if (stopping)
                // Stopping, cannot enqueue.
                return null;
            else {
                StatusFuture fut0 = fut;

                if (fut0 != null)
                    assert F.eq(jobId, fut0.jobId); // Protocol-per-job approach.
                else {
                    // Fallback to synchronizer.
                    synchronized (this) {
                        if (stopping)
                            return null;
                        else {
                            fut0 = fut;

                            if (fut0 != null)
                                assert F.eq(jobId, fut0.jobId); // Protocol-per-job approach.
                            else {
                                fut0 = new StatusFuture(jobId);

                                fut = fut0;

                                notifyAll();
                            }
                        }
                    }
                }

                return fut0;
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (true) {
                StatusFuture fut0 = fut;

                if (fut0 == null) {
                    synchronized (this) {
                        fut0 = fut;

                        if (fut0 == null) {
                            try {
                                wait(1000); // TODO GG-8035: Move to configuration.
                            }
                            catch (InterruptedException ignore) {
                                // Should never happen in current implementation.
                            }

                            fut0 = fut;
                        }

                        if (fut0 == null) {
                            stopping = true;

                            return;
                        }
                    }
                }

                try {
                    fut0.onDone(jobStatus(fut0.jobId));
                }
                catch (Throwable e) {
                    fut0.onDone(e);
                }
                finally {
                    synchronized (this) {
                        fut = null;
                    }
                }
            }
        }
    }
}
