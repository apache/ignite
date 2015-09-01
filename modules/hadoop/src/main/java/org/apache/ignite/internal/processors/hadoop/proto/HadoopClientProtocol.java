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

package org.apache.ignite.internal.processors.hadoop.proto;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;
import org.apache.ignite.internal.processors.hadoop.HadoopJobStatus;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReduceCounters;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.JOB_SUBMISSION_START_TS_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.REQ_NEW_JOBID_TS_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.RESPONSE_NEW_JOBID_TS_PROPERTY;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Hadoop client protocol.
 */
public class HadoopClientProtocol implements ClientProtocol {
    /** Protocol version. */
    private static final long PROTO_VER = 1L;

    /** Default Ignite system directory. */
    private static final String SYS_DIR = ".ignite/system";

    /** Configuration. */
    private final Configuration conf;

    /** Ignite client. */
    private volatile GridClient cli;

    /** Last received version. */
    private long lastVer = -1;

    /** Last received status. */
    private HadoopJobStatus lastStatus;

    /**
     * Constructor.
     *
     * @param conf Configuration.
     * @param cli Ignite client.
     */
    public HadoopClientProtocol(Configuration conf, GridClient cli) {
        assert cli != null;

        this.conf = conf;
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public JobID getNewJobID() throws IOException, InterruptedException {
        try {
            conf.setLong(REQ_NEW_JOBID_TS_PROPERTY, U.currentTimeMillis());

            HadoopJobId jobID = cli.compute().execute(HadoopProtocolNextTaskIdTask.class.getName(), null);

            conf.setLong(RESPONSE_NEW_JOBID_TS_PROPERTY, U.currentTimeMillis());

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
            conf.setLong(JOB_SUBMISSION_START_TS_PROPERTY, U.currentTimeMillis());

            HadoopJobStatus status = cli.compute().execute(HadoopProtocolSubmitJobTask.class.getName(),
                new HadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId(), createJobInfo(conf)));

            if (status == null)
                throw new IOException("Failed to submit job (null status obtained): " + jobId);

            return processStatus(status);
        }
        catch (GridClientException | IgniteCheckedException e) {
            throw new IOException("Failed to submit job.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics getClusterMetrics() throws IOException, InterruptedException {
        return new ClusterMetrics(0, 0, 0, 0, 0, 0, 1000, 1000, 1, 100, 0, 0);
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
    @Override public void killJob(JobID jobId) throws IOException, InterruptedException {
        try {
            cli.compute().execute(HadoopProtocolKillJobTask.class.getName(),
                new HadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId()));
        }
        catch (GridClientException e) {
            throw new IOException("Failed to kill job: " + jobId, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setJobPriority(JobID jobid, String priority) throws IOException, InterruptedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException,
        InterruptedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public JobStatus getJobStatus(JobID jobId) throws IOException, InterruptedException {
        try {
            Long delay = conf.getLong(HadoopJobProperty.JOB_STATUS_POLL_DELAY.propertyName(), -1);

            HadoopProtocolTaskArguments args = delay >= 0 ?
                new HadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId(), delay) :
                new HadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId());

            HadoopJobStatus status = cli.compute().execute(HadoopProtocolJobStatusTask.class.getName(), args);

            if (status == null)
                throw new IOException("Job tracker doesn't have any information about the job: " + jobId);

            return processStatus(status);
        }
        catch (GridClientException e) {
            throw new IOException("Failed to get job status: " + jobId, e);
        }
    }

    /** {@inheritDoc} */
    @Override public Counters getJobCounters(JobID jobId) throws IOException, InterruptedException {
        try {
            final HadoopCounters counters = cli.compute().execute(HadoopProtocolJobCountersTask.class.getName(),
                new HadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId()));

            if (counters == null)
                throw new IOException("Job tracker doesn't have any information about the job: " + jobId);

            return new HadoopMapReduceCounters(counters);
        }
        catch (GridClientException e) {
            throw new IOException("Failed to get job counters: " + jobId, e);
        }
    }

    /** {@inheritDoc} */
    @Override public TaskReport[] getTaskReports(JobID jobid, TaskType type) throws IOException, InterruptedException {
        return new TaskReport[0];
    }

    /** {@inheritDoc} */
    @Override public String getFilesystemName() throws IOException, InterruptedException {
        return FileSystem.get(conf).getUri().toString();
    }

    /** {@inheritDoc} */
    @Override public JobStatus[] getAllJobs() throws IOException, InterruptedException {
        return new JobStatus[0];
    }

    /** {@inheritDoc} */
    @Override public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId, int maxEvents)
        throws IOException, InterruptedException {
        return new TaskCompletionEvent[0];
    }

    /** {@inheritDoc} */
    @Override public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException, InterruptedException {
        return new String[0];
    }

    /** {@inheritDoc} */
    @Override public TaskTrackerInfo[] getActiveTrackers() throws IOException, InterruptedException {
        return new TaskTrackerInfo[0];
    }

    /** {@inheritDoc} */
    @Override public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException, InterruptedException {
        return new TaskTrackerInfo[0];
    }

    /** {@inheritDoc} */
    @Override public String getSystemDir() throws IOException, InterruptedException {
        Path sysDir = new Path(SYS_DIR);

        return sysDir.toString();
    }

    /** {@inheritDoc} */
    @Override public String getStagingAreaDir() throws IOException, InterruptedException {
        String usr = UserGroupInformation.getCurrentUser().getShortUserName();

        return HadoopUtils.stagingAreaDir(conf, usr).toString();
    }

    /** {@inheritDoc} */
    @Override public String getJobHistoryDir() throws IOException, InterruptedException {
        return JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getQueues() throws IOException, InterruptedException {
        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo getQueue(String queueName) throws IOException, InterruptedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException, InterruptedException {
        return new QueueAclsInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public QueueInfo[] getChildQueues(String queueName) throws IOException, InterruptedException {
        return new QueueInfo[0];
    }

    /** {@inheritDoc} */
    @Override public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException,
        InterruptedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException,
        InterruptedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException,
        InterruptedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID) throws IOException,
        InterruptedException {
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
     * Process received status update.
     *
     * @param status Ignite status.
     * @return Hadoop status.
     */
    private JobStatus processStatus(HadoopJobStatus status) {
        // IMPORTANT! This method will only work in single-threaded environment. It is valid at the moment because
        // IgniteHadoopClientProtocolProvider creates new instance of this class for every new job and Job class
        // serializes invocations of submitJob() and getJobStatus() methods. However, if any of these conditions will
        // change in future and either protocol will serve statuses for several jobs or status update will not be
        // serialized anymore, then we have to fallback to concurrent approach (e.g. using ConcurrentHashMap).
        // (vozerov)
        if (lastVer < status.version()) {
            lastVer = status.version();

            lastStatus = status;
        }
        else
            assert lastStatus != null;

        return HadoopUtils.status(lastStatus, conf);
    }
}