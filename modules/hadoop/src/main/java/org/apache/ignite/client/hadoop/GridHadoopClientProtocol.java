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

package org.apache.ignite.client.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.protocol.*;
import org.apache.hadoop.mapreduce.security.token.delegation.*;
import org.apache.hadoop.mapreduce.v2.*;
import org.apache.hadoop.mapreduce.v2.jobhistory.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.authorize.*;
import org.apache.hadoop.security.token.*;
import org.apache.ignite.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.hadoop.counter.*;
import org.apache.ignite.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.proto.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Hadoop client protocol.
 */
public class GridHadoopClientProtocol implements ClientProtocol {
    /** GridGain framework name property. */
    public static final String FRAMEWORK_NAME = "gridgain";

    /** Protocol version. */
    private static final long PROTO_VER = 1L;

    /** Default GridGain system directory. */
    private static final String SYS_DIR = ".gridgain/system";

    /** Configuration. */
    private final Configuration conf;

    /** GG client. */
    private volatile GridClient cli;

    /** Last received version. */
    private long lastVer = -1;

    /** Last received status. */
    private GridHadoopJobStatus lastStatus;

    /**
     * Constructor.
     *
     * @param conf Configuration.
     * @param cli GG client.
     */
    GridHadoopClientProtocol(Configuration conf, GridClient cli) {
        assert cli != null;

        this.conf = conf;
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public JobID getNewJobID() throws IOException, InterruptedException {
        try {
            conf.setLong(REQ_NEW_JOBID_TS_PROPERTY, U.currentTimeMillis());

            GridHadoopJobId jobID = cli.compute().execute(GridHadoopProtocolNextTaskIdTask.class.getName(), null);

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

            GridHadoopJobStatus status = cli.compute().execute(GridHadoopProtocolSubmitJobTask.class.getName(),
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId(), createJobInfo(conf)));

            assert status != null;

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
            cli.compute().execute(GridHadoopProtocolKillJobTask.class.getName(),
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId()));
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
            Long delay = conf.getLong(GridHadoopJobProperty.JOB_STATUS_POLL_DELAY.propertyName(), -1);

            GridHadoopProtocolTaskArguments args = delay >= 0 ?
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId(), delay) :
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId());

            GridHadoopJobStatus status = cli.compute().execute(GridHadoopProtocolJobStatusTask.class.getName(), args);

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
            final GridHadoopCounters counters = cli.compute().execute(GridHadoopProtocolJobCountersTask.class.getName(),
                new GridHadoopProtocolTaskArguments(jobId.getJtIdentifier(), jobId.getId()));

            if (counters == null)
                throw new IOException("Job tracker doesn't have any information about the job: " + jobId);

            return new GridHadoopClientCounters(counters);
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

        return GridHadoopUtils.stagingAreaDir(conf, usr).toString();
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
     * @param status GG status.
     * @return Hadoop status.
     */
    private JobStatus processStatus(GridHadoopJobStatus status) {
        // IMPORTANT! This method will only work in single-threaded environment. It is valid at the moment because
        // GridHadoopClientProtocolProvider creates new instance of this class for every new job and Job class
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

        return GridHadoopUtils.status(lastStatus, conf);
    }
}
