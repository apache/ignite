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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.GridTopic.TOPIC_JOB;
import static org.apache.ignite.internal.GridTopic.TOPIC_JOB_CANCEL;
import static org.apache.ignite.internal.GridTopic.TOPIC_TASK;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * This class provides implementation for job sibling.
 */
public class GridJobSiblingImpl implements ComputeJobSibling, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sesId;

    /** */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private IgniteUuid jobId;

    /** */
    private Object taskTopic;

    /** */
    private Object jobTopic;

    /** */
    private UUID nodeId;

    /** */
    private boolean isJobDone;

    /** */
    private transient GridKernalContext ctx;

    /** */
    public GridJobSiblingImpl() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     * @param jobId Job ID.
     * @param nodeId ID of the node where this sibling was sent for execution.
     * @param ctx Managers registry.
     */
    public GridJobSiblingImpl(IgniteUuid sesId, IgniteUuid jobId, UUID nodeId, GridKernalContext ctx) {
        assert sesId != null;
        assert jobId != null;
        assert nodeId != null;
        assert ctx != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.nodeId = nodeId;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getJobId() {
        return jobId;
    }

    /**
     * @return Node ID.
     */
    public synchronized UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node where this sibling is executing.
     */
    public synchronized void nodeId(UUID nodeId) {
        this.nodeId = nodeId;

        taskTopic = null;
        jobTopic = null;
    }

    /**
     * @return {@code True} if job has finished.
     */
    public synchronized boolean isJobDone() {
        return isJobDone;
    }

    /** */
    public synchronized void onJobDone() {
        isJobDone = true;
    }

    /**
     * @return Communication topic for receiving.
     */
    public synchronized Object taskTopic() {
        if (taskTopic == null)
            taskTopic = TOPIC_TASK.topic(jobId, nodeId);

        return taskTopic;
    }

    /**
     * @return Communication topic for sending.
     */
    public synchronized Object jobTopic() {
        if (jobTopic == null)
            jobTopic = TOPIC_JOB.topic(jobId, nodeId);

        return jobTopic;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        GridTaskSessionImpl ses = ctx.session().getSession(sesId);

        Collection<ClusterNode> nodes = ses == null ? ctx.discovery().remoteNodes() : ctx.discovery().nodes(ses.getTopology());

        for (ClusterNode node : nodes) {
            if (!ctx.localNodeId().equals(node.id())) {
                try {
                    ctx.io().sendToGridTopic(node, TOPIC_JOB_CANCEL, new GridJobCancelRequest(sesId, jobId), SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    IgniteLogger log = ctx.log(GridJobSiblingImpl.class);

                    if (log.isDebugEnabled())
                        log.debug("Failed to send cancel request, node left [nodeId=" + node.id() + ", ses=" + ses + ']');
                }
                catch (IgniteCheckedException e) {
                    // Avoid stack trace for left nodes.
                    if (ctx.discovery().node(node.id()) != null && ctx.discovery().pingNodeNoError(node.id()))
                        U.error(ctx.log(GridJobSiblingImpl.class), "Failed to send cancel request to node " +
                            "[nodeId=" + node.id() + ", ses=" + ses + ']', e);
                }
            }
        }

        // Cancel local jobs directly.
        ctx.job().cancelJob(sesId, jobId, false);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // Don't serialize node ID.
        U.writeGridUuid(out, sesId);
        U.writeGridUuid(out, jobId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Don't serialize node ID.
        sesId = U.readGridUuid(in);
        jobId = U.readGridUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingImpl.class, this);
    }
}