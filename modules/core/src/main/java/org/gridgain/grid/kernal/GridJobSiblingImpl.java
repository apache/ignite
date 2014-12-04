/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 * This class provides implementation for job sibling.
 */
public class GridJobSiblingImpl extends GridMetadataAwareAdapter implements GridComputeJobSibling, Externalizable {
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
    @Override public void cancel() throws GridException {
        GridTaskSessionImpl ses = ctx.session().getSession(sesId);

        if (ses == null) {
            Collection<ClusterNode> nodes = ctx.discovery().remoteNodes();

            if (!nodes.isEmpty())
                ctx.io().send(nodes, TOPIC_JOB_CANCEL, new GridJobCancelRequest(sesId, jobId), SYSTEM_POOL);

            // Cancel local jobs directly.
            ctx.job().cancelJob(sesId, jobId, false);

            return;
        }

        for (ClusterNode node : ctx.discovery().nodes(ses.getTopology())) {
            if (ctx.localNodeId().equals(node.id()))
                // Cancel local jobs directly.
                ctx.job().cancelJob(ses.getId(), jobId, false);
            else {
                try {
                    ctx.io().send(node, TOPIC_JOB_CANCEL, new GridJobCancelRequest(ses.getId(), jobId), SYSTEM_POOL);
                }
                catch (GridException e) {
                    // Avoid stack trace for left nodes.
                    if (ctx.discovery().node(node.id()) != null && ctx.discovery().pingNode(node.id()))
                        U.error(ctx.log(GridJobSiblingImpl.class), "Failed to send cancel request to node " +
                            "[nodeId=" + node.id() + ", ses=" + ses + ']', e);
                }
            }
        }
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
