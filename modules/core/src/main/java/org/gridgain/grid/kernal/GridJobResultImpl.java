/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import java.util.*;

/**
 * Class provides implementation for job result.
 */
public class GridJobResultImpl implements GridComputeJobResult {
    /** */
    private final GridComputeJob job;

    /** */
    private final GridJobSiblingImpl sib;

    /** */
    private final GridJobContextImpl jobCtx;

    /** */
    private ClusterNode node;

    /** */
    private Object data;

    /** */
    private GridException ex;

    /** */
    private boolean hasRes;

    /** */
    private boolean isCancelled;

    /** */
    private boolean isOccupied;

    /**
     * @param job Job instance.
     * @param jobId ID of the job.
     * @param node Node from where this result was received.
     * @param sib Sibling associated with this result.
     */
    public GridJobResultImpl(GridComputeJob job, GridUuid jobId, ClusterNode node, GridJobSiblingImpl sib) {
        assert jobId != null;
        assert node != null;
        assert sib != null;

        this.job = job;
        this.node = node;
        this.sib = sib;

        jobCtx = new GridJobContextImpl(null, jobId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridComputeJob getJob() {
        return job;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobContext getJobContext() {
        return jobCtx;
    }

    /**
     * @return Sibling associated with this result.
     */
    public GridJobSiblingImpl getSibling() {
        return sib;
    }

    /** {@inheritDoc} */
    @Override public synchronized ClusterNode getNode() {
        return node;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public synchronized Object getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public synchronized GridException getException() {
        return ex;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean isCancelled() {
        return isCancelled;
    }

    /**
     * @param node Node from where this result was received.
     */
    public synchronized void setNode(ClusterNode node) {
        this.node = node;
    }

    /**
     * @param data Job data.
     * @param ex Job exception.
     * @param jobAttrs Job attributes.
     * @param isCancelled Whether job was cancelled or not.
     */
    public synchronized void onResponse(@Nullable Object data, @Nullable GridException ex,
        @Nullable Map<Object, Object> jobAttrs, boolean isCancelled) {
        this.data = data;
        this.ex = ex;
        this.isCancelled = isCancelled;

        if (jobAttrs != null)
            jobCtx.setAttributes(jobAttrs);

        hasRes = true;
    }

    /**
     * @param isOccupied {@code True} if job for this response is being sent.
     */
    public synchronized void setOccupied(boolean isOccupied) {
        this.isOccupied = isOccupied;
    }

    /**
     * @return {@code True} if job for this response is being sent.
     */
    public synchronized boolean isOccupied() {
        return isOccupied;
    }

    /**
     * Clears stored job data.
     */
    public synchronized void clearData() {
        data = null;
    }

    /** */
    public synchronized void resetResponse() {
        data = null;
        ex = null;

        hasRes = false;
    }

    /**
     * @return {@code true} if remote job responded.
     */
    public synchronized boolean hasResponse() {
        return hasRes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobResultImpl.class, this);
    }
}
