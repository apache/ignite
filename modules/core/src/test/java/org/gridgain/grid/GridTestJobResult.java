/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;

import java.io.*;

/**
 * Test job result.
 */
public class GridTestJobResult implements GridComputeJobResult {
    /** */
    private final Serializable data;

    /** */
    private final GridException e;

    /** */
    private final GridComputeJob job;

    /** */
    private final ClusterNode node;

    /** */
    private final GridComputeJobContext jobCtx;

    /** */
    public GridTestJobResult() {
        e = null;
        job = null;
        data = null;
        node = null;
        jobCtx = new GridTestJobContext();
    }

    /**
     * @param data Data.
     * @param e Exception.
     * @param job Gird job.
     * @param node Grid node.
     * @param jobCtx Job context.
     */
    public GridTestJobResult(Serializable data, GridException e, GridComputeJob job, ClusterNode node, GridComputeJobContext jobCtx) {
        this.data = data;
        this.e = e;
        this.job = job;
        this.node = node;
        this.jobCtx = jobCtx;
    }

    /**
     * @param node Grid node.
     */
    public GridTestJobResult(ClusterNode node) {
        this.node = node;

        e = null;
        job = null;
        data = null;
        jobCtx = new GridTestJobContext();
    }

    /** {@inheritDoc} */ @Override public Serializable getData() { return data; }

    /** {@inheritDoc} */ @Override public GridException getException() { return e; }

    /** {@inheritDoc} */ @Override public boolean isCancelled() { return false; }

    /** {@inheritDoc} */ @Override public GridComputeJob getJob() { return job; }

    /** {@inheritDoc} */ @Override public ClusterNode getNode() { return node; }

    /** {@inheritDoc} */ @Override public GridComputeJobContext getJobContext() { return jobCtx; }
}
