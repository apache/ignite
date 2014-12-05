/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision;

import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

/**
 * Test implementation for job collision context.
 */
public class GridTestCollisionJobContext implements CollisionJobContext {
    /** Task session. */
    private ComputeTaskSession ses;

    /** {@code true} if job was activated. */
    private boolean activated;

    /** {@code true} if job was  canceled. */
    private boolean canceled;

    /** Index in wrapping collection. */
    private int idx = -1;

    /** Job context. */
    private ComputeJobContext ctx;

    /** Kob. */
    private GridTestJob job;

    /** Job activation callback. */
    private IgniteInClosure<GridTestCollisionJobContext> onActivate;

    /**
     * @param ses Session.
     */
    public GridTestCollisionJobContext(ComputeTaskSession ses) {
        this.ses = ses;

        ctx = new GridTestJobContext();

        job = new GridTestJob();
    }

    /**
     * @param ses Session.
     * @param idx Index.
     */
    public GridTestCollisionJobContext(ComputeTaskSession ses, int idx) {
        this.idx = idx;
        this.ses = ses;

        ctx = new GridTestJobContext();

        job = new GridTestJob();
    }

    /**
     * @param ses Session.
     * @param idx Index in wrapping collection.
     * @param onActivate Closure to be called when current task get activated.
     */
    public GridTestCollisionJobContext(ComputeTaskSession ses, int idx,
        IgniteInClosure<GridTestCollisionJobContext> onActivate) {
        this(ses, idx);

        this.onActivate = onActivate;
    }

    /**
     * @param ses Task session.
     * @param jobId Job ID.
     */
    public GridTestCollisionJobContext(ComputeTaskSession ses, IgniteUuid jobId) {
        this.ses = ses;

        ctx = new GridTestJobContext(jobId);

        job = new GridTestJob();
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskSession getTaskSession() {
        return ses;
    }

    /**
     * @param ses Session.
     */
    public void setTaskSession(ComputeTaskSession ses) {
        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobContext getJobContext() {
        return ctx;
    }

    /**
     * @param ctx Job context.
     */
    public void setJobContext(ComputeJobContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Index.
     */
    public int getIndex() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public boolean activate() {
        activated = true;

        if (onActivate != null)
            onActivate.apply(this);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        canceled = true;

        return true;
    }

    /** {@inheritDoc} */
    @Override public ComputeJob getJob() {
        return job;
    }

    /**
     * @return {@code True} if job was activated.
     */
    public boolean isActivated() {
        return activated;
    }

    /**
     * @return {@code True} if job was activated.
     */
    public boolean isCanceled() {
        return canceled;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getName());
        buf.append(" [ses=").append(ses);
        buf.append(", idx=").append(idx);
        buf.append(", activated=").append(activated);
        buf.append(", canceled=").append(canceled);
        buf.append(']');

        return buf.toString();
    }
}
