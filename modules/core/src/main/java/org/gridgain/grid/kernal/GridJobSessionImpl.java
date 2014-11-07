/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Job session implementation.
 */
public class GridJobSessionImpl implements GridTaskSessionInternal {
    /** Wrapped task session. */
    private final GridTaskSessionImpl ses;

    /** Job ID. */
    private final GridUuid jobId;

    /** Processor registry. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     * @param ses Task session.
     * @param jobId Job ID.
     */
    public GridJobSessionImpl(GridKernalContext ctx, GridTaskSessionImpl ses, GridUuid jobId) {
        assert ctx != null;
        assert ses != null;
        assert jobId != null;

        assert ses.getJobId() == null;

        this.ctx = ctx;
        this.ses = ses;
        this.jobId = jobId;
    }

    /** {@inheritDoc} */
    @Override public GridTaskSessionInternal session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public GridUuid getJobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void onClosed() {
        ses.onClosed();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return ses.isClosed();
    }

    /** {@inheritDoc} */
    @Override public boolean isTaskNode() {
        return ses.isTaskNode();
    }

    /** {@inheritDoc} */
    @Override public String getCheckpointSpi() {
        return ses.getCheckpointSpi();
    }

    /** {@inheritDoc} */
    @Override public String getTaskName() {
        return ses.getTaskName();
    }

    /**
     * Returns task class name.
     *
     * @return Task class name.
     */
    public String getTaskClassName() {
        return ses.getTaskClassName();
    }

    /** {@inheritDoc} */
    @Override public UUID getTaskNodeId() {
        return ses.getTaskNodeId();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return ses.getStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getEndTime() {
        return ses.getEndTime();
    }

    /** {@inheritDoc} */
    @Override public GridUuid getId() {
        return ses.getId();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return ses.getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridComputeJobSibling> refreshJobSiblings() throws GridException {
        if (!isTaskNode()) {
            Collection<GridComputeJobSibling> sibs = ctx.job().requestJobSiblings(this);

            // Request siblings list from task node (task is continuous).
            ses.setJobSiblings(sibs);

            return sibs;
        }

        if (!ses.isFullSupport()) {
            // Need to fetch task session from task worker.
            GridComputeTaskFuture<Object> fut = ctx.task().taskFuture(ses.getId());

            return fut.getTaskSession().getJobSiblings();
        }

        return ses.getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridComputeJobSibling> getJobSiblings() throws GridException {
        Collection<GridComputeJobSibling> sibs = ses.getJobSiblings();

        if (sibs == null) {
            if (isTaskNode()) {
                assert !ses.isFullSupport();

                // Need to fetch task session from task worker.
                GridComputeTaskFuture<Object> fut = ctx.task().taskFuture(ses.getId());

                return fut.getTaskSession().getJobSiblings();
            }

            // Request siblings list from task node (task is continuous).
            ses.setJobSiblings(sibs = ctx.job().requestJobSiblings(this));
        }

        return sibs;
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobSibling getJobSibling(GridUuid jobId) throws GridException {
        for (GridComputeJobSibling sib : getJobSiblings())
            if (sib.getJobId().equals(jobId))
                return sib;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, @Nullable Object val) throws GridException {
        setAttributes(Collections.singletonMap(key, val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> V getAttribute(K key) {
        return ses.getAttribute(key);
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) throws GridException {
        ses.setAttributes(attrs);

        if (!isTaskNode())
            ctx.job().setAttributes(this, attrs);
    }


    /** {@inheritDoc} */
    @Override public Map<?, ?> getAttributes() {
        return ses.getAttributes();
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(GridComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        ses.addAttributeListener(lsnr, rewind);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(GridComputeTaskSessionAttributeListener lsnr) {
        return ses.removeAttributeListener(lsnr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> V waitForAttribute(K key) throws InterruptedException {
        return ses.waitForAttribute(key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean waitForAttribute(K key, @Nullable V val) throws InterruptedException {
        return ses.waitForAttribute(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> V waitForAttribute(K key, long timeout) throws InterruptedException {
        return ses.waitForAttribute(key, timeout);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean waitForAttribute(K key, @Nullable V val, long timeout)
        throws InterruptedException {
        return ses.waitForAttribute(key, val, timeout);
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys) throws InterruptedException {
        return ses.waitForAttributes(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs) throws InterruptedException {
        return ses.waitForAttributes(attrs);
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) throws InterruptedException {
        return ses.waitForAttributes(keys, timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException {
        return ses.waitForAttributes(attrs, timeout);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) throws GridException {
        saveCheckpoint(key, state, GridComputeTaskSessionScope.SESSION_SCOPE, 0);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout)
        throws GridException {
        saveCheckpoint(key, state, scope, timeout, true);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope,
        long timeout, boolean overwrite) throws GridException {
        ses.saveCheckpoint0(this, key, state, scope, timeout, overwrite);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> T loadCheckpoint(String key) throws GridException {
        return ses.loadCheckpoint0(this, key);
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) throws GridException {
        return ses.removeCheckpoint0(this, key);
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getTopology() {
        return ses.getTopology();
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSupport() {
        return ses.isFullSupport();
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        return ses.subjectId();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSessionImpl.class, this);
    }
}
