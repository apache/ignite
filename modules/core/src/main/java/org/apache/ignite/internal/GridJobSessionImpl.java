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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Job session implementation.
 */
public class GridJobSessionImpl implements GridTaskSessionInternal {
    /** Wrapped task session. */
    private final GridTaskSessionImpl ses;

    /** Job ID. */
    private final IgniteUuid jobId;

    /** Processor registry. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     * @param ses Task session.
     * @param jobId Job ID.
     */
    public GridJobSessionImpl(GridKernalContext ctx, GridTaskSessionImpl ses, IgniteUuid jobId) {
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
    @Override public IgniteUuid getJobId() {
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
    @Override public IgniteUuid getId() {
        return ses.getId();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return ses.getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> refreshJobSiblings() {
        if (!isTaskNode()) {
            try {
                Collection<ComputeJobSibling> sibs = ctx.job().requestJobSiblings(this);

                // Request siblings list from task node (task is continuous).
                ses.setJobSiblings(sibs);

                return sibs;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        if (!ses.isFullSupport()) {
            // Need to fetch task session from task worker.
            ComputeTaskInternalFuture<Object> fut = ctx.task().taskFuture(ses.getId());

            return fut.getTaskSession().getJobSiblings();
        }

        return ses.getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> getJobSiblings() {
        Collection<ComputeJobSibling> sibs = ses.getJobSiblings();

        if (sibs == null) {
            if (isTaskNode()) {
                assert !ses.isFullSupport();

                // Need to fetch task session from task worker.
                ComputeTaskInternalFuture<Object> fut = ctx.task().taskFuture(ses.getId());

                return fut.getTaskSession().getJobSiblings();
            }

            try {
                // Request siblings list from task node (task is continuous).
                ses.setJobSiblings(sibs = ctx.job().requestJobSiblings(this));
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        return sibs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobSibling getJobSibling(IgniteUuid jobId) {
        for (ComputeJobSibling sib : getJobSiblings())
            if (sib.getJobId().equals(jobId))
                return sib;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, @Nullable Object val) {
        setAttributes(Collections.singletonMap(key, val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> V getAttribute(K key) {
        return ses.getAttribute(key);
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        ses.setAttributes(attrs);

        if (!isTaskNode()) {
            try {
                ctx.job().setAttributes(this, attrs);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> getAttributes() {
        return ses.getAttributes();
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(ComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        ses.addAttributeListener(lsnr, rewind);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(ComputeTaskSessionAttributeListener lsnr) {
        return ses.removeAttributeListener(lsnr);
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
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) throws InterruptedException {
        return ses.waitForAttributes(keys, timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException {
        return ses.waitForAttributes(attrs, timeout);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) {
        saveCheckpoint(key, state, ComputeTaskSessionScope.SESSION_SCOPE, 0);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope, long timeout) {
        saveCheckpoint(key, state, scope, timeout, true);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope,
        long timeout, boolean overwrite) {
        ses.saveCheckpoint0(this, key, state, scope, timeout, overwrite);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> T loadCheckpoint(String key) {
        return ses.loadCheckpoint0(this, key);
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
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
    @Override public IgniteFuture<?> mapFuture() {
        return new IgniteFinishedFutureImpl<>(null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSessionImpl.class, this);
    }
}