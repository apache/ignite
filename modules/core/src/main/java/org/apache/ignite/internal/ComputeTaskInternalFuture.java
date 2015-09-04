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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * This class provide implementation for task future.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public class ComputeTaskInternalFuture<R> extends GridFutureAdapter<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ComputeTaskSession ses;

    /** */
    private GridKernalContext ctx;

    /** */
    @GridToStringExclude
    private ComputeFuture<R> userFut;

    /**
     * @param ses Task session instance.
     * @param ctx Kernal context.
     */
    public ComputeTaskInternalFuture(ComputeTaskSession ses, GridKernalContext ctx) {
        assert ses != null;
        assert ctx != null;

        this.ses = ses;
        this.ctx = ctx;

        userFut = new ComputeFuture<>(this);
    }

    /**
     * @param ctx Context.
     * @param taskCls Task class.
     * @param e Error.
     * @return Finished task future.
     */
    public static <R> ComputeTaskInternalFuture<R> finishedFuture(final GridKernalContext ctx,
        final Class<?> taskCls,
        IgniteCheckedException e) {
        assert ctx != null;
        assert taskCls != null;
        assert e != null;

        final long time = U.currentTimeMillis();

        final IgniteUuid id = IgniteUuid.fromUuid(ctx.localNodeId());

        ComputeTaskSession ses = new ComputeTaskSession() {
            @Override public String getTaskName() {
                return taskCls.getName();
            }

            @Override public UUID getTaskNodeId() {
                return ctx.localNodeId();
            }

            @Override public long getStartTime() {
                return time;
            }

            @Override public long getEndTime() {
                return time;
            }

            @Override public IgniteUuid getId() {
                return id;
            }

            @Override public ClassLoader getClassLoader() {
                return null;
            }

            @Override public Collection<ComputeJobSibling> getJobSiblings() throws IgniteException {
                return Collections.emptyList();
            }

            @Override public Collection<ComputeJobSibling> refreshJobSiblings() throws IgniteException {
                return Collections.emptyList();
            }

            @Nullable @Override public ComputeJobSibling getJobSibling(IgniteUuid jobId) throws IgniteException {
                return null;
            }

            @Override public void setAttribute(Object key, @Nullable Object val) throws IgniteException {
            }

            @Nullable @Override public <K, V> V getAttribute(K key) {
                return null;
            }

            @Override public void setAttributes(Map<?, ?> attrs) throws IgniteException {
                // No-op.
            }

            @Override public Map<?, ?> getAttributes() {
                return Collections.emptyMap();
            }

            @Override public void addAttributeListener(ComputeTaskSessionAttributeListener lsnr, boolean rewind) {
                // No-op.
            }

            @Override public boolean removeAttributeListener(ComputeTaskSessionAttributeListener lsnr) {
                return false;
            }

            @Override public <K, V> V waitForAttribute(K key, long timeout) throws InterruptedException {
                throw new InterruptedException("Session was closed.");
            }

            @Override public <K, V> boolean waitForAttribute(K key, @Nullable V val, long timeout) throws InterruptedException {
                throw new InterruptedException("Session was closed.");
            }

            @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) throws InterruptedException {
                throw new InterruptedException("Session was closed.");
            }

            @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException {
                throw new InterruptedException("Session was closed.");
            }

            @Override public void saveCheckpoint(String key, Object state) {
                throw new IgniteException("Session was closed.");
            }

            @Override public void saveCheckpoint(String key,
                Object state,
                ComputeTaskSessionScope scope,
                long timeout)
            {
                throw new IgniteException("Session was closed.");
            }

            @Override public void saveCheckpoint(String key,
                Object state,
                ComputeTaskSessionScope scope,
                long timeout,
                boolean overwrite) {
                throw new IgniteException("Session was closed.");
            }

            @Nullable @Override public <T> T loadCheckpoint(String key) throws IgniteException {
                throw new IgniteException("Session was closed.");
            }

            @Override public boolean removeCheckpoint(String key) throws IgniteException {
                throw new IgniteException("Session was closed.");
            }

            @Override public Collection<UUID> getTopology() {
                return Collections.emptyList();
            }

            @Override public IgniteFuture<?> mapFuture() {
                return new IgniteFinishedFutureImpl<Object>();
            }
        };

        ComputeTaskInternalFuture<R> fut = new ComputeTaskInternalFuture<>(ses, ctx);

        fut.onDone(e);

        return fut;
    }

    /**
     * @return Future returned by public API.
     */
    public ComputeTaskFuture<R> publicFuture() {
        return userFut;
    }

    /**
     * Gets task timeout.
     *
     * @return Task timeout.
     */
    public ComputeTaskSession getTaskSession() {
        if (ses == null)
            throw new IllegalStateException("Cannot access task session after future has been deserialized.");

        return ses;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        ctx.security().authorize(ses.getTaskName(), SecurityPermission.TASK_CANCEL, null);

        if (onCancelled()) {
            ctx.task().onCancelled(ses.getId());

            return true;
        }

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ComputeTaskInternalFuture.class, this, "super", super.toString());
    }

    /**
     *
     */
    private static class ComputeFuture<R> extends IgniteFutureImpl<R> implements ComputeTaskFuture<R> {
        /**
         * @param fut Future.
         */
        private ComputeFuture(ComputeTaskInternalFuture<R> fut) {
            super(fut);
        }

        /** {@inheritDoc} */
        @Override public ComputeTaskSession getTaskSession() {
            return ((ComputeTaskInternalFuture<R>)fut).getTaskSession();
        }
    }
}