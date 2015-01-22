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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * This class provide implementation for task future.
 * @param <R> Type of the task result returning from {@link org.apache.ignite.compute.ComputeTask#reduce(List)} method.
 */
public class GridTaskFutureImpl<R> extends GridFutureAdapter<R> implements ComputeTaskFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ComputeTaskSession ses;

    /** */
    private GridKernalContext ctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridTaskFutureImpl() {
        // No-op.
    }

    /**
     * @param ses Task session instance.
     * @param ctx Kernal context.
     */
    public GridTaskFutureImpl(ComputeTaskSession ses, GridKernalContext ctx) {
        super(ctx);

        assert ses != null;
        assert ctx != null;

        this.ses = ses;
        this.ctx = ctx;
    }

    /**
     * Gets task timeout.
     *
     * @return Task timeout.
     */
    @Override public ComputeTaskSession getTaskSession() {
        if (ses == null)
            throw new IllegalStateException("Cannot access task session after future has been deserialized.");

        return ses;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        ctx.security().authorize(ses.getTaskName(), GridSecurityPermission.TASK_CANCEL, null);

        checkValid();

        if (onCancelled()) {
            ctx.task().onCancelled(ses.getId());

            return true;
        }

        return isCancelled();
    }

    /**
     * Cancel task on master leave event. Does not send cancel request to remote jobs and invokes master-leave
     * callback on local jobs.
     *
     * @return {@code True} if future was cancelled (i.e. was not finished prior to this call).
     * @throws IgniteCheckedException If failed.
     */
    public boolean cancelOnMasterLeave() throws IgniteCheckedException {
        checkValid();

        if (onCancelled()) {
            // Invoke master-leave callback on spawned jobs on local node and then cancel them.
            for (ClusterNode node : ctx.discovery().nodes(ses.getTopology())) {
                if (ctx.localNodeId().equals(node.id())) {
                    ctx.job().masterLeaveLocal(ses.getId());

                    ctx.job().cancelJob(ses.getId(), null, false);
                }
            }

            return true;
        }

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskFutureImpl.class, this, "super", super.toString());
    }
}
