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

import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Class provides implementation for job result.
 */
public class GridJobResultImpl implements ComputeJobResult {
    /** */
    private final ComputeJob job;

    /** */
    private final GridJobSiblingImpl sib;

    /** */
    private final GridJobContextImpl jobCtx;

    /** */
    private ClusterNode node;

    /** */
    private Object data;

    /** */
    private IgniteException ex;

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
    public GridJobResultImpl(ComputeJob job, IgniteUuid jobId, ClusterNode node, GridJobSiblingImpl sib) {
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
    @Override public ComputeJob getJob() {
        return job;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobContext getJobContext() {
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
    @Override public synchronized IgniteException getException() {
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
    public synchronized void onResponse(@Nullable Object data,
        @Nullable IgniteException ex,
        @Nullable Map<Object, Object> jobAttrs,
        boolean isCancelled) {
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