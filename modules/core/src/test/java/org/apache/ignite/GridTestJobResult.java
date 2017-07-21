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

package org.apache.ignite;

import java.io.Serializable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;

/**
 * Test job result.
 */
public class GridTestJobResult implements ComputeJobResult {
    /** */
    private final Serializable data;

    /** */
    private final IgniteException e;

    /** */
    private final ComputeJob job;

    /** */
    private final ClusterNode node;

    /** */
    private final ComputeJobContext jobCtx;

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
    public GridTestJobResult(Serializable data,
        IgniteException e,
        ComputeJob job,
        ClusterNode node,
        ComputeJobContext jobCtx) {
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

    /** {@inheritDoc} */ @Override public IgniteException getException() { return e; }

    /** {@inheritDoc} */ @Override public boolean isCancelled() { return false; }

    /** {@inheritDoc} */ @Override public ComputeJob getJob() { return job; }

    /** {@inheritDoc} */ @Override public ClusterNode getNode() { return node; }

    /** {@inheritDoc} */ @Override public ComputeJobContext getJobContext() { return jobCtx; }
}