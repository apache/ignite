/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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