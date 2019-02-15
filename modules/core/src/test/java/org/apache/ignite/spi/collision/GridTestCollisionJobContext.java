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

package org.apache.ignite.spi.collision;

import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestJobContext;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;

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