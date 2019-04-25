/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.collision;

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.GridJobSessionImpl;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.spi.collision.CollisionJobContext;

/**
 * Adapter for {@link org.apache.ignite.spi.collision.CollisionJobContext}.
 */
public abstract class GridCollisionJobContextAdapter implements CollisionJobContext {
    /** */
    private final GridJobWorker jobWorker;

    /**
     * @param jobWorker Job worker instance.
     */
    protected GridCollisionJobContextAdapter(GridJobWorker jobWorker) {
        assert jobWorker != null;

        this.jobWorker = jobWorker;
    }

    /** {@inheritDoc} */
    @Override public GridJobSessionImpl getTaskSession() {
        return jobWorker.getSession();
    }

    /** {@inheritDoc} */
    @Override public ComputeJobContext getJobContext() {
        return jobWorker.getJobContext();
    }

    /**
     * @return Job worker.
     */
    public GridJobWorker getJobWorker() {
        return jobWorker;
    }

    /** {@inheritDoc} */
    @Override public ComputeJob getJob() {
        return jobWorker.getJob();
    }
}