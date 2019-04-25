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

package org.apache.ignite.loadtests.job;

import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.compute.ComputeJobResultPolicy.REDUCE;

/**
 *
 */
public class GridJobExecutionLoadTestTask implements ComputeTask<Object, Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
        return F.asMap(new GridJobExecutionLoadTestJob(), subgrid.get(0));
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        return REDUCE;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
        return null;
    }
}
