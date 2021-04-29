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

package org.apache.ignite.internal.client.thin;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task which returns node id for routing node and list of node ids for each node was affected.
 */
@ComputeTaskName(ComputeTaskTest.TEST_TASK_NAME)
public class TestTask extends ComputeTaskAdapter<Long, T2<UUID, Set<UUID>>> {
    /** Ignite. */
    @IgniteInstanceResource
    Ignite ignite;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
        return subgrid.stream().collect(Collectors.toMap(node -> new TestJob(arg), node -> node));
    }

    /** {@inheritDoc} */
    @Nullable @Override public T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
        return new T2<>(ignite.cluster().localNode().id(),
            results.stream().map(res -> (UUID)res.getData()).collect(Collectors.toSet()));
    }
}
