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

package org.apache.ignite.internal.processors.cache.query.jdbc;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task to validate connection. Checks that cache with provided name exists in grid.
 */
public class GridCacheQueryJdbcValidationTask extends ComputeTaskSplitAdapter<String, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize,
        @Nullable final String cacheName) {
        // Register big data usage.
        return F.asSet(new ComputeJobAdapter() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public Object execute() {
                GridDiscoveryManager discoMgr = ((IgniteKernal)ignite).context().discovery();

                for (ClusterNode n : ignite.cluster().nodes())
                    if (discoMgr.cacheNode(n, cacheName))
                        return true;

                return false;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Boolean reduce(List<ComputeJobResult> results) {
        return F.first(results).getData();
    }
}