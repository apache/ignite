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

package org.apache.ignite.internal.processors.cache.query.jdbc;

import java.util.Collection;
import java.util.Collections;
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
    @Override protected Collection<? extends ComputeJob> split(int gridSize, @Nullable final String cacheName) {
        return Collections.singleton(new ComputeJobAdapter() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public Object execute() {
                if (cacheName == null)
                    return true;

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