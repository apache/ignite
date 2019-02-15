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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.Iterator;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.testframework.assertions.Assertion;

/**
 * {@link Assertion} that checks that the primary and backup partitions are distributed such that we won't lose any data
 * if we lose a single node. This implies that the cache in question was configured with a backup count of at least one
 * and that all partitions are backed up to a different node from the primary.
 */
public class CacheNodeSafeAssertion implements Assertion {
    /** The {@link Ignite} instance. */
    private final Ignite ignite;

    /** The cache name. */
    private final String cacheName;

    /**
     * Construct a new {@link CacheNodeSafeAssertion} for the given {@code cacheName}.
     *
     * @param ignite The Ignite instance.
     * @param cacheName The cache name.
     */
    public CacheNodeSafeAssertion(Ignite ignite, String cacheName) {
        this.ignite = ignite;
        this.cacheName = cacheName;
    }

    /**
     * @return Ignite instance.
     */
    protected Ignite ignite() {
        return ignite;
    }

    /** {@inheritDoc} */
    @Override public void test() throws AssertionError {
        Affinity<?> affinity = ignite.affinity(cacheName);

        int partCnt = affinity.partitions();

        boolean hostSafe = true;

        boolean nodeSafe = true;

        for (int x = 0; x < partCnt; ++x) {
            // Results are returned with the primary node first and backups after. We want to ensure that there is at
            // least one backup on a different host.
            Collection<ClusterNode> results = affinity.mapPartitionToPrimaryAndBackups(x);

            Iterator<ClusterNode> nodes = results.iterator();

            boolean newHostSafe = false;

            boolean newNodeSafe = false;

            if (nodes.hasNext()) {
                ClusterNode primary = nodes.next();

                // For host safety, get all nodes on the same host as the primary node and ensure at least one of the
                // backups is on a different host. For node safety, make sure at least of of the backups is not the
                // primary.
                Collection<ClusterNode> neighbors = hostSafe ? ignite.cluster().forHost(primary).nodes() : null;

                while (nodes.hasNext()) {
                    ClusterNode backup = nodes.next();

                    if (hostSafe) {
                        if (!neighbors.contains(backup))
                            newHostSafe = true;
                    }

                    if (nodeSafe) {
                        if (!backup.equals(primary))
                            newNodeSafe = true;
                    }
                }
            }

            hostSafe = newHostSafe;

            nodeSafe = newNodeSafe;

            if (!hostSafe && !nodeSafe)
                break;
        }

        if (hostSafe)
            return;

        if (nodeSafe)
            return;

        throw new AssertionError("Cache " + cacheName + " is endangered!");
    }
}
