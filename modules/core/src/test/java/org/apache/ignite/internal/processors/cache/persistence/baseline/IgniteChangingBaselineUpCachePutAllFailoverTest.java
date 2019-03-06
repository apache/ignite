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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.CachePutAllFailoverAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * Failover test for cache putAll operations when BaselineTopology is changed up
 * (new node is added to BaselineTopology).
 */
public class IgniteChangingBaselineUpCachePutAllFailoverTest extends CachePutAllFailoverAbstractTest {
    /** */
    private static final int GRIDS_COUNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(200L * 1024 * 1024)
                        .setMaxSize(200L * 1024 * 1024)
                        .setCheckpointPageBufferSize(200L * 1024 * 1024)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(GRIDS_COUNT);

        grid(0).active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture createAndRunConcurrentAction(final AtomicBoolean finished, final long endTime) {
        return GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                U.sleep(15_000);

                log.info("Starting node");

                startGrid(GRIDS_COUNT);

                IgniteEx ig0 = grid(0);

                ig0.cluster().setBaselineTopology(baselineNodes(ig0.cluster().forServers().nodes()));

                U.sleep(3_000);

                return null;
            }
        });
    }

    /** */
    private Collection<BaselineNode> baselineNodes(Collection<ClusterNode> clNodes) {
        Collection<BaselineNode> res = new ArrayList<>(clNodes.size());

        for (ClusterNode clN : clNodes)
            res.add(clN);

        return res;
    }
}
