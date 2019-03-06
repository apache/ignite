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

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ClusterNodeMetricsUpdateTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsUpdateFrequency(500);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetrics() throws Exception {
        int NODES = 6;

        Ignite srv0 = startGridsMultiThreaded(NODES / 2);

        client = true;

        startGridsMultiThreaded(NODES / 2, NODES / 2);

        Map<UUID, Integer> expJobs = new HashMap<>();

        for (int i = 0; i < NODES; i++)
            expJobs.put(nodeId(i), 0);

        checkMetrics(NODES, expJobs);

        for (int i = 0; i < NODES; i++) {
            UUID nodeId = nodeId(i);

            IgniteCompute c = srv0.compute(srv0.cluster().forNodeId(nodeId(i)));

            c.call(new DummyCallable(null));

            expJobs.put(nodeId, 1);
        }
    }

    /**
     * @param expNodes Expected nodes.
     * @param expJobs Expected jobs number per node.
     */
    private void checkMetrics0(int expNodes, Map<UUID, Integer> expJobs) {
        List<Ignite> nodes = Ignition.allGrids();

        assertEquals(expNodes, nodes.size());
        assertEquals(expNodes, expJobs.size());

        int totalJobs = 0;

        for (Integer c : expJobs.values())
            totalJobs += c;

        for (final Ignite ignite : nodes) {
            ClusterMetrics m = ignite.cluster().metrics();

            assertEquals(expNodes, m.getTotalNodes());
            assertEquals(totalJobs, m.getTotalExecutedJobs());

            for (Map.Entry<UUID, Integer> e : expJobs.entrySet()) {
                UUID nodeId = e.getKey();

                ClusterGroup g = ignite.cluster().forNodeId(nodeId);

                ClusterMetrics nodeM = g.metrics();

                assertEquals(e.getValue(), (Integer)nodeM.getTotalExecutedJobs());
            }
        }
    }

    /**
     * @param expNodes Expected nodes.
     * @param expJobs Expected jobs number per node.
     * @throws Exception If failed.
     */
    private void checkMetrics(final int expNodes, final Map<UUID, Integer> expJobs) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    checkMetrics0(expNodes, expJobs);
                }
                catch (AssertionError e) {
                    return false;
                }

                return true;
            }
        }, 5000);

        checkMetrics0(expNodes, expJobs);
    }

    /**
     *
     */
    private static class DummyCallable implements IgniteCallable<Object> {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        DummyCallable(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return data;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
