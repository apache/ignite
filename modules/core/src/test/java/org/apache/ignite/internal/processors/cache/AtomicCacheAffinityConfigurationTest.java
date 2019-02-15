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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.datastructures.AtomicDataStructureProxy;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 */
@RunWith(JUnit4.class)
public class AtomicCacheAffinityConfigurationTest extends GridCommonAbstractTest {
    /** Affinity function. */
    private AffinityFunction affinityFunction;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setAtomicConfiguration(new AtomicConfiguration()
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(affinityFunction));
    }

    /**
     * @throws Exception If failed.
     *
     */
    @Test
    public void testRendezvousAffinity() throws Exception {
        try {
            affinityFunction = new RendezvousAffinityFunction(false, 10);

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                IgniteAtomicLong atomic = igniteEx.atomicLong("test", 0, true);

                GridCacheContext cctx = GridTestUtils.getFieldValue(atomic, AtomicDataStructureProxy.class, "ctx");

                AffinityFunction aff = cctx.config().getAffinity();

                assertNotNull(aff);

                assertEquals(aff.partitions(), affinityFunction.partitions());

                assertEquals(aff.getClass(), affinityFunction.getClass());
            }

            checkAtomics();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTestAffinity() throws Exception {
        try {
            affinityFunction = new TestAffinityFunction("Some value");

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                IgniteAtomicLong atomic = igniteEx.atomicLong("test", 0, true);

                GridCacheContext cctx = GridTestUtils.getFieldValue(atomic, AtomicDataStructureProxy.class, "ctx");

                TestAffinityFunction aff = (TestAffinityFunction) cctx.config().getAffinity();

                assertNotNull(aff);

                assertEquals(aff.partitions(), affinityFunction.partitions());

                assertEquals(aff.getCustomAttribute(), ((TestAffinityFunction)affinityFunction).getCustomAttribute());
            }

            checkAtomics();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultAffinity() throws Exception {
        try {
            affinityFunction = null;

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                IgniteAtomicLong atomic = igniteEx.atomicLong("test", 0, true);

                GridCacheContext cctx = GridTestUtils.getFieldValue(atomic, AtomicDataStructureProxy.class, "ctx");

                AffinityFunction aff = cctx.config().getAffinity();

                assertNotNull(aff);
            }

            checkAtomics();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private void checkAtomics() {
        Ignite node0 = grid(0);

        node0.atomicLong("l1", 0, true).incrementAndGet();
        node0.atomicSequence("s1", 10, true);

        for (int i = 0; i < 3; i++) {
            assertEquals(1, ignite(i).atomicLong("l1", 0, false).get());

            assertNotNull(ignite(i).atomicSequence("s1", 0, false));

            ignite(i).atomicSequence("s1", 0, false).getAndIncrement();
        }
    }

    /**
     * Test affinity function.
     */
    private static class TestAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private String customAttr;

        /**
         * Default constructor.
         */
        public TestAffinityFunction() {
            // No-op.
        }

        /**
         * @param customAttr Custom attribute.
         */
        TestAffinityFunction(String customAttr) {
            this.customAttr = customAttr;
        }

        /**
         * @return Custom attribute.
         */
        String getCustomAttribute() {
            return customAttr;
        }
    }
}
