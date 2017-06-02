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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
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
    public void testRendezvousAffinity() throws Exception {
        try {
            affinityFunction = new RendezvousAffinityFunction(false, 10);

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

                AffinityFunction aff = cConf.getAffinity();

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
    public void testTestAffinity() throws Exception {
        try {
            affinityFunction = new TestAffinityFunction("Some value");

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

                TestAffinityFunction aff = (TestAffinityFunction)cConf.getAffinity();

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
    public void testDefaultAffinity() throws Exception {
        try {
            affinityFunction = null;

            startGrids(3);

            for (int i = 0; i < 3; i++) {
                IgniteEx igniteEx = grid(i);

                CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

                assertNotNull(cConf.getAffinity());
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
