package org.apache.ignite.internal.processors.cache;

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
    AffinityFunction affinityFunction = null;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setAtomicConfiguration(new AtomicConfiguration()
                .setAffinity(affinityFunction));
    }

    /**
     *
     */
    public void testRendezvousAffinity() throws Exception {
        try {
            affinityFunction = new RendezvousAffinityFunction(false, 10);

            IgniteEx igniteEx = startGrid(0);

            CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

            AffinityFunction aff = cConf.getAffinity();

            assertNotNull(aff);

            assertEquals(aff.partitions(), affinityFunction.partitions());

            assertEquals(aff.getClass(), affinityFunction.getClass());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testTestAffinity() throws Exception {
        try {
            affinityFunction = new TestAffinityFunction("Some value");

            IgniteEx igniteEx = startGrid(0);

            CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

            TestAffinityFunction aff = (TestAffinityFunction)cConf.getAffinity();

            assertNotNull(aff);

            assertEquals(aff.partitions(), affinityFunction.partitions());

            assertEquals(aff.getCustomAttr(), ((TestAffinityFunction)affinityFunction).getCustomAttr());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public void testDefaultAffinity() throws Exception {
        try {
            affinityFunction = null;

            IgniteEx igniteEx = startGrid(0);

            CacheConfiguration cConf = igniteEx.context().cache().cache("ignite-atomics-sys-cache").configuration();

            assertNotNull(cConf.getAffinity());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test Affinity funcation.
     */
    private static class TestAffinityFunction extends RendezvousAffinityFunction {

        private String customAttr;

        /**
         * Default constructor.
         */
        public TestAffinityFunction() {
        }

        public TestAffinityFunction(String customAttr) {
            this.customAttr = customAttr;
        }

        /**
         *
         */
        public String getCustomAttr() {
            return customAttr;
        }
    }
}
