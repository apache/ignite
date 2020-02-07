package org.apache.ignite.util.mbeans;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.DATA_LOST_ON_DEACTIVATION_WARNING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** MBean test of cluster state operations. */
public class GridMBeanClusterStateTest extends GridCommonAbstractTest {
    /**
     * Test deactivation works via JMX
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotDeactivated() throws Exception {
        IgniteEx ignite = startGrids(2);
        IgniteMXBean mxBean = (IgniteMXBean)ignite;

        IgniteCache<?, ?> cache = ignite.createCache("non-persistent-cache");

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        assertFalse(ignite.context().state().isDeactivationSafe());

        assertThrows(log, () -> {
            mxBean.active(false);
            return null;
        }, Exception.class, DATA_LOST_ON_DEACTIVATION_WARNING);

        assertThrows(log, () -> {
            mxBean.clusterState(INACTIVE.name(), false);
            return null;
        }, Exception.class, DATA_LOST_ON_DEACTIVATION_WARNING);

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        mxBean.clusterState(INACTIVE.name(), true);

        assertFalse(mxBean.active());
        assertEquals(INACTIVE.name(), mxBean.clusterState());
    }
}
