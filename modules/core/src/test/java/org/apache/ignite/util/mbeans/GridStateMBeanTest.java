package org.apache.ignite.util.mbeans;

import org.apache.ignite.Ignite;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.DATA_LOST_ON_DEACTIVATION_WARNING;

/** MBean test of cluster state operations. */
public class GridStateMBeanTest extends GridCommonAbstractTest {
    /**
     * Test deactivation works via JMX
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotDeactivated() throws Exception {
        Ignite ignite = startGrids(1);
        IgniteMXBean mxBean = (IgniteMXBean)ignite;

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        try {
            // There is at least the system cache. Might be internal caches. Difficult to predict data erasure.
            mxBean.deactivate(false);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(DATA_LOST_ON_DEACTIVATION_WARNING));
        }

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        mxBean.activate();
        assertTrue(mxBean.active());

        mxBean.deactivate(true);

        assertFalse(mxBean.active());
        assertEquals(INACTIVE.name(), mxBean.clusterState());
    }
}
