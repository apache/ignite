package org.apache.ignite.util.mbeans;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.apache.ignite.internal.visor.cluster.VisorCheckDeactivationTask;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/** MBean test of cluster state operations. */
public class GridStateMBeanTest extends GridCommonAbstractTest {
    /** */
    protected boolean persistenceEnabled = false;

    /** */
    protected boolean activeAtStart = false;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClusterStateOnStart(activeAtStart ? ACTIVE : INACTIVE);
        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();
        if(dsCfg==null)
            cfg.setDataStorageConfiguration(dsCfg = new DataStorageConfiguration());

        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistenceEnabled);

        return cfg;
    }

    /**
     * Test deactivation works via JMX
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotDeactivated() throws Exception {
        persistenceEnabled = false;
        activeAtStart = true;

        Ignite ignite = startGrids(1);
        IgniteMXBean mxBean = (IgniteMXBean)ignite;

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        ignite.createCache("non-persistent-cache");

        try {
            mxBean.deactivate(false);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(VisorCheckDeactivationTask.WARN_DEACTIVATION_IN_MEM_CACHES));
        }

        assertTrue(mxBean.active());
        assertEquals(ACTIVE.name(), mxBean.clusterState());

        ignite.destroyCache("non-persistent-cache");

        mxBean.deactivate(false);

        assertFalse(mxBean.active());
        assertEquals(INACTIVE.name(), mxBean.clusterState());

        mxBean.activate();
        assertTrue(mxBean.active());

        ignite.createCache("non-persistent-cache");

        mxBean.deactivate(true);

        assertFalse(mxBean.active());
        assertEquals(INACTIVE.name(), mxBean.clusterState());
    }
}
