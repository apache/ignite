package org.apache.ignite.util.mbeans;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class GridMBeanBaselineTest extends GridCommonAbstractTest {
    /** Client index. */
    private static final int CLIENT_IDX = 33;

    /** Nodes. */
    public static final int NODES = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(igniteInstanceName.equals(getTestIgniteInstanceName(CLIENT_IDX)))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(2_000)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(200L * 1024 * 1024)
                            .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * Test ignite kernal node in baseline test.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testIgniteKernalNodeInBaselineTest() throws Exception {
        try {
            IgniteEx ignite0 = (IgniteEx)startGrids(NODES);

            startGrid(CLIENT_IDX);

            ignite0.cluster().active(true);

            checkBaselineInFromMBean(ignite0);

            startGrid(NODES);

            checkBaselineInFromMBean(ignite0);

            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

            checkBaselineInFromMBean(ignite0);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void checkBaselineInFromMBean(IgniteEx ignite) {
        Set<Object> cIds = ignite.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        for (Ignite ign : Ignition.allGrids()) {
            IgniteMXBean igniteMXBean = (IgniteMXBean)ign;

            assertEquals(cIds.contains(ign.cluster().localNode().consistentId()),
                igniteMXBean.isNodeInBaseline());
        }
    }

}
