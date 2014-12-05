/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PDoubleDeploymentSelfTest extends GridCommonAbstractTest {
    /** Deployment mode. */
    private IgniteDeploymentMode depMode;

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName());

        // Test requires SHARED mode to test local deployment priority over p2p.
        cfg.setDeploymentMode(depMode);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestBothNodesDeploy(IgniteDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr = new GridTestClassLoader(
                Collections.singletonMap("org/gridgain/grid/p2p/p2p.properties", "resource=loaded"),
                GridP2PTestTask.class.getName(),
                GridP2PTestJob.class.getName()
            );

            Class<? extends ComputeTask<?, ?>> taskCls =
                (Class<? extends ComputeTask<?, ?>>)ldr.loadClass(GridP2PTestTask.class.getName());

            ignite1.compute().localDeployTask(taskCls, ldr);

            Integer res1 = (Integer) ignite1.compute().execute(taskCls.getName(), 1);

            ignite1.compute().undeployTask(taskCls.getName());

            // Wait here 1 sec before the deployment as we have async undeploy.
            Thread.sleep(1000);

            ignite1.compute().localDeployTask(taskCls, ldr);
            ignite2.compute().localDeployTask(taskCls, ldr);

            Integer res2 = (Integer) ignite2.compute().execute(taskCls.getName(), 2);

            info("Checking results...");

            assert res1 == 10 : "Invalid res1 value: " + res1;
            assert res2 == 20 : "Invalid res1 value: " + res1;

            info("Tests passed.");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTestBothNodesDeploy(IgniteDeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTestBothNodesDeploy(IgniteDeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTestBothNodesDeploy(IgniteDeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTestBothNodesDeploy(IgniteDeploymentMode.SHARED);
    }
}
