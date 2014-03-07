/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * The test does the following:
 *
 * 1. The test should execute a task in SHARED_DEPLOY mode, restart a node with new version and make sure that a
 *      new class loader is created on remote node.
 * 2. The test should execute a task in SHARED_DEPLOY mode, restart a node with same version and make sure
 *      that the same class loader is created on remote node.
 * 3. The test should execute a task in SHARED_UNDEPLOY mode, restart a node with same version and
 *      make sure that a new class loader is created on remote node.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ObjectEquality", "unchecked"})
public class GridP2PUserVersionChangeSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. */
    private GridDeploymentMode depMode;

    /** Class Name of task. */
    private static final String TEST_TASK_NAME = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** IP finder. */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    public GridP2PUserVersionChangeSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);
        cfg.setNetworkTimeout(10000);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRedeployVersionChangeContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        checkRedeployVersionChange();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRedeployVersionChangeSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        checkRedeployVersionChange();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRedeployVersionChange() throws Exception {
        try {
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) },
                Collections.singletonMap("META-INF/gridgain.xml", makeUserVersion("1").getBytes()));

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            grid2.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((GridDeploymentEvent) evt).alias().equals(TEST_TASK_NAME)) {
                        undeployed.countDown();
                    }

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            int[] res1 = (int[])grid1.compute().execute(task1, grid2.localNode().id()).get();

            stopGrid(1);

            ldr.setResourceMap(Collections.singletonMap("META-INF/gridgain.xml", makeUserVersion("2").getBytes()));

            grid1 = startGrid(1);

            int[] res2 = (int[])grid1.compute().execute(task1, grid2.localNode().id()).get();

            assert res1[0] != res2[0];
            assert res1[1] != res2[1];

            // Allow P2P timeout to expire.
            assert undeployed.await(30000, MILLISECONDS);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeployOnNodeRestartContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        try {
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) });

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            grid2.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((GridDeploymentEvent) evt).alias().equals(TEST_TASK_NAME)) {
                        undeployed.countDown();
                    }

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            int[] res1 = (int[])grid1.compute().execute(task1, grid2.localNode().id()).get();

            stopGrid(1);

            grid1 = startGrid(1);

            int[] res2 = (int[]) grid1.compute().execute(task1, grid2.localNode().id()).get();

            assert !undeployed.await(3000, MILLISECONDS);

            assert res1[0] == res2[0];
            assert res1[1] == res2[1];
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeployOnNodeRestartSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        try {
            Grid grid1 = startGrid(1);
            Grid grid2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) });

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            grid2.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((GridDeploymentEvent) evt).alias().equals(TEST_TASK_NAME)) {
                        undeployed.countDown();
                    }

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            final CountDownLatch discoLatch = new CountDownLatch(1);

            grid2.events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    if (evt.type() == EVT_NODE_LEFT) {
                        discoLatch.countDown();
                    }

                    return true;
                }
            }, EVT_NODE_LEFT);

            int[] res1 = (int[])grid1.compute().execute(task1, grid2.localNode().id()).get();

            stopGrid(1);

            assert discoLatch.await(1000, MILLISECONDS);

            assert undeployed.await(1000, MILLISECONDS);

            grid1 = startGrid(1);

            int[] res2 = (int[]) grid1.compute().execute(task1, grid2.localNode().id()).get();

            assert res1[0] != res2[0];
            assert res1[1] != res2[1];
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Creates content of META-INF/gridgain.xml for specified user version.
     *
     * @param userVersion Version to create.
     * @return content of META-INF/gridgain.xml.
     */
    private String makeUserVersion(String userVersion) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " +
            "<beans xmlns=\"http://www.springframework.org/schema/beans\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:util=\"http://www.springframework.org/schema/util\" " +
            "xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd " +
            "http://www.springframework.org/schema/util http://www.springframework.org/schema/util/spring-util-2.0.xsd\"> " +
            "<bean id=\"userVersion\" class=\"java.lang.String\"><constructor-arg value=\"" + userVersion + "\"/></bean> " +
            "</beans>";
    }
}
