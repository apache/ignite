package org.apache.ignite.internal.processors.service;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test class for verifying the behavior of Ignite service deployment when
 * specified service classes are not found or when service initialization fails.
 */
public class IgniteServiceDeploymentFailureTest extends GridCommonAbstractTest {
    /** */
    private static final String NOOP_SERVICE_CLS_NAME = "org.apache.ignite.tests.p2p.NoopService";

    /** */
    public static final String NODE_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.ExcludeNodeFilter";

    /** */
    private static final int CLIENT_NODES_CNT = 3;

    /** */
    private static ClassLoader extClsLdr;

    /** Atomic InitThrowService#init calls counter */
    private static AtomicInteger initCounter = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        extClsLdr = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();

        extClsLdr = null;
    }

    /**
     *  Tests that deploying a service with a missing class causes a ServiceDeploymentException.
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testFailWhenClassNotFound() throws Exception {
        IgniteEx srv = startGrid(getConfiguration("server"));
        IgniteEx cli = startClientGrid(1);

        ServiceConfiguration svcCfg = new ServiceConfiguration()
                .setName("TestDeploymentService")
                .setService(((Class<Service>)extClsLdr.loadClass(NOOP_SERVICE_CLS_NAME)).getDeclaredConstructor().newInstance())
                .setNodeFilter(((Class<IgnitePredicate<ClusterNode>>)extClsLdr.loadClass(NODE_FILTER_CLS_NAME))
                        .getConstructor(UUID.class)
                        .newInstance(cli.configuration().getNodeId()))
                .setTotalCount(1);

        assertThrowsWithCause(() -> cli.services().deploy(svcCfg), ServiceDeploymentException.class);

        assertTrue(cli.services().serviceDescriptors().isEmpty());
    }

    private static class InitThrowingService implements Service {
        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            initCounter.incrementAndGet();
            throw new Exception("Service init exception");
        }
    }

    /**
     * Tests that service descriptors are clear on server node after attempt of
     * deploying a service which throws an exception during initialization
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testServerDescriptorsOfFailedServices() throws Exception {
        IgniteEx srv = startGrid(getConfiguration("server"));

        IgniteEx[] clients = new IgniteEx[CLIENT_NODES_CNT];
        ServiceConfiguration[] configs = new ServiceConfiguration[CLIENT_NODES_CNT];

        for (int i = 0; i < CLIENT_NODES_CNT; i++) {
            clients[i] = startClientGrid(i);
            configs[i] = new ServiceConfiguration()
                    .setName("TestDeploymentService" + i)
                    .setService(new InitThrowingService())
                    .setTotalCount(1);
        }

        for (int i = 0; i < CLIENT_NODES_CNT; i++) {
            int finalI = i;
            assertThrowsWithCause(() -> clients[finalI].services().deploy(configs[finalI]), ServiceDeploymentException.class);
        }

        System.out.println("init attemps: " + initCounter.get());

        assertTrue(srv.services().serviceDescriptors().isEmpty());
    }

    /**
     * Tests that service descriptors are clear on client nodes after attempt of
     * deploying a service which throws an exception during initialization
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testClientDescriptorsOfFailedServices() throws Exception {
        IgniteEx srv = startGrid(getConfiguration("server"));

        IgniteEx[] clients = new IgniteEx[1];
        ServiceConfiguration[] configs = new ServiceConfiguration[clients.length];

        for (int i = 0; i < clients.length; i++) {
            clients[i] = startClientGrid(i);
            configs[i] = new ServiceConfiguration()
                    .setName("TestDeploymentService" + i)
                    .setService(new InitThrowingService())
                    .setTotalCount(1);
        }

        for (int i = 0; i < clients.length; i++) {
            int finalI = i;
            assertThrowsWithCause(() -> clients[finalI].services().deploy(configs[finalI]), ServiceDeploymentException.class);
        }

        System.out.println("init attemps: " + initCounter.get());

        for (int i = 0; i < clients.length; i++)
            assertTrue(clients[i].services().serviceDescriptors().isEmpty());
    }
}

