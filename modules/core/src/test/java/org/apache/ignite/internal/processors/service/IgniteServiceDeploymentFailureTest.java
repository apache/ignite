package org.apache.ignite.internal.processors.service;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
    private static final int SERVER_NODES_CNT = 5;

    /** */
    private static final int CLIENT_NODES_CNT = 4;

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
        /** */
        public static final AtomicInteger initCounter = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            initCounter.incrementAndGet();
            throw new Exception("Service init exception");
        }
    }

    private static class NoopService implements Service {
        /** */
        public static final AtomicInteger initCounter = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            initCounter.incrementAndGet();
        }
    }

    /**
     * Tests that service descriptors are clear after an attempt of deploying
     * a service which throws an exception during initialization
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testServerDescriptorsOfFailedServices() throws Exception {
        final int NOOP_SERVICE_TOTAL_COUNT = 20;
        final int NOOP_SERVICE_MAX_PER_NODE_COUNT_0 = 2;
        final int NOOP_SERVICE_MAX_PER_NODE_COUNT_1 = 4;

        final int THROW_SERVICE_TOTAL_COUNT = 10;
        final int THROW_SERVICE_MAX_PER_NODE_COUNT = 2;

        IgniteEx[] servers = new IgniteEx[SERVER_NODES_CNT];
        for (int i = 0; i < SERVER_NODES_CNT; i++)
            servers[i] = startGrid(getConfiguration("server" + i));

        IgniteEx[] clients = new IgniteEx[CLIENT_NODES_CNT];
        for (int i = 0; i < clients.length; i++)
            clients[i] = startClientGrid(i);

        ServiceConfiguration noopServiceConfig = new ServiceConfiguration()
                .setName(NoopService.class.getSimpleName())
                .setService(new NoopService())
                .setTotalCount(NOOP_SERVICE_TOTAL_COUNT)
                .setMaxPerNodeCount(NOOP_SERVICE_MAX_PER_NODE_COUNT_0);

        ServiceConfiguration throwServiceConfig = new ServiceConfiguration()
                .setName(InitThrowingService.class.getSimpleName())
                .setService(new InitThrowingService())
                .setTotalCount(THROW_SERVICE_TOTAL_COUNT)
                .setMaxPerNodeCount(THROW_SERVICE_MAX_PER_NODE_COUNT);

        clients[0].services().deploy(noopServiceConfig);
        assertEquals(NOOP_SERVICE_MAX_PER_NODE_COUNT_0 * SERVER_NODES_CNT,
                getTotalInstancesCount(clients[0], NoopService.class.getSimpleName()));
        System.out.println("NoopService instances count before faulty deploy: " + getTotalInstancesCount(clients[0], NoopService.class.getSimpleName()));


        assertThrowsWithCause(() -> clients[0].services().deploy(throwServiceConfig), ServiceDeploymentException.class);

        for (IgniteEx server : servers)
            assertTrue(server.services().serviceDescriptors().isEmpty());
        for (IgniteEx client : clients)
            assertTrue(client.services().serviceDescriptors().isEmpty());

        System.out.println("InitThrowingService instances count: " + getTotalInstancesCount(clients[0], "InitThrowingService"));


        clients[0].services().cancel(NoopService.class.getSimpleName());

        noopServiceConfig.setMaxPerNodeCount(NOOP_SERVICE_MAX_PER_NODE_COUNT_1);
        clients[0].services().deploy(noopServiceConfig);

        assertEquals(NOOP_SERVICE_TOTAL_COUNT, getTotalInstancesCount(clients[0], InitThrowingService.class.getSimpleName()));

        System.out.println("NoopService instances count after faulty deploy: " + getTotalInstancesCount(clients[0], NoopService.class.getSimpleName()));
    }

    private static int getTotalInstancesCount(IgniteEx igniteEx, String serviceName) {
        Optional<ServiceDescriptor> desc = igniteEx.services().serviceDescriptors().stream().
                filter(descriptor -> descriptor.name().equals(serviceName)).findAny();
        assertTrue(desc.isPresent());
        return desc.get().topologySnapshot().values().stream().mapToInt(i -> i).sum();
    }
}
