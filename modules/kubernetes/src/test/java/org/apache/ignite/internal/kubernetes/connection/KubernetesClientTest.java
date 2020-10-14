package org.apache.ignite.internal.kubernetes.connection;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

public class KubernetesClientTest extends GridCommonAbstractTest  {

        private static ClientAndServer mockServer;

        @BeforeClass
        public static void startServer() {
            mockServer = startClientAndServer();
        }

        @AfterClass
        public static void stopServer() {
            mockServer.stop();
        }

        @Test
        public void test1() {
            System.out.println(mockServer.getLocalPort());
        }



//    @Test
    public void test() throws Exception {
        Ignite crd = startGrid();

        startGrid(getKubernetesConfiguration());


    }

    private IgniteConfiguration getKubernetesConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        KubernetesConnectionConfiguration k8s = new KubernetesConnectionConfiguration();
        spi.setIpFinder(new TcpDiscoveryKubernetesIpFinder(k8s));
        cfg.setDiscoverySpi(spi);

        return cfg;
    }
}
