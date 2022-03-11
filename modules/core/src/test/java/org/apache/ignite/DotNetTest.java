package org.apache.ignite;

import java.io.Serializable;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/** */
public class DotNetTest extends GridCommonAbstractTest implements Serializable {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCheckpointSpi(null);

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setLocalPort(59000)
            .setIpFinder(new TcpDiscoveryVmIpFinder()
                .setAddresses(F.asList("127.0.0.1:59000"))));

        cfg.setUserAttributes(F.asMap("ROLE", "node"));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

        // ?
        cfg.setPeerClassLoadingEnabled(false);

        cfg.setIncludeEventTypes(EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_CLIENT_NODE_DISCONNECTED);

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        Ignite cln = startClientGrid(1);

        Thread.sleep(500);

        ITestService svc = cln.services().serviceProxy("Service1", ITestService.class, false);

        String res = svc.Test("test");

        System.out.println("RESULT " + res);
    }

    /** */
    @Test
    public void testFullLocal() throws Exception {
        Ignite crd = startGrid(0);

        crd.services().deploy(new ServiceConfiguration()
            .setName("Service1")
            .setService(new TestService())
            .setMaxPerNodeCount(1)
            .setTotalCount(1)
            .setNodeFilter((n) -> "node".equals(n.attribute("ROLE"))));

        Ignite cln = startClientGrid(1);

        ITestService svc = cln.services().serviceProxy("Service1", ITestService.class, false);

        String res = svc.Test("test");

        System.out.println("RESULT " + res);
    }

    public interface ITestService
    {
        String Test(String test);
    }


    public class TestService implements ITestService, Service, Serializable {

        @Override public String Test(String test) {
            return test;
        }
    }

}
