/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.client.GridClientProtocol.*;
import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Tests that client is able to connect to a grid with only default cache enabled.
 */
public class GridClientDefaultCacheSelfTest extends GridCommonAbstractTest {
    /** Path to jetty config configured with SSL. */
    private static final String REST_JETTY_CFG = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** Port. */
    private static final int TCP_PORT = 11211;

    /** Cached local node id. */
    private UUID locNodeId;

    /** Http port. */
    private static final int HTTP_PORT = 8081;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GG_JETTY_PORT, String.valueOf(HTTP_PORT));

        startGrid().localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();

        System.clearProperty (GG_JETTY_PORT);
    }

    @Override
    protected void beforeTest() throws Exception {
        locNodeId = grid().localNode().id();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setRestJettyPath(REST_JETTY_CFG);

        cfg.setClientConnectionConfiguration(clientCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return Client.
     * @throws GridClientException In case of error.
     */
    private GridClient clientTcp() throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(TCP);
        cfg.setServers(getServerList(TCP_PORT));
        cfg.setDataConfigurations(Collections.singleton(new GridClientDataConfiguration()));

        GridClient gridClient = GridClientFactory.start(cfg);

        assert F.exist(gridClient.compute().nodes(), new GridPredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode n) {
                return n.nodeId().equals(locNodeId);
            }
        });

        return gridClient;
    }

    /**
     * Builds list of connection strings with few different ports.
     * Used to avoid possible failures in case of port range active.
     *
     * @param startPort Port to start list from.
     * @return List of client connection strings.
     */
    private Collection<String> getServerList(int startPort) {
        Collection<String> srvs = new ArrayList<>();

        for (int i = startPort; i < startPort + 10; i++)
            srvs.add(HOST + ":" + i);

        return srvs;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTcp() throws Exception {
        try {
            boolean putRes = cache().putx("key", 1);

            assert putRes : "Put operation failed";

            GridClient client = clientTcp();

            Integer val = client.data().<String, Integer>get("key");

            assert val != null;

            assert val == 1;
        }
        finally {
            GridClientFactory.stopAll();
        }
    }
}
