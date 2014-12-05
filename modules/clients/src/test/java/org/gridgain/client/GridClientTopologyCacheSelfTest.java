/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests topology caching.
 */
public class GridClientTopologyCacheSelfTest extends GridCommonAbstractTest {
    static {
        // Override default port.
        System.setProperty(GG_JETTY_PORT, Integer.toString(8081));
    }

    /** Host. */
    public static final String HOST = "127.0.0.1";

    /** Port. */
    public static final int BINARY_PORT = 11212;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyCache() throws Exception {
        testTopologyCache(
            true, // metricsCache
            true, // attrsCache
            false,// autoFetchMetrics
            false,// autoFetchAttrs
            false,// metricsBeforeRefresh
            false,// attrsBeforeRefresh
            true, // metricsAfterRefresh
            true);// attrsAfterRefresh

        testTopologyCache(
            false, // metricsCache
            false, // attrsCache
            false,// autoFetchMetrics
            false,// autoFetchAttrs
            false, // metricsBeforeRefresh
            false, // attrsBeforeRefresh
            false, // metricsAfterRefresh
            false);// attrsAfterRefresh

        testTopologyCache(
            true,  // metricsCache
            false, // attrsCache
            false, // autoFetchMetrics
            false, // autoFetchAttrs
            false, // metricsBeforeRefresh
            false, // attrsBeforeRefresh
            true,  // metricsAfterRefresh
            false);// attrsAfterRefresh

        testTopologyCache(
            false, // metricsCache
            true,  // attrsCache
            false, // autoFetchMetrics
            false, // autoFetchAttrs
            false, // metricsBeforeRefresh
            false, // attrsBeforeRefresh
            false, // metricsAfterRefresh
            true); // attrsAfterRefresh
    }

    public void testAutofetch() throws Exception {
        testTopologyCache(
            true, // metricsCache
            true, // attrsCache
            true, // autoFetchMetrics
            true, // autoFetchAttrs
            true, // metricsBeforeRefresh
            true, // attrsBeforeRefresh
            true, // metricsAfterRefresh
            true);// attrsAfterRefresh

        testTopologyCache(
            true, // metricsCache
            true, // attrsCache
            false,// autoFetchMetrics
            true, // autoFetchAttrs
            false,// metricsBeforeRefresh
            true, // attrsBeforeRefresh
            true, // metricsAfterRefresh
            true);// attrsAfterRefresh

        testTopologyCache(
            true, // metricsCache
            true, // attrsCache
            true, // autoFetchMetrics
            false,// autoFetchAttrs
            true, // metricsBeforeRefresh
            false,// attrsBeforeRefresh
            true, // metricsAfterRefresh
            true);// attrsAfterRefresh

        testTopologyCache(
            true, // metricsCache
            true, // attrsCache
            false,// autoFetchMetrics
            false,// autoFetchAttrs
            false,// metricsBeforeRefresh
            false,// attrsBeforeRefresh
            true, // metricsAfterRefresh
            true);// attrsAfterRefresh
    }

    /**
     * Starts new client with the given caching configuration and refreshes topology,
     * Checks node metrics and attributes availability according to the given flags
     * before and after refresh.
     *
     * @param metricsCache Should metrics be cached?
     * @param attrsCache Should attributes be cached?
     * @param autoFetchMetrics Should metrics be fetched automatically?
     * @param autoFetchAttrs Should attributes be fetched automatically?
     * @param metricsBeforeRefresh Should metrics be available before topology refresh?
     * @param attrsBeforeRefresh Should attributes be available before topology refresh?
     * @param metricsAfterRefresh Should metrics be available after topology refresh?
     * @param attrsAfterRefresh Should attributes be available after topology refresh?
     * @throws Exception If failed.
     */
    private void testTopologyCache(boolean metricsCache, boolean attrsCache,
        boolean autoFetchMetrics, boolean autoFetchAttrs,
        boolean metricsBeforeRefresh, boolean attrsBeforeRefresh,
        boolean metricsAfterRefresh, boolean attrsAfterRefresh) throws Exception {
        GridClient client = client(metricsCache, attrsCache, autoFetchMetrics, autoFetchAttrs);

        try {
            // Exclude cache metrics because there is no background refresh for them.
            assertEquals(metricsBeforeRefresh, metricsAvailable(client, false));
            assertEquals(attrsBeforeRefresh, attrsAvailable(client));

            client.compute().refreshTopology(true, true);
            client.data(CACHE_NAME).metrics();

            assertEquals(metricsAfterRefresh, metricsAvailable(client, true));
            assertEquals(attrsAfterRefresh, attrsAvailable(client));
        }
        finally {
            GridClientFactory.stop(client.id(), false);
        }
    }

    /**
     * @param client Client instance.
     * @param includeCache If {@code true} then cache metrics should be considered
     * and their consistency with node metrics should be asserted, otherwise consider only node metrics.
     * @return {@code true} if node metrics available through this client,
     * {@code false} otherwise.
     * @throws GridClientException If data projection is not available.
     */
    private boolean metricsAvailable(GridClient client, boolean includeCache) throws GridClientException {
        if (includeCache) {
            boolean node = nodeMetricsAvailable(client);
            boolean cache = client.data(CACHE_NAME).cachedMetrics() != null;

            assertTrue("Inconsistency between cache and node metrics cache.", node == cache);

            return node && cache;
        }
        else
            return nodeMetricsAvailable(client);
    }

    /**
     * @param client Client instance.
     * @return {@code true} if node node metrics available through this client,
     * {@code false} otherwise.
     */
    private boolean nodeMetricsAvailable(GridClient client) throws GridClientException {
        for (GridClientNode node : client.compute().nodes())
            if (node.metrics() != null)
                return true;

        return false;
    }

    /**
     * @param client Client instance.
     * @return {@code true} if node attributes available through this client,
     * {@code false} otherwise.
     */
    private boolean attrsAvailable(GridClient client) throws GridClientException {
        for (GridClientNode node : client.compute().nodes())
            if (node.attributes() != null && !node.attributes().isEmpty())
                return true;

        return false;
    }

    /**
     * @param metricsCache Should metrics cache be enabled?
     * @param attrsCache Should attributes cache be enabled?
     * @return Client.
     * @throws GridClientException In case of error.
     */
    private GridClient client(boolean metricsCache, boolean attrsCache,
        boolean autoFetchMetrics, boolean autoFetchAttrs) throws GridClientException {
        GridClientDataConfiguration cache = new GridClientDataConfiguration();

        cache.setName(CACHE_NAME);

        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setServers(Arrays.asList(HOST + ":" + BINARY_PORT));
        cfg.setEnableMetricsCache(metricsCache);
        cfg.setEnableAttributesCache(attrsCache);
        cfg.setAutoFetchMetrics(autoFetchMetrics);
        cfg.setAutoFetchAttributes(autoFetchAttrs);
        cfg.setDataConfigurations(Collections.singleton(cache));

        return GridClientFactory.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setSwapEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);

        assert cfg.getClientConnectionConfiguration() == null;

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setRestTcpPort(BINARY_PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(disco);

        return cfg;
    }
}
