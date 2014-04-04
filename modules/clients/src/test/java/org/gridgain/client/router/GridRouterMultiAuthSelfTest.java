/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.client.*;
import org.gridgain.client.router.impl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.passcode.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.spi.securesession.rememberme.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.client.integration.GridClientAbstractSelfTest.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridRouterMultiAuthSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    public static final int BINARY_PORT = 11212;

    /** Port number to use by router. */
    private static final int ROUTER_PORT = BINARY_PORT + 1;

    /* Passcode. */
    private static final String PASSCODE = "s3cret";

    /** TCp router instance. */
    private static GridTcpRouterImpl router;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();

        router = new GridTcpRouterImpl(routerConfiguration());

        router.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (router != null)
            router.stop();

        stopGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(CACHE_NAME).clearAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConsequentFailure() throws Exception {
        try {
            GridClient authClient = client(PASSCODE);

            authClient.data(CACHE_NAME).put("1", 1);

            try {
                GridClient brokenClient = client("WRONG PASSCODE");

                brokenClient.data(CACHE_NAME).put("1", 1);
            }
            catch (GridClientDisconnectedException e) {
                assertTrue(X.hasCause(e, GridClientAuthenticationException.class));
            }

            assertEquals(1, authClient.data(CACHE_NAME).get("1"));
        }
        finally {
            GridClientFactory.stopAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLeadingFailure() throws Exception {
        try {
            try {
                GridClient brokenClient = client("WRONG PASSCODE");

                brokenClient.data(CACHE_NAME).put("1", 1);
            }
            catch (GridClientDisconnectedException e) {
                assertTrue(X.hasCause(e, GridClientAuthenticationException.class));
            }

            GridClient authClient = client(PASSCODE);

            authClient.data(CACHE_NAME).put("1", 1);

            assertEquals(1, authClient.data(CACHE_NAME).get("1"));
        }
        finally {
            GridClientFactory.stopAll();
        }
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        GridPasscodeAuthenticationSpi authSpi = new GridPasscodeAuthenticationSpi();

        authSpi.setPasscodes(F.asMap(GridSecuritySubjectType.REMOTE_CLIENT, PASSCODE));

        GridSecureSessionSpi sesSpi = new GridRememberMeSecureSessionSpi();

        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);
        cfg.setRestTcpPort(BINARY_PORT);
        cfg.setRestEnabled(true);
        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfg);
        cfg.setAuthenticationSpi(authSpi);
        cfg.setSecureSessionSpi(sesSpi);

        return cfg;
    }

    /**
     * @return Router configuration.
     */
    public GridTcpRouterConfiguration routerConfiguration() throws GridException {
        GridTcpRouterConfiguration cfg = new GridTcpRouterConfiguration();

        cfg.setHost(HOST);
        cfg.setPort(ROUTER_PORT);
        cfg.setPortRange(0);
        cfg.setServers(Collections.singleton(HOST+":"+BINARY_PORT));
        cfg.setCredentials(PASSCODE);
        cfg.setLogger(new GridLog4jLogger(ROUTER_LOG_CFG));

        return cfg;
    }

    /**
     * @param passcode Passcode.
     * @return Client.
     * @throws GridClientException In case of error.
     */
    private GridClient client(String passcode) throws GridClientException {
        return GridClientFactory.start(clientConfiguration(passcode));
    }

    /** {@inheritDoc} */
    private GridClientConfiguration clientConfiguration(String passcode) {
        GridClientConfiguration cfg = new GridClientConfiguration();

        GridClientDataConfiguration cache = new GridClientDataConfiguration();

        cache.setName(CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(cache));
        cfg.setServers(Collections.<String>emptySet());
        cfg.setRouters(Collections.singleton(HOST + ":" + ROUTER_PORT));
        cfg.setCredentials(passcode);

        return cfg;
    }
}
