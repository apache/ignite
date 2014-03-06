/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.spi.authentication.noop.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import static org.gridgain.grid.GridSystemProperties.GG_JETTY_PORT;

/**
 * Test authentication over plain HTTP.
 */
public class GridClientHttpRestSecuritySelfTest extends GridCommonAbstractTest {
    /** REST host. */
    protected static final String REST_HOST = "127.0.0.1";

    /** REST port. */
    private int restPort;

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private GridAuthenticationSpi authSpi;

    /** Pattern for authentication failure response. */
    private static final String AUT_FAILURE_PATTERN =
        "\\{\\\"error\\\":\\\".+\\\"\\," +  // Non-empty error message.
        "\\\"response\\\":null\\," +        // null response.
        "\\\"sessionToken\\\":\\\"\\\"," +  // No session token.
        "\\\"successStatus\\\":2\\}";       // Status == 2.

    /** Pattern for authentication passed response. */
    private static final String AUT_PASSED_PATTERN =
        "\\{\\\"error\\\":\\\"\\\"\\," +    // Empty error message.
        "\\\"response\\\":\\[.+\\]\\," +    // List of nodes in response.
        "\\\"sessionToken\\\":\\\"\\\"," +  // No session token.
        "\\\"successStatus\\\":0\\}";       // Status == 0.

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(REST_HOST);

        cfg.setRestEnabled(true);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        cfg.setAuthenticationSpi(authSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientIdRequiredSpi() throws Exception {
        authSpi = new RequireClientIdSpi();
        restPort = 8093;

        System.setProperty(GG_JETTY_PORT, Integer.toString(restPort));

        startGrid();

        try {
            String json = content(F.asMap("cmd", "top"));

            assertTrue("Auth failure not found in response: " + json, Pattern.matches(AUT_FAILURE_PATTERN, json));
        }
        finally {
            stopAllGrids();
            System.clearProperty(GG_JETTY_PORT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientIdNotRequiredSpi() throws Exception {
        authSpi = new GridNoopAuthenticationSpi();
        restPort = 8094;

        System.setProperty(GG_JETTY_PORT, Integer.toString(restPort));

        startGrid();

        try {
            String json = content(F.asMap("cmd", "top"));

            assertTrue("Request did not succeed: " + json, Pattern.matches(AUT_PASSED_PATTERN, json));
        }
        finally {
            stopAllGrids();
            System.clearProperty(GG_JETTY_PORT);
        }
    }

    /**
     * @param params Command parameters.
     * @return Returned content.
     * @throws Exception If failed.
     */
    private String content(Map<String, String> params) throws Exception {
        String addr = "http://" + REST_HOST + ":" + restPort + "/gridgain?";

        for (Map.Entry<String, String> e : params.entrySet())
            addr += e.getKey() + '=' + e.getValue() + '&';

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        InputStream in = conn.getInputStream();

        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in));

        StringBuilder buf = new StringBuilder(256);

        for (String line = rdr.readLine(); line != null; line = rdr.readLine())
            buf.append(line);

        return buf.toString();
    }

    /**
     * Test SPI. Only requires non-empty client ID.
     */
    @GridSpiInfo(
        author = "GridGain Systems",
        url = "www.gridgain.com",
        email = "support@gridgain.com",
        version = "x.x")
    @GridSpiMultipleInstancesSupport(true)
    private static class RequireClientIdSpi implements GridAuthenticationSpi {
        /** {@inheritDoc} */
        @Override public boolean supported(GridSecuritySubjectType subjType) {
            return subjType == GridSecuritySubjectType.REMOTE_CLIENT;
        }

        /** {@inheritDoc} */
        @Override public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
            @Nullable Object credentials)
        throws GridSpiException {
            return subjId != null && subjId.length > 0;
        }

        /** {@inheritDoc} */
        @Override public String getName() {
            return "RequireClientIdSpi";
        }

        /** {@inheritDoc} */
        @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onContextInitialized(GridSpiContext spiCtx) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onContextDestroyed() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }

        @Override public void setJson(String json) {
            // No-op.
        }
    }
}
