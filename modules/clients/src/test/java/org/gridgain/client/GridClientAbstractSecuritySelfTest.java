/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.authentication.passcode.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.spi.GridSecuritySubjectType.*;

/**
 * Tests client security.
 */
public abstract class GridClientAbstractSecuritySelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    public static final int JETTY_PORT = 8081;

    /** Binary  */
    public static final int BINARY_PORT = 11212;

    /** Passcode. */
    public static final String PASSCODE = "passcode";

    /** Test authentication spi. */
    private static TestAuthenticationSpi authSpi = new TestAuthenticationSpi();

    /** Test secure session spi. */
    private static TestSecureSessionSpi secureSesSpi = new TestSecureSessionSpi();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GG_JETTY_PORT, Integer.toString(JETTY_PORT));

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();

        System.clearProperty(GG_JETTY_PORT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(null).clearAll();
        grid().cache(CACHE_NAME).clearAll();

        GridClientFactory.stopAll(false);

        authSpi.reset();
        secureSesSpi.reset();
    }

    /**
     * Gets protocol which should be used in client connection.
     *
     * @return Protocol.
     */
    protected abstract GridClientProtocol protocol();

    /**
     * Gets authentication spi call overhead. (In HTTP mode re-authentication may proceed in a fewer count spi calls.)
     *
     * @return Authentication SPI calls overhead.
     */
    protected int authOverhead() {
        return 0;
    }

    /**
     * Gets secure session spi call overhead. (In HTTP mode re-authentication may proceed in a fewer count spi calls.)
     *
     * @return SecureSessionSpi call overhead.
     */
    protected int sesTokOverhead() {
        return 0;
    }

    /**
     * Gets server address to which client should connect.
     *
     * @return Server address in format "host:port".
     */
    protected abstract String serverAddress();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);
        cfg.setRestTcpPort(BINARY_PORT);
        cfg.setRestEnabled(true);

        authSpi.setPasscodes(F.asMap(REMOTE_CLIENT, PASSCODE));

        cfg.setAuthenticationSpi(authSpi);

        cfg.setSecureSessionSpi(secureSesSpi);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration("replicated"),
            cacheConfiguration("partitioned"), cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private GridCacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(cacheName == null || CACHE_NAME.equals(cacheName) ? LOCAL : "replicated".equals(cacheName) ?
            REPLICATED : PARTITIONED);
        cfg.setName(cacheName);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cfg.getCacheMode() == PARTITIONED)
            cfg.setBackups(1);

        return cfg;
    }

    /**
     * @param passcode Passcode for client authentication.
     * @return Client.
     * @throws GridClientException In case of error.
     */
    private GridClient client(@Nullable String passcode) throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setTopologyRefreshFrequency(60000);

        cfg.setCredentials(passcode);

        GridClientDataConfiguration nullCache = new GridClientDataConfiguration();

        GridClientDataConfiguration cache = new GridClientDataConfiguration();

        cache.setName(CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(nullCache, cache));

        cfg.setProtocol(protocol());
        cfg.setServers(Arrays.asList(serverAddress()));

        return GridClientFactory.start(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectAuthentication() throws Exception {
        try (GridClient client = client(PASSCODE)) {
            assertEquals(1 + authOverhead(), authSpi.getCallCount());
            assertEquals(1 + sesTokOverhead(), secureSesSpi.getCallCount());

            assertTrue(client.data(CACHE_NAME).put("1", "2"));
            assertEquals("2", client.data(CACHE_NAME).get("1"));

            assertEquals(1 + authOverhead(), authSpi.getCallCount());
            assertEquals(5 + sesTokOverhead(), secureSesSpi.getCallCount());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWrongPasscode() throws Exception {
        GridClient c = client("WrongPasscode");

        try {
            c.compute().refreshTopology(false, false);

            fail("Exception wasn't thrown.");
        }
        catch (GridClientDisconnectedException e) {
            assertTrue(X.hasCause(e, GridClientAuthenticationException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullPasscode() throws Exception {
        GridClient c = client(null);

        try {
            c.compute().refreshTopology(false, false);

            fail("Exception wasn't thrown.");
        }
        catch (GridClientDisconnectedException e) {
            assertTrue(X.hasCause(e, GridClientAuthenticationException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionExpiration() throws Exception {
        try (GridClient client = client(PASSCODE)) {
            assertEquals(1 + authOverhead(), authSpi.getCallCount());
            assertEquals(1 + sesTokOverhead(), secureSesSpi.getCallCount());

            authSpi.reset();
            secureSesSpi.reset();

            assertTrue(client.data(CACHE_NAME).put("1", "2"));

            assertEquals(2, authSpi.getCallCount());
            assertEquals(2 + sesTokOverhead(), secureSesSpi.getCallCount());
        }
    }

    /**
     * Test authentication spi that counts authentication requests.
     */
    private static class TestAuthenticationSpi extends GridPasscodeAuthenticationSpi {
        /** Call counter. */
        private AtomicInteger callCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public boolean authenticate(GridSecuritySubjectType subjType, byte[] subjId,
            @Nullable Object creds) throws GridSpiException {
            if (subjType == REMOTE_CLIENT)
                callCnt.incrementAndGet();

            return super.authenticate(subjType, subjId, creds);
        }

        /**
         * @return Call count with subject type {@code REMOTE_CLIENT}
         */
        public int getCallCount() {
            return callCnt.get();
        }

        /**
         * Resets call counter to zero.
         */
        public void reset() {
            callCnt.set(0);
        }
    }

    /**
     * Test secure session spi used to emulate session expiration.
     */
    @GridSpiMultipleInstancesSupport(true)
    private static class TestSecureSessionSpi extends GridSpiAdapter implements GridSecureSessionSpi {
        /** Session token. */
        private AtomicReference<byte[]> sesTokRef = new AtomicReference<>();

        /** Call counter. */
        private AtomicInteger callCnt = new AtomicInteger();

        /** Random for token generation. */
        private Random rnd = new Random();

        /** {@inheritDoc} */
        @Override public boolean supported(GridSecuritySubjectType subjType) {
            return subjType == REMOTE_CLIENT;
        }

        /** {@inheritDoc} */
        @Override public byte[] validate(GridSecuritySubjectType subjType, byte[] subjId, @Nullable byte[] tok,
            @Nullable Object params) throws GridSpiException {
            assert subjType == REMOTE_CLIENT;

            callCnt.incrementAndGet();

            if (tok == null) {
                while (true) {
                    byte[] sesTok = sesTokRef.get();

                    if (sesTok != null)
                        return sesTok;
                    else {
                        sesTok = new byte[10];

                        rnd.nextBytes(sesTok);

                        if (sesTokRef.compareAndSet(null, sesTok))
                            return sesTok;
                    }
                }
            }
            else {
                byte[] sesTok = sesTokRef.get();

                return sesTok == null || !Arrays.equals(sesTok, tok) ? null : tok;
            }
        }

        /**
         * @return Generated session token or {@code null} if token was not generated.
         */
        public byte[] getSessionToken() {
            return sesTokRef.get();
        }

        /**
         * @return Total call count to this spi since last reset.
         */
        public int getCallCount() {
            return callCnt.get();
        }

        /**
         * Resets call count and remembered session token.
         */
        public void reset() {
            callCnt.set(0);

            sesTokRef.set(null);
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }
    }
}
