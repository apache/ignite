/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.commons.lang.exception.*;
import org.gridgain.client.impl.connection.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.nio.channels.*;
import java.util.*;

import static org.gridgain.client.GridClientTestRestServer.*;

/**
 *
 */
public class GridClientReconnectionSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    private GridClientTestRestServer[] srvs = new GridClientTestRestServer[SERVERS_CNT];

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < srvs.length; i++) {
            GridClientTestRestServer srv = srvs[i];

            if (srv != null)
                srv.stop();

            srvs[i] = null;
        }

        super.afterTest();
    }

    /**
     * @return Client for test.
     * @throws GridClientException In case of error.
     */
    private GridClient client() throws GridClientException {
        return client(HOST);
    }

    /**
     * @param host - server host
     * @return Client for test.
     * @throws GridClientException In case of error.
     */
    private GridClient client(String host) throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setProtocol(GridClientProtocol.TCP);

        Collection<String> addrs = new ArrayList<>();

        for (int port = FIRST_SERVER_PORT; port < FIRST_SERVER_PORT + SERVERS_CNT; port++)
            addrs.add(host + ":" + port);

        cfg.setServers(addrs);

        cfg.setTopologyRefreshFrequency(60 * 60 * 1000);

        return GridClientFactory.start(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoFailedReconnection() throws Exception {
        for (int i = 0; i < SERVERS_CNT; i++)
            runServer(i, false);

        try (GridClient client = client()) { // Here client opens initial connection and fetches topology.
            // Only first server in list should be contacted.
            assertEquals(1, srvs[0].getConnectCount());

            for (int i = 1; i < SERVERS_CNT; i++)
                assertEquals(0, srvs[i].getConnectCount());

            srvs[0].resetCounters();

            int contactedSrv = 0;

            for (int i = 0; i < 100; i++) {
                int failed = contactedSrv;

                srvs[failed].fail();

                // Sometimes session close missing on client side. Retry few times until request succeeds.
                while (true)
                    try {
                        client.compute().refreshTopology(false, false);

                        break;
                    }
                    catch (GridClientConnectionResetException e) {
                        info("Exception caught: " + e);
                    }

                // Check which servers where contacted,
                int connects = 0;

                for (int srv = 0; srv < SERVERS_CNT; srv++) {
                    if (srvs[srv].getSuccessfulConnectCount() > 0) {
                        assertTrue("Failed server was contacted: " + srv, srv != failed);

                        contactedSrv = srv;
                    }

                    connects += srvs[srv].getSuccessfulConnectCount();
                }

                assertEquals(1, connects); // Only one new connection should be opened.

                srvs[failed].repair();

                srvs[contactedSrv].resetCounters(); // It should be the only server with non-0 counters.
            }

        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectInit() throws Exception {
        for (int i = 0; i < SERVERS_CNT; i++)
            runServer(i, i == 0);

        try (GridClient ignored = client()) { // Here client opens initial connection and fetches topology.
            // First and second should be contacted, due to failure in initial request to the first.
            for (int i = 0; i < 2; i++)
                assertEquals("Iteration: " + i, 1, srvs[i].getConnectCount());

            for (int i = 2; i < SERVERS_CNT; i++)
                assertEquals(0, srvs[i].getConnectCount());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedInit() throws Exception {
        for (int i = 0; i < SERVERS_CNT; i++)
            runServer(i, true);

        GridClient c = client();

        try {
            c.compute().execute("fake", "arg");

            fail("Client operation should fail when server resets connections.");
        }
        catch (GridClientDisconnectedException e) {
            assertTrue("Thrown exception doesn't have an expected cause: " + ExceptionUtils.getStackTrace(e),
                X.hasCause(e, GridClientConnectionResetException.class, ClosedChannelException.class));
        }

        for (int i = 0; i < SERVERS_CNT; i++)
            // Connection manager does 3 attempts to get topology before failure.
            assertEquals("Server: " + i, 3, srvs[i].getConnectCount());
    }

    /**
     * @throws Exception If failed.
     */
    // TODO Uncomment when GG-3789 fixed.
//    public void testIdleConnection() throws Exception {
//        for (int i = 0; i < SERVERS_CNT; i++)
//            runServer(i, false);
//
//        GridClient client = client(); // Here client opens initial connection and fetches topology.
//
//        try {
//            // Only first server in list should be contacted.
//            assertEquals(1, srvs[0].getConnectCount());
//
//            Thread.sleep(35000); // Timeout as idle.
//
//            assertEquals(1, srvs[0].getDisconnectCount());
//
//            for (int i = 1; i < SERVERS_CNT; i++)
//                assertEquals(0, srvs[i].getConnectCount());
//
//            srvs[0].resetCounters();
//
//            // On new request connection should be re-opened.
//            client.compute().refreshTopology(false, false);
//
//            assertEquals(1, srvs[0].getConnectCount());
//
//            for (int i = 1; i < SERVERS_CNT; i++)
//                assertEquals(0, srvs[i].getConnectCount());
//        }
//        finally {
//            GridClientFactory.stop(client.id());
//        }
//    }

    /**
     * Runs a new server with given index.
     *
     * @param idx Server index, same as in client configuration's servers property.
     * @param failOnConnect If {@code true} the server should fail incoming connection immediately.
     * @return Server instance.
     * @throws GridException If failed.
     */
    private GridClientTestRestServer runServer(int idx, boolean  failOnConnect) throws GridException {
        GridClientTestRestServer srv = new GridClientTestRestServer(FIRST_SERVER_PORT + idx, failOnConnect, log());

        srv.start();

        srvs[idx] = srv;

        return srv;
    }
}
