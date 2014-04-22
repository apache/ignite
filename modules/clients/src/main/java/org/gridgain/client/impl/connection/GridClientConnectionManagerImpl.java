/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

import io.netty.channel.nio.*;
import org.gridgain.client.*;
import org.gridgain.client.util.*;
import io.netty.channel.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static java.util.logging.Level.*;
import static org.gridgain.client.impl.connection.GridClientConnectionCloseReason.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Cached connections manager.
 */
public class GridClientConnectionManagerImpl implements GridClientConnectionManager {
    /** Class logger. */
    private static final Logger log = Logger.getLogger(GridClientConnectionManagerImpl.class.getName());

    /** Count of reconnect retries before init considered failed. */
    private static final int INIT_RETRY_CNT = 3;

    /** Initialization retry interval. */
    private static final int INIT_RETRY_INTERVAL = 1000;

    /** Active connections. */
    private final ConcurrentMap<InetSocketAddress, GridClientConnection> conns = new ConcurrentHashMap<>();

    /** SSL context. */
    private final SSLContext sslCtx;

    /** Client configuration. */
    private final GridClientConfiguration cfg;

    /** Topology. */
    private final GridClientTopology top;

    /** Client id. */
    private final UUID clientId;

    /** Router endpoints to use instead of topology info. */
    private final Collection<InetSocketAddress> routers;

    /** Closed flag. */
    private volatile boolean closed;

    /** Shared executor service. */
    private final ExecutorService executor;

    /**
     * Netty executor common for all connections.
     * Initialized to {@code null} if HTTP protocol is used.
     */
    protected final EventLoopGroup evtLoop;

    /** Endpoint striped lock. */
    private final GridClientStripedLock endpointStripedLock = new GridClientStripedLock(16);

    /** Custom protocol ID. */
    private final Byte protoId;

    /**
     * Constructs connection manager.
     *
     * @param clientId Client ID.
     * @param sslCtx SSL context to enable secured connection or {@code null} to use unsecured one.
     * @param cfg Client configuration.
     * @param routers Routers or empty collection to use endpoints from topology info.
     * @param top Topology.
     * @param protoId Custom protocol ID (optional).
     */
    public GridClientConnectionManagerImpl(UUID clientId, SSLContext sslCtx, GridClientConfiguration cfg,
        Collection<InetSocketAddress> routers, GridClientTopology top, Byte protoId) {
        assert clientId != null : "clientId != null";
        assert cfg != null : "cfg != null";
        assert routers != null : "routers != null";
        assert top != null : "top != null";

        this.clientId = clientId;
        this.sslCtx = sslCtx;
        this.cfg = cfg;
        this.routers = new ArrayList<>(routers);
        this.top = top;
        this.protoId = protoId;

        executor = cfg.getExecutorService() != null ? cfg.getExecutorService() : Executors.newCachedThreadPool();

        int workerCnt = Runtime.getRuntime().availableProcessors();

        evtLoop = cfg.getProtocol() == GridClientProtocol.TCP ? new NioEventLoopGroup(workerCnt) : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void init(Collection<InetSocketAddress> srvs) throws GridClientException, InterruptedException {
        GridClientException firstEx = null;

        for (int i = 0; i < INIT_RETRY_CNT; i++) {
            Collection<InetSocketAddress> srvsCp = new ArrayList<>(srvs);

            while (!srvsCp.isEmpty()) {
                GridClientConnection conn = null;

                try {
                    conn = connect(srvsCp);

                    conn.topology(cfg.isAutoFetchAttributes(), cfg.isAutoFetchMetrics(), null).get();

                    return;
                }
                catch (GridServerUnreachableException e) {
                    // No connection could be opened to any of initial addresses - exit to retry loop.
                    assert conn == null :
                        "GridClientConnectionResetException was thrown from GridClientConnection#topology";

                    if (firstEx == null)
                        firstEx = e;

                    break;
                }
                catch (GridClientConnectionResetException e) {
                    // Connection was established but topology update failed -
                    // trying other initial addresses if any.
                    assert conn != null : "GridClientConnectionResetException was thrown from connect()";

                    if (firstEx == null)
                        firstEx = e;

                    if (!srvsCp.remove(conn.serverAddress()))
                        // We have misbehaving collection or equals - just exit to avoid infinite loop.
                        break;
                }
            }

            Thread.sleep(INIT_RETRY_INTERVAL);
        }

        for (GridClientConnection c : conns.values()) {
            conns.remove(c.serverAddress(), c);

            c.close(FAILED, false);
        }

        throw firstEx;
    }

    /**
     * Gets active communication facade.
     *
     * @param node Remote node to which connection should be established.
     * @throws GridServerUnreachableException If none of the servers can be reached after the exception.
     * @throws GridClientClosedException If client was closed manually.
     * @throws InterruptedException If connection was interrupted.
     */
    @Override public GridClientConnection connection(GridClientNode node)
        throws GridClientClosedException, GridServerUnreachableException, InterruptedException {
        assert node != null;

        // Use router's connections if defined.
        if (!routers.isEmpty())
            return connection(routers);

        // Use node's connection, if node is available over rest.
        Collection<InetSocketAddress> endpoints = node.availableAddresses(cfg.getProtocol());

        if (endpoints.isEmpty()) {
            throw new GridServerUnreachableException("No available endpoints to connect " +
                "(is rest enabled for this node?): " + node);
        }

        List<InetSocketAddress> srvs = new ArrayList<>(endpoints.size());

        boolean sameHost = node.attributes().isEmpty() ||
            F.containsAny(U.allLocalMACs(), node.attribute(ATTR_MACS).toString().split(", "));

        if (sameHost) {
            srvs.addAll(endpoints);

            Collections.sort(srvs, GridClientUtils.inetSocketAddressesComparator(true));
        }
        else {
            for (InetSocketAddress endpoint : endpoints)
                if (!endpoint.getAddress().isLoopbackAddress())
                    srvs.add(endpoint);
        }

        return connection(srvs);
    }

    /**
     * Returns connection to one of the given addresses.
     *
     * @param srvs Collection of addresses to connect to.
     * @return Connection to use for operations, targeted for the given node.
     * @throws GridServerUnreachableException If connection can't be established.
     * @throws GridClientClosedException If connections manager has been closed already.
     * @throws InterruptedException If connection was interrupted.
     */
    public GridClientConnection connection(Collection<InetSocketAddress> srvs)
        throws GridServerUnreachableException, GridClientClosedException, InterruptedException {
        if (srvs == null || srvs.isEmpty())
            throw new GridServerUnreachableException("Failed to establish connection to the grid" +
                " (address list is empty).");

        checkClosed();

        // Search for existent connection.
        for (InetSocketAddress endPoint : srvs) {
            assert endPoint != null;

            GridClientConnection conn = conns.get(endPoint);

            if (conn == null)
                continue;

            // Ignore closed connections.
            if (conn.closeIfIdle(cfg.getMaxConnectionIdleTime())) {
                closeIdle();

                continue;
            }

            return conn;
        }

        return connect(srvs);
    }

    /**
     * Creates a connected facade and returns it. Called either from constructor or inside
     * a write lock.
     *
     * @param srvs List of server addresses that this method will try to connect to.
     * @return Established connection.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws InterruptedException If connection was interrupted.
     */
    protected GridClientConnection connect(Collection<InetSocketAddress> srvs) throws GridServerUnreachableException,
        InterruptedException {
        if (srvs.isEmpty())
            throw new GridServerUnreachableException("Failed to establish connection to the grid node (address " +
                "list is empty).");

        Exception cause = null;

        for (InetSocketAddress srv : srvs) {
            try {
                return connect(srv);
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                if (cause == null)
                    cause = e;
                else if (log.isLoggable(INFO))
                    log.info("Unable to connect to grid node [srvAddr=" + srv + ", msg=" + e.getMessage() + ']');
            }
        }

        assert cause != null;

        throw new GridServerUnreachableException("Failed to connect to any of the servers in list: " + srvs, cause);
    }

    /**
     * Create new connection to specified server.
     *
     * @param addr Remote socket to connect.
     * @return Established connection.
     * @throws IOException If connection failed.
     * @throws GridClientException If protocol error happened.
     * @throws InterruptedException If thread was interrupted before connection was established.
     */
    protected GridClientConnection connect(InetSocketAddress addr)
        throws IOException, GridClientException, InterruptedException {

        endpointStripedLock.lock(addr);

        try {
            GridClientConnection old = conns.get(addr);

            if (old != null)
                return old;

            GridClientConnection conn;

            switch (cfg.getProtocol()) {
                case TCP: {
                    conn = new GridClientTcpConnection(clientId, addr, sslCtx, evtLoop,
                        cfg.getConnectTimeout(), cfg.getPingInterval(), cfg.getPingTimeout(),
                        cfg.isTcpNoDelay(), protoId == null ? cfg.getMarshaller() : null,
                        top, cfg.getCredentials(), protoId);

                    break;
                }

                case HTTP: {
                    conn = new GridClientHttpConnection(clientId, addr, sslCtx,
                        // Applying max idle time as read timeout for HTTP connections.
                        cfg.getConnectTimeout(), (int)cfg.getMaxConnectionIdleTime(), top,
                        executor == null ? cfg.getExecutorService() : executor, cfg.getCredentials());

                    break;
                }

                default: {
                    throw new GridServerUnreachableException("Failed to create client (protocol is not supported): " +
                        cfg.getProtocol());
                }
            }

            old = conns.putIfAbsent(addr, conn);

            assert old == null;

            return conn;
        }
        finally {
            endpointStripedLock.unlock(addr);
        }
    }

    /** {@inheritDoc} */
    @Override public void terminateConnection(GridClientConnection conn, GridClientNode node, Throwable e) {
        if (log.isLoggable(Level.FINE))
            log.fine("Connection with remote node was terminated [node=" + node + ", srvAddr=" +
                conn.serverAddress() + ", errMsg=" + e.getMessage() + ']');

        conns.remove(conn.serverAddress(), conn);

        conn.close(FAILED, false);
    }

    /**
     * Closes all opened connections.
     *
     * @param waitCompletion If {@code true} waits for all pending requests to be proceeded.
     */
    @SuppressWarnings("TooBroadScope")
    @Override public void stop(boolean waitCompletion) {
        Collection<GridClientConnection> closeConns;

        if (closed)
            return;

        // Mark manager as closed.
        closed = true;

        // Remove all connections from cache.
        closeConns = new ArrayList<>(conns.values());

        conns.clear();

        // Close old connection outside the writer lock.
        for (GridClientConnection conn : closeConns)
            conn.close(CLIENT_CLOSED, waitCompletion);

        if (evtLoop != null)
            evtLoop.shutdownGracefully();
        else if (executor != null)
            // If we are in HTTP mode shutdown explicitly.
            GridClientUtils.shutdownNow(GridClientConnectionManager.class, executor, log);
    }

    /**
     * Close all connections idling for more then
     * {@link GridClientConfiguration#getMaxConnectionIdleTime()} milliseconds.
     */
    private void closeIdle() {
        for (GridClientConnection conn : conns.values())
            if (conn.closeIfIdle(cfg.getMaxConnectionIdleTime()))
                conns.remove(conn.serverAddress(), conn);
    }

    /**
     * Checks and throws an exception if this client was closed.
     *
     * @throws GridClientClosedException If client was closed.
     */
    private void checkClosed() throws GridClientClosedException {
        if (closed)
            throw new GridClientClosedException("Client was closed (no public methods of client can be used anymore).");
    }
}
