/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

import org.gridgain.client.*;
import org.gridgain.client.impl.*;
import org.gridgain.client.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static java.util.logging.Level.*;
import static org.gridgain.client.impl.connection.GridClientConnectionCloseReason.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridClientPacketType.*;
import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

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

    /** Request header. */
    private static final byte REQ_HEADER = (byte)0x90;

    /** NIO server. */
    private GridNioServer srv;

    /** Active connections. */
    private final ConcurrentMap<InetSocketAddress, GridClientConnection> conns = new ConcurrentHashMap<>();

    /** Active connections of nodes. */
    private final ConcurrentMap<UUID, GridClientConnection> nodeConns = new ConcurrentHashMap<>();

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

    /** Endpoint striped lock. */
    private final GridClientStripedLock endpointStripedLock = new GridClientStripedLock(16);

    /** Custom protocol ID. */
    private final Byte protoId;

    /** Service for ping requests. */
    private final ScheduledExecutorService pingExecutor;

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

        pingExecutor = cfg.getProtocol() == GridClientProtocol.TCP ?
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors()) : null;

        if (cfg.getProtocol() == GridClientProtocol.TCP) {
            try {
                GridLogger gridLog = new GridJavaLogger();

                GridNioFilter[] filters;

                GridNioFilter codecFilter = new GridNioCodecFilter(new NioParser(), gridLog, false);

                if (sslCtx != null) {
                    filters = new GridNioFilter[]{codecFilter};

                    // FIXME: need client SSL filter.
                    //GridNioFilter sslFilter = ;

                    //filters = new GridNioFilter[]{codecFilter, sslFilter};
                }
                else
                    filters = new GridNioFilter[]{codecFilter};

                srv = GridNioServer.builder().address(U.getLocalHost())
                    .port(-1)
                    .listener(new NioListener())
                    .filters(filters)
                    .logger(gridLog)
                    .selectorCount(Runtime.getRuntime().availableProcessors())
                    .sendQueueLimit(0)
                    .byteOrder(ByteOrder.nativeOrder())
                    .tcpNoDelay(cfg.isTcpNoDelay())
                    .directBuffer(true)
                    .idleTimeout(60_000)
                    .gridName("client")
                    .build();

                srv.start();
            }
            catch (IOException | GridException e) {
                throw new RuntimeException("Failed to start connection server.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait", "unchecked"})
    @Override public void init(Collection<InetSocketAddress> srvs) throws GridClientException, InterruptedException {
        GridClientException firstEx = null;

        for (int i = 0; i < INIT_RETRY_CNT; i++) {
            Collection<InetSocketAddress> srvsCp = new ArrayList<>(srvs);

            while (!srvsCp.isEmpty()) {
                GridClientConnection conn = null;

                try {
                    conn = connect(null, srvsCp);

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
            return connection(null, routers);

        GridClientConnection conn = nodeConns.get(node.nodeId());

        if (conn != null) {
            // Ignore closed connections.
            if (conn.closeIfIdle(cfg.getMaxConnectionIdleTime()))
                closeIdle();
            else
                return conn;
        }

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

        return connection(node.nodeId(), srvs);
    }

    /**
     * Returns connection to one of the given addresses.
     *
     * @param nodeId {@code UUID} of node for mapping with connection.
     *      {@code null} if no need of mapping.
     * @param srvs Collection of addresses to connect to.
     * @return Connection to use for operations, targeted for the given node.
     * @throws GridServerUnreachableException If connection can't be established.
     * @throws GridClientClosedException If connections manager has been closed already.
     * @throws InterruptedException If connection was interrupted.
     */
    public GridClientConnection connection(@Nullable UUID nodeId, Collection<InetSocketAddress> srvs)
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

            if (nodeId != null)
                nodeConns.put(nodeId, conn);

            return conn;
        }

        return connect(nodeId, srvs);
    }

    /**
     * Creates a connected facade and returns it. Called either from constructor or inside
     * a write lock.
     *
     * @param nodeId {@code UUID} of node for mapping with connection.
     *      {@code null} if no need of mapping.
     * @param srvs List of server addresses that this method will try to connect to.
     * @return Established connection.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws InterruptedException If connection was interrupted.
     */
    protected GridClientConnection connect(@Nullable UUID nodeId, Collection<InetSocketAddress> srvs) throws GridServerUnreachableException,
        InterruptedException {
        if (srvs.isEmpty())
            throw new GridServerUnreachableException("Failed to establish connection to the grid node (address " +
                "list is empty).");

        Exception cause = null;

        for (InetSocketAddress srv : srvs) {
            try {
                return connect(nodeId, srv);
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
     * @param nodeId {@code UUID} of node for mapping with connection.
     *      {@code null} if no need of mapping.
     * @param addr Remote socket to connect.
     * @return Established connection.
     * @throws IOException If connection failed.
     * @throws GridClientException If protocol error happened.
     * @throws InterruptedException If thread was interrupted before connection was established.
     */
    protected GridClientConnection connect(@Nullable UUID nodeId, InetSocketAddress addr)
        throws IOException, GridClientException, InterruptedException {
        endpointStripedLock.lock(addr);

        try {
            GridClientConnection old = conns.get(addr);

            if (old != null) {
                if (nodeId != null)
                    nodeConns.put(nodeId, old);

                return old;
            }

            GridClientConnection conn;

            switch (cfg.getProtocol()) {
                case TCP: {
                    conn = new GridClientNioTcpConnection(srv, clientId, addr, sslCtx, pingExecutor,
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

            if (nodeId != null)
                nodeConns.put(nodeId, conn);

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

        closeIdle();

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

        nodeConns.clear();

        // Close old connection outside the writer lock.
        for (GridClientConnection conn : closeConns)
            conn.close(CLIENT_CLOSED, waitCompletion);

        if (pingExecutor != null)
            GridClientUtils.shutdownNow(GridClientConnectionManager.class, pingExecutor, log);

        GridClientUtils.shutdownNow(GridClientConnectionManager.class, executor, log);

        if (srv != null)
            srv.stop();
    }

    /**
     * Close all connections idling for more then
     * {@link GridClientConfiguration#getMaxConnectionIdleTime()} milliseconds.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void closeIdle() {
        for (Iterator<Map.Entry<UUID, GridClientConnection>> it = nodeConns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<UUID, GridClientConnection> entry = it.next();

            GridClientConnection conn = entry.getValue();

            if (conn.closeIfIdle(cfg.getMaxConnectionIdleTime())) {
                conns.remove(conn.serverAddress(), conn);

                nodeConns.remove(entry.getKey(), conn);
            }
        }

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

    /**
     *
     */
    private static class NioListener implements GridNioServerListener {
        /** {@inheritDoc} */
        @Override public void onConnected(GridNioSession ses) {
            if (log.isLoggable(Level.FINE))
                log.fine("Session connected: " + ses);
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
            if (log.isLoggable(Level.FINE))
                log.fine("Session disconnected: " + ses);

            GridClientFutureAdapter<Boolean> handshakeFut =
                ses.removeMeta(GridClientNioTcpConnection.SES_META_HANDSHAKE);

            if (handshakeFut != null)
                handshakeFut.onDone(new GridClientConnectionResetException("Failed to perform handshake (connection failed)"));
            else {
                GridClientNioTcpConnection conn = ses.meta(GridClientNioTcpConnection.SES_META_CONN);

                if (conn != null)
                    conn.close(FAILED, false);
            }
        }

        /** {@inheritDoc} */
        @Override public void onMessage(GridNioSession ses, Object msg) {
            GridClientFutureAdapter<Boolean> handshakeFut =
                ses.removeMeta(GridClientNioTcpConnection.SES_META_HANDSHAKE);

            if (handshakeFut != null) {
                assert msg instanceof GridClientHandshakeResponse;

                handleHandshakeResponse(handshakeFut, (GridClientHandshakeResponse)msg);
            }
            else {
                GridClientNioTcpConnection conn = ses.meta(GridClientNioTcpConnection.SES_META_CONN);

                assert conn != null;

                if (msg instanceof GridClientRequestData) {
                    GridClientRequestData reqData = (GridClientRequestData)msg;

                    assert reqData.body() != null;

                    conn.handleResponse((GridClientRequestData)msg);
                }
                else {
                    assert msg instanceof GridClientPingPacket : msg;

                    conn.handlePingResponse();
                }
            }
        }

        /**
         * Handles client handshake response.
         *
         * @param handshakeFut Future.
         * @param msg A handshake response.
         */
        private void handleHandshakeResponse(GridClientFutureAdapter<Boolean> handshakeFut,
            GridClientHandshakeResponse msg) {
            byte rc = msg.resultCode();

            if (rc != GridClientHandshakeResponse.OK.resultCode()) {
                if (rc == GridClientHandshakeResponse.ERR_UNKNOWN_PROTO_ID.resultCode())
                    handshakeFut.onDone(new GridClientHandshakeException(rc, "Unknown/unsupported protocol ID."));
                else
                    handshakeFut.onDone(new GridClientHandshakeException(rc,
                        "Handshake failed due to internal error (see server log for more details)."));
            }
            else
                handshakeFut.onDone(true);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) {
            log.warning("Closing NIO session because of write timeout.");

            ses.close();
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) {
            log.warning("Closing NIO session because of idle timeout.");

            ses.close();
        }
    }

    /**
     *
     */
    private class NioParser implements GridNioParser {
        /** {@inheritDoc} */
        @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, GridException {
            if (msg instanceof GridClientPingPacket)
                return ByteBuffer.wrap(GridClientPingPacket.PING_PACKET);
            else if (msg instanceof GridClientHandshakeRequest)
                return ByteBuffer.wrap(((GridClientHandshakeRequest)msg).rawBytes());

            GridClientMessage clientMsg = (GridClientMessage)msg;

            byte[] msgBytes = clientMsg instanceof GridRouterRequest ? ((GridRouterRequest)clientMsg).body() :
                cfg.getMarshaller().marshal(clientMsg);

            ByteBuffer res = ByteBuffer.allocate(msgBytes.length + 45);

            res.put(REQ_HEADER);

            res.put(U.intToBytes(msgBytes.length + 40));

            res.put(U.longToBytes(clientMsg.requestId()));
            res.put(U.uuidToBytes(clientMsg.clientId()));

            if (clientMsg.destinationId() != null) {
                res.put(U.longToBytes(clientMsg.destinationId().getMostSignificantBits()));
                res.put(U.longToBytes(clientMsg.destinationId().getLeastSignificantBits()));
            }
            else {
                res.put(U.longToBytes(0));
                res.put(U.longToBytes(0));
            }

            res.put(msgBytes);

            res.flip();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, GridException {
            GridClientFutureAdapter<?> handshakeFut = ses.meta(GridClientNioTcpConnection.SES_META_HANDSHAKE);

            if (handshakeFut != null) {
                byte code = buf.get();

                return new GridClientHandshakeResponse(code);
            }

            ParserState state = ses.removeMeta(PARSER_STATE.ordinal());

            if (state == null)
                state = new ParserState();

            if (state.packetType() == null) {
                byte hdr = buf.get();

                if (hdr != REQ_HEADER)
                    throw new GridException("Unexpected message header: " + Integer.toHexString(hdr));

                state.packetType(GRIDGAIN);
            }

            Object msg = parseCustomPacket(ses, buf, state);

            if (msg == null)
                // Packet was not fully parsed yet.
                ses.addMeta(PARSER_STATE.ordinal(), state);

            return msg;
        }

        /**
         * Parses custom packet serialized by GridGain marshaller.
         *
         * @param ses Session.
         * @param buf Buffer containing not parsed bytes.
         * @param state Parser state.
         * @return Parsed message.
         * @throws IOException If packet parsing or deserialization failed.
         */
        @Nullable private Object parseCustomPacket(GridNioSession ses, ByteBuffer buf, ParserState state)
            throws IOException {
            ByteArrayOutputStream tmp = state.buffer();

            int len = state.index();

            if (buf.remaining() > 0) {
                if (len == 0) { // Don't know the size yet.
                    byte[] lenBytes = statefulRead(buf, tmp, 4);

                    if (lenBytes != null) {
                        len = U.bytesToInt(lenBytes, 0);

                        if (len == 0)
                            return GridClientPingPacket.PING_MESSAGE;
                        else if (len < 0)
                            throw new IOException("Failed to parse incoming packet (invalid packet length) " +
                                "[ses=" + ses + ", len=" + len + ']');

                        state.index(len);
                    }
                }

                if (len > 0 && state.requestData() == null) {
                    byte[] hdrBytes = statefulRead(buf, tmp, 40);

                    if (hdrBytes != null) {
                        long reqId = GridClientByteUtils.bytesToLong(hdrBytes, 0);
                        UUID clientId = GridClientByteUtils.bytesToUuid(hdrBytes, 8);
                        UUID destId = GridClientByteUtils.bytesToUuid(hdrBytes, 24);

                        state.requestData(new GridClientRequestData(reqId, clientId, destId));
                    }
                }

                if (len > 0 && state.requestData() != null) {
                    final int packetSize = len - 40;

                    if (tmp.size() + buf.remaining() >= packetSize) {
                        if (buf.remaining() > 0) {
                            byte[] bodyBytes = new byte[packetSize - tmp.size()];

                            buf.get(bodyBytes);

                            tmp.write(bodyBytes);
                        }

                        GridClientRequestData reqData = state.requestData();

                        reqData.body(state.buffer().toByteArray());

                        return reqData;
                    }
                    else
                        copyRemaining(buf, tmp);
                }
            }

            return null;
        }

        /**
         * Tries to read the specified amount of bytes using intermediate buffer. Stores
         * the bytes to intermediate buffer, if size requirement is not met.
         *
         * @param buf Byte buffer to read from.
         * @param intBuf Intermediate buffer to read bytes from and to save remaining bytes to.
         * @param size Number of bytes to read.
         * @return Resulting byte array or {@code null}, if both buffers contain less bytes
         *         than required. In case of non-null result, the intermediate buffer is empty.
         *         In case of {@code null} result, the input buffer is empty (read fully).
         * @throws IOException If IO error occurs.
         */
        @Nullable private byte[] statefulRead(ByteBuffer buf, ByteArrayOutputStream intBuf, int size)
            throws IOException {
            if (intBuf.size() + buf.remaining() >= size) {
                int off = 0;
                byte[] bytes = new byte[size];

                if (intBuf.size() > 0) {
                    assert intBuf.size() < size;

                    byte[] tmpBytes = intBuf.toByteArray();

                    System.arraycopy(tmpBytes, 0, bytes, 0, tmpBytes.length);

                    off = intBuf.size();

                    intBuf.reset();
                }

                buf.get(bytes, off, size - off);

                return bytes;
            }
            else {
                copyRemaining(buf, intBuf);

                return null;
            }
        }

        /**
         * Copies remaining bytes from byte buffer to output stream.
         *
         * @param src Source buffer.
         * @param dest Destination stream.
         * @throws IOException If IO error occurs.
         */
        private void copyRemaining(ByteBuffer src, OutputStream dest) throws IOException {
            byte[] b = new byte[src.remaining()];

            src.get(b);

            dest.write(b);
        }
    }

    /**
     * Holder for parser state and temporary buffer.
     */
    private static class ParserState {
        /** Parser index. */
        private int idx;

        /** Temporary data buffer. */
        private ByteArrayOutputStream buf = new ByteArrayOutputStream();

        /** Packet type. */
        private GridClientPacketType packetType;

        /** Request data. */
        private GridClientRequestData reqData;

        /**
         * @return Stored parser index.
         */
        public int index() {
            return idx;
        }

        /**
         * @param idx Index to store.
         */
        public void index(int idx) {
            this.idx = idx;
        }

        /**
         * @return Temporary data buffer.
         */
        public ByteArrayOutputStream buffer() {
            return buf;
        }

        /**
         * @return Pending packet type.
         */
        public GridClientPacketType packetType() {
            return packetType;
        }

        /**
         * @param packetType Pending packet type.
         */
        public void packetType(GridClientPacketType packetType) {
            this.packetType = packetType;
        }

        /**
         * @return Request data.
         */
        public GridClientRequestData requestData() {
            return reqData;
        }

        /**
         * @param reqData Request data.
         */
        public void requestData(GridClientRequestData reqData) {
            this.reqData = reqData;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ParserState.class, this);
        }
    }
}
