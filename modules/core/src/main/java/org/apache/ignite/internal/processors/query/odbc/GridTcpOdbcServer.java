package org.apache.ignite.internal.processors.query.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.jetbrains.annotations.Nullable;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * TCP server that handles communication with ODBC driver.
 */
public class GridTcpOdbcServer {

    /** Server. */
    private GridNioServer<byte[]> srv;

    /** NIO server listener. */
    private GridNioServerListener<byte[]> lsnr;

    /** Logger. */
    protected final IgniteLogger log;

    /** Context. */
    protected final GridKernalContext ctx;

    /** Host used by this protocol. */
    protected InetAddress host;

    /** Port used by this protocol. */
    protected int port;

    /** */
    public String name() {
        return "ODBC server";
    }

    public GridTcpOdbcServer(GridKernalContext ctx) {
        assert ctx != null;
        assert ctx.config().getConnectorConfiguration() != null;

        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    @SuppressWarnings("BusyWait")
    public void start() throws IgniteCheckedException {
        OdbcConfiguration cfg = ctx.config().getOdbcConfiguration();

        assert cfg != null;

        lsnr = new GridTcpOdbcNioListener(log, this, ctx);

        GridNioParser parser = new GridNioParser() {
            @Nullable
            @Override
            public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                return bytes;
            }

            @Override
            public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
                return null;
            }
        };

        try {
            host = resolveOdbcTcpHost(ctx.config());

            SSLContext sslCtx = null;

            if (cfg.isSslEnabled()) {
                Factory<SSLContext> igniteFactory = ctx.config().getSslContextFactory();

                Factory<SSLContext> factory = cfg.getSslFactory();

                if (factory == null && igniteFactory == null)
                    // Thrown SSL exception instead of IgniteCheckedException for writing correct warning message into log.
                    throw new SSLException("SSL is enabled, but SSL context factory is not specified.");

                if (factory != null)
                    sslCtx = factory.create();
                else
                    sslCtx = igniteFactory.create();
            }

            int odbcPort = cfg.getPort();

            if (startTcpServer(host, odbcPort, lsnr, parser, sslCtx, cfg)) {
                port = odbcPort;

                System.out.println("ODBC Server has started on TCP port " + port);

                return;
            }

            U.warn(log, "Failed to start " + name() + " (possibly all ports in range are in use) " +
                    "[odbcPort=" + odbcPort + ", host=" + host + ']');
        }
        catch (SSLException e) {
            U.warn(log, "Failed to start " + name() + " on port " + port + ": " + e.getMessage(),
                    "Failed to start " + name() + " on port " + port + ". Check if SSL context factory is " +
                            "properly configured.");
        }
        catch (IOException e) {
            U.warn(log, "Failed to start " + name() + " on port " + port + ": " + e.getMessage(),
                    "Failed to start " + name() + " on port " + port + ". " +
                            "Check restTcpHost configuration property.");
        }
    }

    /** */
    public void onKernalStart() {
    }

    /** */
    public void stop() {
        if (srv != null) {
            ctx.ports().deregisterPorts(getClass());

            srv.stop();
        }
    }

    /**
     * Resolves host for server using grid configuration.
     *
     * @param cfg Grid configuration.
     * @return Host address.
     * @throws IOException If failed to resolve host.
     */
    private InetAddress resolveOdbcTcpHost(IgniteConfiguration cfg) throws IOException {
        String host = cfg.getConnectorConfiguration().getHost();

        if (host == null)
            host = cfg.getLocalHost();

        return U.resolveLocalHost(host);
    }

    /**
     * Tries to start server with given parameters.
     *
     * @param hostAddr Host on which server should be bound.
     * @param port Port on which server should be bound.
     * @param lsnr Server message listener.
     * @param parser Server message parser.
     * @param cfg Configuration for other parameters.
     * @return {@code True} if server successfully started, {@code false} if port is used and
     *      server was unable to start.
     */
    private boolean startTcpServer(InetAddress hostAddr, int port, GridNioServerListener<byte[]> lsnr,
                                   GridNioParser parser, @Nullable SSLContext sslCtx, OdbcConfiguration cfg) {
        try {
            GridNioFilter codec = new GridNioCodecFilter(parser, log, false);

            GridNioFilter[] filters;

            if (sslCtx != null) {
                GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx,
                        cfg.isDirectBuffer(), ByteOrder.nativeOrder(), log);

                sslFilter.directMode(false);

                boolean auth = cfg.isSslClientAuth();

                sslFilter.wantClientAuth(auth);

                sslFilter.needClientAuth(auth);

                filters = new GridNioFilter[] {
                        codec,
                        sslFilter
                };
            }
            else
                filters = new GridNioFilter[] { codec };

            srv = GridNioServer.<byte[]>builder()
                    .address(hostAddr)
                    .port(port)
                    .listener(lsnr)
                    .logger(log)
                    .selectorCount(cfg.getSelectorCount())
                    .gridName(ctx.gridName())
                    .tcpNoDelay(cfg.isNoDelay())
                    .directBuffer(cfg.isDirectBuffer())
                    .byteOrder(ByteOrder.nativeOrder())
                    .socketSendBufferSize(cfg.getSendBufferSize())
                    .socketReceiveBufferSize(cfg.getReceiveBufferSize())
                    .sendQueueLimit(cfg.getSendQueueLimit())
                    .filters(filters)
                    .directMode(false)
                    .build();

            srv.idleTimeout(cfg.getIdleTimeout());

            srv.start();

            ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

            return true;
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to start " + name() + " on port " + port + ": " + e.getMessage());

            return false;
        }
    }
}
