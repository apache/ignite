/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.management.JMException;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.walker.ClientConnectionViewWalker;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedThinClientConfiguration;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.nio.GridNioAsyncNotifyFilter;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.clientTypeLabel;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CLI_TYPES;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CONN_CTX_META_KEY;

/**
 * Client connector processor.
 */
public class ClientListenerProcessor extends GridProcessorAdapter {
    /** */
    public static final String CLI_CONN_VIEW = metricName("client", "connections");

    /** */
    public static final String CLI_CONN_VIEW_DESC = "Client connections";

    /** */
    public static final String METRIC_ACTIVE = "ActiveSessions";

    /** Default client connector configuration. */
    public static final ClientConnectorConfiguration DFLT_CLI_CFG = new ClientConnectorConfigurationEx();

    /** Client listener port. */
    public static final String CLIENT_LISTENER_PORT = "clientListenerPort";

    /** Cancel counter. For testing purposes only. */
    public static final AtomicLong CANCEL_COUNTER = new AtomicLong(0);

    /** Default TCP direct buffer flag. */
    private static final boolean DFLT_TCP_DIRECT_BUF = true;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** TCP Server. */
    private GridNioServer<ClientMessage> srv;

    /** Executor service. */
    private ExecutorService execSvc;

    /** Thin client distributed configuration. */
    private DistributedThinClientConfiguration distrThinCfg;

    /**
     * @param ctx Kernal context.
     */
    public ClientListenerProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        ClientConnectorConfiguration cliConnCfg = prepareConfiguration(cfg);

        if (cliConnCfg != null) {
            try {
                validateConfiguration(cliConnCfg);

                // Resolve host.
                String host = cliConnCfg.getHost();

                if (host == null)
                    host = cfg.getLocalHost();

                InetAddress hostAddr;

                try {
                    hostAddr = U.resolveLocalHost(host);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to resolve client connector host: " + host, e);
                }

                execSvc = ctx.pools().getThinClientExecutorService();

                Exception lastErr = null;

                int portTo = cliConnCfg.getPort() + cliConnCfg.getPortRange();

                if (portTo <= 0) // Handle int overflow.
                    portTo = Integer.MAX_VALUE;

                GridNioFilter[] filters = makeFilters(cliConnCfg);

                long idleTimeout = cliConnCfg.getIdleTimeout();

                int selectorCnt = cliConnCfg.getSelectorCount();

                MetricRegistry mreg = ctx.metric().registry(CLIENT_CONNECTOR_METRICS);

                for (int port = cliConnCfg.getPort(); port <= portTo && port <= 65535; port++) {
                    try {
                        srv = GridNioServer.<ClientMessage>builder()
                            .address(hostAddr)
                            .port(port)
                            .listener(new ClientListenerNioListener(ctx, busyLock, cliConnCfg))
                            .logger(log)
                            .selectorCount(selectorCnt)
                            .igniteInstanceName(ctx.igniteInstanceName())
                            .serverName("client-listener")
                            .tcpNoDelay(cliConnCfg.isTcpNoDelay())
                            .directBuffer(DFLT_TCP_DIRECT_BUF)
                            .byteOrder(ByteOrder.nativeOrder())
                            .socketSendBufferSize(cliConnCfg.getSocketSendBufferSize())
                            .socketReceiveBufferSize(cliConnCfg.getSocketReceiveBufferSize())
                            .filters(filters)
                            .directMode(true)
                            .idleTimeout(idleTimeout > 0 ? idleTimeout : Long.MAX_VALUE)
                            .metricRegistry(mreg)
                            .build();

                        ctx.ports().registerPort(port, IgnitePortProtocol.TCP, getClass());

                        if (log.isInfoEnabled())
                            log.info("Client connector processor has started on TCP port " + port);

                        lastErr = null;

                        ctx.addNodeAttribute(CLIENT_LISTENER_PORT, port);

                        break;
                    }
                    catch (Exception e) {
                        lastErr = e;
                    }
                }

                assert (srv != null && lastErr == null) || (srv == null && lastErr != null);

                if (lastErr != null)
                    throw new IgniteCheckedException("Failed to bind to any [host:port] from the range [" +
                        "host=" + host + ", portFrom=" + cliConnCfg.getPort() + ", portTo=" + portTo +
                        ", lastErr=" + lastErr + ']', lastErr);

                if (!U.IGNITE_MBEANS_DISABLED)
                    registerMBean();

                registerClientMetrics(mreg);

                ctx.systemView().registerView(CLI_CONN_VIEW, CLI_CONN_VIEW_DESC,
                    new ClientConnectionViewWalker(),
                    srv.sessions(),
                    ClientConnectionView::new);

                distrThinCfg = new DistributedThinClientConfiguration(ctx);
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start client connector processor.", e);
            }
        }
    }

    /** @param mreg Metric registry. */
    private void registerClientMetrics(MetricRegistry mreg) {
        for (int i = 0; i < CLI_TYPES.length; i++) {
            byte cliType = CLI_TYPES[i];

            String cliTypeName = clientTypeLabel(cliType);

            mreg.register(
                metricName(cliTypeName, METRIC_ACTIVE),
                () -> {
                    int res = 0;

                    for (GridNioSession ses : srv.sessions()) {
                        ClientListenerConnectionContext ctx = ses.meta(CONN_CTX_META_KEY);

                        if (ctx != null && ctx.clientType() == cliType)
                            ++res;
                    }

                    return res;
                },
                "Number of active sessions for the " + cliTypeName + " client."
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        super.onKernalStart(active);

        if (srv != null)
            srv.start();
    }

    /**
     * Register an Ignite MBean for managing clients connections.
     */
    private void registerMBean() throws IgniteCheckedException {
        assert !U.IGNITE_MBEANS_DISABLED;

        String name = getClass().getSimpleName();

        try {
            ObjectName objName = U.registerMBean(
                ctx.config().getMBeanServer(),
                ctx.config().getIgniteInstanceName(),
                "Clients", name, new ClientProcessorMXBeanImpl(), ClientProcessorMXBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered MBean: " + objName);
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register MBean " + name, e);
        }
    }

    /**
     * Unregisters given MBean.
     */
    private void unregisterMBean() {
        assert !U.IGNITE_MBEANS_DISABLED;

        String name = getClass().getSimpleName();

        try {
            ObjectName objName = U.makeMBeanName(ctx.config().getIgniteInstanceName(), "Clients", name);

            ctx.config().getMBeanServer().unregisterMBean(objName);

            if (log.isDebugEnabled())
                log.debug("Unregistered MBean: " + objName);
        }
        catch (JMException e) {
            U.error(log, "Failed to unregister MBean: " + name, e);
        }
    }

    /**
     * Make NIO server filters.
     *
     * @param cliConnCfg Client configuration.
     * @return Array of filters, suitable for the configuration.
     * @throws IgniteCheckedException if provided SslContextFactory is null.
     */
    @NotNull private GridNioFilter[] makeFilters(@NotNull ClientConnectorConfiguration cliConnCfg)
        throws IgniteCheckedException {
        GridNioFilter openSesFilter = new GridNioAsyncNotifyFilter(ctx.igniteInstanceName(), execSvc, log) {
            @Override public void onSessionOpened(GridNioSession ses)
                throws IgniteCheckedException {
                proceedSessionOpened(ses);
            }

            @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
                ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

                if (connCtx != null && connCtx.parser() != null && connCtx.handler().isCancellationSupported()) {
                    ClientMessage inMsg;

                    int cmdType;

                    long reqId;

                    try {
                        inMsg = (ClientMessage)msg;

                        cmdType = connCtx.parser().decodeCommandType(inMsg);

                        reqId = connCtx.parser().decodeRequestId(inMsg);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to parse client request.", e);

                        ses.close();

                        return;
                    }

                    if (connCtx.handler().isCancellationCommand(cmdType)) {
                        CANCEL_COUNTER.incrementAndGet();

                        proceedMessageReceived(ses, msg);
                    }
                    else {
                        connCtx.handler().registerRequest(reqId, cmdType);

                        super.onMessageReceived(ses, msg);
                    }
                }
                else
                    super.onMessageReceived(ses, msg);
            }
        };

        GridNioFilter codecFilter = new GridNioCodecFilter(new ClientListenerNioMessageParser(log), log, true);

        if (cliConnCfg.isSslEnabled()) {
            Factory<SSLContext> sslCtxFactory = cliConnCfg.isUseIgniteSslContextFactory() ?
                ctx.config().getSslContextFactory() : cliConnCfg.getSslContextFactory();

            if (sslCtxFactory == null)
                throw new IgniteCheckedException("Failed to create client listener " +
                    "(SSL is enabled but factory is null). Check the ClientConnectorConfiguration");

            GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtxFactory.create(),
                true, ByteOrder.nativeOrder(), log, ctx.metric().registry(CLIENT_CONNECTOR_METRICS));

            sslFilter.directMode(true);

            boolean auth = cliConnCfg.isSslClientAuth();

            sslFilter.wantClientAuth(auth);
            sslFilter.needClientAuth(auth);

            return new GridNioFilter[] {
                openSesFilter,
                codecFilter,
                sslFilter
            };
        }
        else {
            return new GridNioFilter[] {
                openSesFilter,
                codecFilter
            };
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (srv != null) {
            busyLock.block();

            srv.stop();

            ctx.ports().deregisterPorts(getClass());

            execSvc = null;

            if (!U.IGNITE_MBEANS_DISABLED)
                unregisterMBean();

            if (log.isDebugEnabled())
                log.debug("Client connector processor stopped.");
        }
    }

    /**
     *
     */
    public void closeAllSessions() {
        Collection<? extends GridNioSession> sessions = srv.sessions();

        for (GridNioSession ses : sessions) {
            ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

            if (connCtx == null || ses.closeTime() != 0)
                continue; // Skip non-initialized or closed session.

            srv.close(ses);

            if (log.isInfoEnabled()) {
                log.info("Client session has been dropped: "
                    + clientConnectionDescription(ses, connCtx));
            }
        }
    }

    /**
     * Compose connection description string.
     * @param ses Client's NIO session.
     * @param ctx Client's connection context.
     * @return connection description.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private static String clientConnectionDescription(
        GridNioSession ses,
        ClientListenerConnectionContext ctx
    ) {
        StringBuilder sb = new StringBuilder();

        if (ctx instanceof JdbcConnectionContext)
            sb.append("JdbcClient [");
        else if (ctx instanceof OdbcConnectionContext)
            sb.append("OdbcClient [");
        else
            sb.append("ThinClient [");

        InetSocketAddress rmtAddr = ses.remoteAddress();
        InetSocketAddress locAddr = ses.localAddress();

        assert rmtAddr != null;
        assert locAddr != null;

        String rmtAddrStr = rmtAddr.getHostString() + ":" + rmtAddr.getPort();
        String locAddrStr = locAddr.getHostString() + ":" + locAddr.getPort();

        String login = ctx.securityContext() == null ? "<anonymous>" : "@" + ctx.securityContext().subject().login();

        sb.append("id=" + ctx.connectionId());
        sb.append(", user=").append(login);
        sb.append(", rmtAddr=" + rmtAddrStr);
        sb.append(", locAddr=" + locAddrStr);

        return sb.append(']').toString();
    }

    /**
     * @return Server port.
     */
    public int port() {
        return srv.port();
    }

    /**
     * Prepare connector configuration.
     *
     * @param cfg Ignite configuration.
     * @return Connector configuration.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    @Nullable private ClientConnectorConfiguration prepareConfiguration(IgniteConfiguration cfg)
        throws IgniteCheckedException {
        OdbcConfiguration odbcCfg = cfg.getOdbcConfiguration();
        SqlConnectorConfiguration sqlConnCfg = cfg.getSqlConnectorConfiguration();
        ClientConnectorConfiguration cliConnCfg = cfg.getClientConnectorConfiguration();

        if (cliConnCfg == null && sqlConnCfg == null && odbcCfg == null)
            return null;

        if (isNotDefault(cliConnCfg)) {
            // User set configuration explicitly. User it, but print a warning about ignored SQL/ODBC configs.
            if (odbcCfg != null) {
                U.warn(log, "Deprecated " + OdbcConfiguration.class.getSimpleName() + " will be ignored because " +
                    ClientConnectorConfiguration.class.getSimpleName() + " is set.");
            }

            if (sqlConnCfg != null) {
                U.warn(log, "Deprecated " + SqlConnectorConfiguration.class.getSimpleName() + " will be ignored " +
                    "because " + ClientConnectorConfiguration.class.getSimpleName() + " is set.");
            }
        }
        else {
            cliConnCfg = new ClientConnectorConfiguration();

            if (sqlConnCfg != null) {
                // Migrate from SQL configuration.
                cliConnCfg.setHost(sqlConnCfg.getHost());
                cliConnCfg.setMaxOpenCursorsPerConnection(sqlConnCfg.getMaxOpenCursorsPerConnection());
                cliConnCfg.setPort(sqlConnCfg.getPort());
                cliConnCfg.setPortRange(sqlConnCfg.getPortRange());
                cliConnCfg.setSocketSendBufferSize(sqlConnCfg.getSocketSendBufferSize());
                cliConnCfg.setSocketReceiveBufferSize(sqlConnCfg.getSocketReceiveBufferSize());
                cliConnCfg.setTcpNoDelay(sqlConnCfg.isTcpNoDelay());
                cliConnCfg.setThreadPoolSize(sqlConnCfg.getThreadPoolSize());

                U.warn(log, "Automatically converted deprecated "
                    + SqlConnectorConfiguration.class.getSimpleName() +
                    " to " + ClientConnectorConfiguration.class.getSimpleName() + ".");

                if (odbcCfg != null) {
                    U.warn(log, "Deprecated " + OdbcConfiguration.class.getSimpleName() +
                        " will be ignored because " +
                        SqlConnectorConfiguration.class.getSimpleName() + " is set.");
                }
            }
            else if (odbcCfg != null) {
                // Migrate from ODBC configuration.
                HostAndPortRange hostAndPort = parseOdbcEndpoint(odbcCfg);

                cliConnCfg.setHost(hostAndPort.host());
                cliConnCfg.setPort(hostAndPort.portFrom());
                cliConnCfg.setPortRange(hostAndPort.portTo() - hostAndPort.portFrom());
                cliConnCfg.setThreadPoolSize(odbcCfg.getThreadPoolSize());
                cliConnCfg.setSocketSendBufferSize(odbcCfg.getSocketSendBufferSize());
                cliConnCfg.setSocketReceiveBufferSize(odbcCfg.getSocketReceiveBufferSize());
                cliConnCfg.setMaxOpenCursorsPerConnection(odbcCfg.getMaxOpenCursors());

                U.warn(log, "Automatically converted deprecated " + OdbcConfiguration.class.getSimpleName() +
                    " to " + ClientConnectorConfiguration.class.getSimpleName() + ".");
            }
        }

        return cliConnCfg;
    }

    /**
     * Validate client connector configuration.
     *
     * @param cfg Configuration.
     * @throws IgniteCheckedException If failed.
     */
    private void validateConfiguration(ClientConnectorConfiguration cfg) throws IgniteCheckedException {
        assertParameter(cfg.getPort() > 1024, "port > 1024");
        assertParameter(cfg.getPort() <= 65535, "port <= 65535");
        assertParameter(cfg.getPortRange() >= 0, "portRange > 0");
        assertParameter(cfg.getSocketSendBufferSize() >= 0, "socketSendBufferSize > 0");
        assertParameter(cfg.getSocketReceiveBufferSize() >= 0, "socketReceiveBufferSize > 0");
        assertParameter(cfg.getMaxOpenCursorsPerConnection() >= 0, "maxOpenCursorsPerConnection() >= 0");
        assertParameter(cfg.getThreadPoolSize() > 0, "threadPoolSize > 0");
    }

    /**
     * Parse ODBC endpoint.
     *
     * @param odbcCfg ODBC configuration.
     * @return ODBC host and port range.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    private HostAndPortRange parseOdbcEndpoint(OdbcConfiguration odbcCfg) throws IgniteCheckedException {
        HostAndPortRange res;

        if (F.isEmpty(odbcCfg.getEndpointAddress())) {
            res = new HostAndPortRange(OdbcConfiguration.DFLT_TCP_HOST,
                OdbcConfiguration.DFLT_TCP_PORT_FROM,
                OdbcConfiguration.DFLT_TCP_PORT_TO
            );
        }
        else {
            res = HostAndPortRange.parse(odbcCfg.getEndpointAddress(),
                OdbcConfiguration.DFLT_TCP_PORT_FROM,
                OdbcConfiguration.DFLT_TCP_PORT_TO,
                "Failed to parse ODBC endpoint address"
            );
        }

        return res;
    }

    /**
     * Check whether configuration is not default.
     *
     * @param cliConnCfg Client connector configuration.
     * @return {@code True} if not default.
     */
    private static boolean isNotDefault(ClientConnectorConfiguration cliConnCfg) {
        return cliConnCfg != null && !(cliConnCfg instanceof ClientConnectorConfigurationEx);
    }

    /**
     * @return If {@code true} sends a server exception stack to the client side.
     */
    public boolean sendServerExceptionStackTraceToClient() {
        Boolean send = distrThinCfg.sendServerExceptionStackTraceToClient();

        return send == null ?
            ctx.config().getClientConnectorConfiguration().getThinClientConfiguration().sendServerExceptionStackTraceToClient() : send;
    }

    /**
     * @return MX bean instance.
     */
    public ClientProcessorMXBean mxBean() {
        return new ClientProcessorMXBeanImpl();
    }

    /**
     * ClientProcessorMXBean interface.
     */
    public class ClientProcessorMXBeanImpl implements ClientProcessorMXBean {
        /** {@inheritDoc} */
        @Override public List<String> getConnections() {
            Collection<? extends GridNioSession> sessions = srv.sessions();

            List<String> res = new ArrayList<>(sessions.size());

            for (GridNioSession ses : sessions) {
                ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

                if (connCtx == null || ses.closeTime() != 0)
                    continue; // Skip non-initialized or closed session.

                String desc = clientConnectionDescription(ses, connCtx);

                res.add(desc);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void dropAllConnections() {
            ctx.security().authorize(null, SecurityPermission.ADMIN_OPS);

            closeAllSessions();
        }

        /** {@inheritDoc} */
        @Override public boolean dropConnection(long id) {
            ctx.security().authorize(null, SecurityPermission.ADMIN_OPS);

            if ((id >> 32) != ctx.discovery().localNode().order())
                return false;

            Collection<? extends GridNioSession> sessions = srv.sessions();

            for (GridNioSession ses : sessions) {
                ClientListenerConnectionContext connCtx = ses.meta(CONN_CTX_META_KEY);

                if (connCtx == null || connCtx.connectionId() != id)
                    continue;

                if (ses.closeTime() != 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("Client session is already closed: " +
                            clientConnectionDescription(ses, connCtx));
                    }

                    return false;
                }

                srv.close(ses);

                if (log.isInfoEnabled()) {
                    log.info("Client session has been dropped: " +
                        clientConnectionDescription(ses, connCtx));
                }

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void showFullStackOnClientSide(boolean show) {
            try {
                distrThinCfg.updateThinClientSendServerStackTraceAsync(show).get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
