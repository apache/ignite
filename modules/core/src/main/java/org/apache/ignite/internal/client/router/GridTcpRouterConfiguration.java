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

package org.apache.ignite.internal.client.router;

import java.net.Socket;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.security.SecurityCredentialsProvider;
import org.jetbrains.annotations.Nullable;

/**
 * This class defines runtime configuration for TCP router.
 * <p>
 * Note that you should only set values
 * that differ from defaults, as router will automatically pick default values
 * for all values that are not set.
 * <p>
 * For more information about router configuration and startup refer to {@code GridRouterFactory}
 * documentation.
 */
public class GridTcpRouterConfiguration {
    /** Default servers to which router will try to connect. */
    public static final Collection<String> DFLT_SERVERS =
        Collections.singleton("127.0.0.1:" + IgniteConfiguration.DFLT_TCP_PORT);

    /** Default TCP host for router to bind to. */
    public static final String DFLT_TCP_HOST = "0.0.0.0";

    /** Default TCP port. The next port number after Grid's default is used. */
    public static final int DFLT_TCP_PORT = IgniteConfiguration.DFLT_TCP_PORT + 1;

    /** Default port range. */
    public static final int DFLT_PORT_RANGE = 0;

    /** Default nodelay. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Host. */
    private String host = DFLT_TCP_HOST;

    /** Port. */
    private int port = DFLT_TCP_PORT;

    /** Port range. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int portRange = DFLT_PORT_RANGE;

    /** No delay. */
    private boolean noDelay = DFLT_TCP_NODELAY;

    /** Idle timeout. */
    private long idleTimeout = ConnectorConfiguration.DFLT_IDLE_TIMEOUT;

    /** Client auth. */
    private boolean sslClientAuth;

    /** Ssl context factory. */
    private GridSslContextFactory sslCtxFactory;

    /** Collection of servers */
    private Collection<String> srvrs = DFLT_SERVERS;

    /** Logger. */
    private IgniteLogger log;

    /** Credentials. */
    private SecurityCredentialsProvider credsProvider;

    /**
     * Gets TCP host or IP address for router to bind to.
     * <p>
     * If not defined, router will try to bind to all interfaces.
     *
     * @return TCP host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets port for TCP binary protocol server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets port range for TCP binary protocol server. If port number returned from {@link #getPort()}
     * is busy then ports withing this range will be tried.
     * <p>
     * Note: zero-range means only user-specified port will be used.
     * <p>
     * Default is {@link #DFLT_PORT_RANGE}.
     *
     * @return TCP port.
     */
    public int getPortRange() {
        return portRange;
    }

    /**
     * Gets flag indicating whether {@code TCP_NODELAY} option should be set for accepted client connections.
     * Setting this option reduces network latency and should be set to {@code true} in majority of cases.
     * For more information, see {@link Socket#setTcpNoDelay(boolean)}
     * <p/>
     * If not specified, default value is {@code true}.
     *
     * @return Whether {@code TCP_NODELAY} option should be enabled.
     */
    public boolean isNoDelay() {
        return noDelay;
    }

    /**
     * Gets timeout in milliseconds to consider connection idle. If no messages sent by client
     * within this interval router closes idling connection.
     * <p/>
     * If not specified, default value is {@link ConnectorConfiguration#DFLT_IDLE_TIMEOUT}.
     *
     * @return Idle timeout.
     */
    public long getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Gets a flag indicating whether or not remote clients will be required to have
     * a valid SSL certificate which validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     */
    public boolean isSslClientAuth() {
        return sslClientAuth;
    }

    /**
     * Gets SSL context factory that will be used for creating a secure socket layer
     * of both rest binary server and out coming connections.
     *
     * @return SslContextFactory instance.
     * @see GridSslContextFactory
     */
    @Nullable public GridSslContextFactory getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Gets list of server addresses to which router should try to connect to.
     * <p>
     * Node that this list will be used only for initial grid connectivity.
     * Once connected to the grid, router may establish connections to any grid node.
     *
     * @return List of server addresses.
     */
    public Collection<String> getServers() {
        return srvrs;
    }

    /**
     * Gets logger for the router instance.
     * If no logger provided JDK logging will be used by router implementation.
     *
     * @return Logger or {@code null} if no logger provided by configuration.
     */
    public IgniteLogger getLogger() {
        return log;
    }

    /**
     * Gets credentials provider for grid access.
     * <p>
     * This credentials will be used only for initial connection and topology discovery
     * by the router, not for client's request authorization.
     *
     * @return Credentials.
     */
    @Nullable public SecurityCredentialsProvider getSecurityCredentialsProvider() {
        return credsProvider;
    }

    /**
     * Sets host for router.
     *
     * @param host Host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Sets port for router.
     *
     * @param port Port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Sets port range router will be allowed to try.
     * <p>
     * Note: zero-range means only user-specified port will be used.
     *
     * @param portRange Port range.
     * @see #DFLT_PORT_RANGE
     */
    public void setPortRange(int portRange) {
        A.ensure(portRange >= 0, "portRange >= 0");

        this.portRange = portRange;
    }

    /**
     * Sets flag indicating whether {@code TCP_NODELAY} option should be set
     * for accepted client connections.
     *
     * @param noDelay No delay.
     */
    public void setNoDelay(boolean noDelay) {
        this.noDelay = noDelay;
    }

    /**
     * Sets idle timeout.
     *
     * @param idleTimeout Idle timeout in milliseconds.
     */
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Sets flag indicating whether or not remote clients will be required to have
     * a valid SSL certificate which validity will be verified with trust manager.
     *
     * @param sslClientAuth Ssl client auth.
     */
    public void setSslClientAuth(boolean sslClientAuth) {
        this.sslClientAuth = sslClientAuth;
    }

    /**
     * Sets SSL context factory that will be used for creating a secure socket layer
     * of both rest binary server and out coming connections.
     *
     * @param sslCtxFactory Ssl context factory.
     */
    public void setSslContextFactory(GridSslContextFactory sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;
    }

    /**
     * Sets list of server addresses where router's embedded client should connect.
     *
     * @param srvrs List of servers.
     */
    public void setServers(Collection<String> srvrs) {
        this.srvrs = srvrs;
    }

    /**
     * Sets logger for the router instance.
     *
     * @param log Logger.
     */
    public void setLogger(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Sets credentials provider for grid access.
     *
     * @param credsProvider Credentials provider.
     */
    public void setSecurityCredentialsProvider(SecurityCredentialsProvider credsProvider) {
        this.credsProvider = credsProvider;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpRouterConfiguration.class, this);
    }
}