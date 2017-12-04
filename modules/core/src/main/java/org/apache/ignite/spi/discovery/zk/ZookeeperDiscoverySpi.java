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

package org.apache.ignite.spi.discovery.zk;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperClusterNode;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 *
 */
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiOrderSupport(true)
@DiscoverySpiHistorySupport(true)
public class ZookeeperDiscoverySpi extends IgniteSpiAdapter implements DiscoverySpi, IgniteDiscoverySpi {
    /** */
    public static final String DFLT_ROOT_PATH = "/apacheIgnite";

    /** */
    @GridToStringInclude
    private String zkConnectionString;

    /** */
    @GridToStringInclude
    private int sesTimeout = 10_000;

    /** */
    @GridToStringInclude
    private String zkRootPath = DFLT_ROOT_PATH;

    /** */
    @GridToStringExclude
    private DiscoverySpiListener lsnr;

    /** */
    @GridToStringExclude
    private DiscoverySpiDataExchange exchange;

    /** */
    @GridToStringExclude
    private DiscoverySpiNodeAuthenticator auth;

    /** */
    @GridToStringExclude
    private DiscoveryMetricsProvider metricsProvider;

    /** */
    @GridToStringExclude
    private ZookeeperDiscoveryImpl impl;

    /** */
    @GridToStringExclude
    private Map<String, Object> locNodeAttrs;

    /** */
    @GridToStringExclude
    private IgniteProductVersion locNodeVer;

    /** */
    @GridToStringExclude
    private Serializable consistentId;

    /** Local node addresses. */
    private IgniteBiTuple<Collection<String>, Collection<String>> addrs;

    /** */
    @LoggerResource
    @GridToStringExclude
    private IgniteLogger log;

    /** */
    private boolean clientReconnectDisabled;

    public String getZkRootPath() {
        return zkRootPath;
    }

    public ZookeeperDiscoverySpi setZkRootPath(String zkRootPath) {
        this.zkRootPath = zkRootPath;

        return this;
    }

    public int getSessionTimeout() {
        return sesTimeout;
    }

    public ZookeeperDiscoverySpi setSessionTimeout(int sesTimeout) {
        this.sesTimeout = sesTimeout;

        return this;
    }

    public String getZkConnectionString() {
        return zkConnectionString;
    }

    public ZookeeperDiscoverySpi setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;

        return this;
    }

    /**
     * If {@code true} client does not try to reconnect.
     *
     * @return Client reconnect disabled flag.
     */
    public boolean isClientReconnectDisabled() {
        return clientReconnectDisabled;
    }

    /**
     * Sets client reconnect disabled flag.
     *
     * @param clientReconnectDisabled Client reconnect disabled flag.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setClientReconnectDisabled(boolean clientReconnectDisabled) {
        this.clientReconnectDisabled = clientReconnectDisabled;
    }

    /** {@inheritDoc} */
    @Override public boolean reconnectSupported() {
        return !clientReconnectDisabled;
    }

    /** {@inheritDoc} */
    @Override public void reconnect() {
        impl.reconnect();
    }

    /** {@inheritDoc} */
    @Override public boolean knownNode(UUID nodeId) {
        return impl.knownNode(nodeId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable consistentId() throws IgniteSpiException {
        if (consistentId == null) {
            consistentId = ignite.configuration().getConsistentId();

            if (consistentId == null) {
                final List<String> sortedAddrs = new ArrayList<>(addrs.get1());

                Collections.sort(sortedAddrs);

                if (getBoolean(IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT))
                    consistentId = U.consistentId(sortedAddrs);
                else {
                    Integer commPort = (Integer)locNodeAttrs.get(
                        TcpCommunicationSpi.class.getSimpleName() + "." + TcpCommunicationSpi.ATTR_PORT);

                    if (commPort == null)
                        throw new IgniteSpiException("Can not initialized consistent ID.");

                    consistentId = U.consistentId(sortedAddrs, (Integer)commPort);
                }
            }
        }

        return consistentId;
    }

    /**
     *
     */
    private void initAddresses() {
        if (addrs == null) {
            String locHost = ignite != null ? ignite.configuration().getLocalHost() : null;

            InetAddress locAddr;

            try {
                locAddr = U.resolveLocalHost(locHost);
            }
            catch (IOException e) {
                throw new IgniteSpiException("Unknown local address: " + locHost, e);
            }

            try {
                addrs = U.resolveLocalAddresses(locAddr);
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to resolve local host to set of external addresses: " + locHost,
                    e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return impl.remoteNodes();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getLocalNode() {
        return impl != null ? impl.localNode() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode getNode(UUID nodeId) {
        return impl.node(nodeId);
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return impl.pingNode(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
        assert locNodeAttrs == null;
        assert locNodeVer == null;

        if (log.isDebugEnabled()) {
            log.debug("Node attributes to set: " + attrs);
            log.debug("Node version to set: " + ver);
        }

        locNodeAttrs = attrs;
        locNodeVer = ver;
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
        this.exchange = exchange;
    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        // TODO ZK
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // TODO ZK
        this.auth = auth;
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        return impl.gridStartTime();
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) {
        IgniteDiscoverySpiInternalListener internalLsnr = impl.internalLsnr;

        if (internalLsnr != null) {
            if (!internalLsnr.beforeSendCustomEvent(this, log, msg))
                return;
        }

        impl.sendCustomMessage(msg);
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        impl.failNode(nodeId, warning);
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        return impl.localNode().isClient();
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        ZookeeperClusterNode locNode = initLocalNode();

        log.info("Start Zookeeper discovery [zkConnectionString=" + zkConnectionString +
            ", sesTimeout=" + sesTimeout +
            ", rootPath=" + zkRootPath + ']');

        impl = new ZookeeperDiscoveryImpl(
            igniteInstanceName,
            zkConnectionString,
            sesTimeout,
            log,
            zkRootPath,
            locNode,
            lsnr,
            exchange,
            locNode.isClient() && !clientReconnectDisabled);

        try {
            impl.joinTopology();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteSpiException("Failed to join cluster, thread was interrupted", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
        impl.internalLsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void simulateNodeFailure() {
        impl.simulateNodeFailure();
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        if (impl != null)
            impl.onStop();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (impl != null) {
            try {
                impl.stop();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteSpiException(e);
            }
        }
    }

    /** {@inheritDoc} */
    public Object clone() {
        ZookeeperDiscoverySpi spi = new ZookeeperDiscoverySpi();

        spi.setZkConnectionString(zkConnectionString);
        spi.setSessionTimeout(sesTimeout);
        spi.setClientReconnectDisabled(clientReconnectDisabled);

        return spi;
    }

    /**
     * @return Local node instance.
     */
    private ZookeeperClusterNode initLocalNode() {
        assert ignite != null;

        initAddresses();

        consistentId = consistentId();

        ZookeeperClusterNode locNode = new ZookeeperClusterNode(
            ignite.configuration().getNodeId(),
            addrs.get1(),
            addrs.get2(),
            locNodeVer,
            locNodeAttrs,
            consistentId,
            ignite.configuration().isClientMode(),
            metricsProvider);

        locNode.local(true);

        DiscoverySpiListener lsnr = this.lsnr;

        if (lsnr != null)
            lsnr.onLocalNodeInitialized(locNode);

        if (log.isDebugEnabled())
            log.debug("Local node initialized: " + locNode);

        if (metricsProvider != null) {
            locNode.setMetrics(metricsProvider.metrics());
            locNode.setCacheMetrics(metricsProvider.cacheMetrics());
        }

        return locNode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZookeeperDiscoverySpi.class, this);
    }
}
