/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.isolated;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoveryNotification;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.security.SecurityUtils.authenticateLocalNode;
import static org.apache.ignite.internal.processors.security.SecurityUtils.withSecurityContext;

/**
 * Special discovery SPI implementation to start a single-node cluster in "isolated" mode.
 *
 * When used, node doesn't try to seek or communicate to other nodes that may be up and running even in the same JVM.
 *
 * At the same time all functions like sending discovery messages are functional with only note that
 * no messages are sent to network but are processed by local node immediately when they are created.
 */
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiHistorySupport(true)
@DiscoverySpiOrderSupport(true)
public class IsolatedDiscoverySpi extends IgniteSpiAdapter implements IgniteDiscoverySpi {
    /** */
    private Serializable consistentId;

    /** */
    private final long startTime = System.currentTimeMillis();

    /** */
    private IsolatedNode locNode;

    /** */
    private DiscoverySpiListener lsnr;

    /** */
    private ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /** */
    private DiscoverySpiNodeAuthenticator nodeAuth;

    /** */
    private Marshaller marsh;

    /** {@inheritDoc} */
    @Override public Serializable consistentId() throws IgniteSpiException {
        if (consistentId == null) {
            IgniteConfiguration cfg = ignite.configuration();

            final Serializable cfgId = cfg.getConsistentId();

            consistentId = cfgId != null ? cfgId : UUID.randomUUID();
        }

        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return emptyList();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getNode(UUID nodeId) {
        return locNode.id().equals(nodeId) ? locNode : null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return locNode.id().equals(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
        locNode = new IsolatedNode(ignite.configuration().getNodeId(), attrs, ver);
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        nodeAuth = auth;
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoveryCustomMessage msg) throws IgniteException {
        exec.execute(() -> {
            IgniteFuture<?> fut = lsnr.onDiscovery(new DiscoveryNotification(
                EVT_DISCOVERY_CUSTOM_EVT,
                1,
                locNode,
                singleton(locNode),
                null,
                msg,
                null));

            // Acknowledge message must be send after initial message processed.
            fut.listen((f) -> {
                DiscoveryCustomMessage ack = msg.ackMessage();

                if (ack != null) {
                    exec.execute(() -> lsnr.onDiscovery(new DiscoveryNotification(
                        EVT_DISCOVERY_CUSTOM_EVT,
                        1,
                        locNode,
                        singleton(locNode),
                        null,
                        ack,
                        null))
                    );
                }
            });
        });
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        if (nodeAuth != null) {
            try {
                SecurityCredentials locSecCred = (SecurityCredentials)locNode.attributes().get(ATTR_SECURITY_CREDENTIALS);

                Map<String, Object> attrs = withSecurityContext(
                    authenticateLocalNode(locNode, locSecCred, nodeAuth), locNode.attributes(), marsh);

                attrs.remove(ATTR_SECURITY_CREDENTIALS);

                locNode.setAttributes(attrs);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSpiException("Failed to authenticate local node (will shutdown local node).", e);
            }
        }

        exec.execute(() -> {
            lsnr.onLocalNodeInitialized(locNode);

            lsnr.onDiscovery(new DiscoveryNotification(
                EVT_NODE_JOINED,
                1,
                locNode,
                singleton(locNode))
            );
        });
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        exec.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(final IgniteSpiContext spiCtx) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean knownNode(UUID nodeId) {
        return getNode(nodeId) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean clientReconnectSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void clientReconnect() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void injectResources(Ignite ignite) {
        if (ignite instanceof IgniteKernal)
            marsh = ((IgniteEx)ignite).context().marshallerContext().jdkMarshaller();

        super.injectResources(ignite);
    }

    /** {@inheritDoc} */
    @Override public void simulateNodeFailure() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean supportsCommunicationFailureResolve() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void resolveCommunicationFailure(ClusterNode node, Exception err) {
        // No-op.
    }
}
