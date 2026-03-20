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

package org.apache.ignite.spi.discovery;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Discovery data exchange test.
 */
public class DiscoverySpiDataExchangeTest extends GridCommonAbstractTest {
    /** Closure to collect discovery data bags when node is joined. */
    private BiConsumer<ClusterNode, DiscoveryDataBag> dataExchangeCollectClosure;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        IgniteConfiguration cfg0 = super.optimize(cfg);

        cfg0.setDiscoverySpi(new DelegatedDiscoverySpi((IgniteDiscoverySpi)cfg.getDiscoverySpi()));

        return cfg0;
    }

    /**
     * Ensures that {@link DiscoveryDataBag#isJoiningNodeClient()} returns a valid value when joining a node.
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testJoiningNodeClientFlag() throws Exception {
        for (int nodeIdx = 0; nodeIdx < 4; nodeIdx++) {
            Collection<T2<ClusterNode, DiscoveryDataBag>> dataBags = new ConcurrentLinkedQueue<>();

            dataExchangeCollectClosure = (locNode, dataBag) -> dataBags.add(new T2<>(locNode, dataBag));

            IgniteEx node = nodeIdx % 2 == 0 ? startGrid(nodeIdx) : startClientGrid(nodeIdx);

            assertFalse(dataBags.isEmpty());

            assertTrue(dataBags.toString(),
                dataBags.stream().allMatch(pair -> node.context().clientNode() == pair.get2().isJoiningNodeClient()));
        }
    }

    /** Delegated discovery. */
    @IgniteSpiMultipleInstancesSupport(true)
    @DiscoverySpiHistorySupport(true)
    private class DelegatedDiscoverySpi extends IgniteSpiAdapter implements IgniteDiscoverySpi {
        /** Discovery delegate. */
        private final IgniteDiscoverySpi delegate;

        /**
         * @param delegate Discovery delegate.
         */
        DelegatedDiscoverySpi(IgniteDiscoverySpi delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Serializable consistentId() throws IgniteSpiException {
            return delegate.consistentId();
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> getRemoteNodes() {
            return delegate.getRemoteNodes();
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getLocalNode() {
            return delegate.getLocalNode();
        }

        /** {@inheritDoc} */
        @Override public @Nullable ClusterNode getNode(UUID nodeId) {
            return delegate.getNode(nodeId);
        }

        /** {@inheritDoc} */
        @Override public boolean pingNode(UUID nodeId) {
            return delegate.pingNode(nodeId);
        }

        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
            delegate.setNodeAttributes(attrs, ver);
        }

        /** {@inheritDoc} */
        @Deprecated
        @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
            delegate.setListener(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
            delegate.setDataExchange(new DelegatedDiscoverySpiDataExchange(exchange));
        }

        /** {@inheritDoc} */
        @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
            delegate.setMetricsProvider(metricsProvider);
        }

        /** {@inheritDoc} */
        @Override public void disconnect() throws IgniteSpiException {
            delegate.disconnect();
        }

        /** {@inheritDoc} */
        @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
            delegate.setAuthenticator(auth);
        }

        /** {@inheritDoc} */
        @Override public long getGridStartTime() {
            return delegate.getGridStartTime();
        }

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoveryCustomMessage msg) throws IgniteException {
            delegate.sendCustomEvent(msg);
        }

        /** {@inheritDoc} */
        @Override public void failNode(UUID nodeId, @Nullable String warning) {
            delegate.failNode(nodeId, warning);
        }

        /** {@inheritDoc} */
        @Override public boolean isClientMode() throws IllegalStateException {
            return delegate.isClientMode();
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            delegate.spiStart(igniteInstanceName);
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            delegate.spiStop();
        }

        /** {@inheritDoc} */
        @Override protected void onContextInitialized0(final IgniteSpiContext spiCtx) throws IgniteSpiException {
            delegate.onContextInitialized(spiCtx);
        }

        /** {@inheritDoc} */
        @Override public void injectResources(Ignite ignite) {
            super.injectResources(ignite);

            if (ignite != null && delegate instanceof IgniteSpiAdapter) {
                try {
                    ((IgniteEx)ignite).context().resource().inject(delegate);
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to inject resources", e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public boolean knownNode(UUID nodeId) {
            return delegate.knownNode(nodeId);
        }

        /** {@inheritDoc} */
        @Override public boolean clientReconnectSupported() {
            return delegate.clientReconnectSupported();
        }

        /** {@inheritDoc} */
        @Override public void clientReconnect() {
            delegate.clientReconnect();
        }

        /** {@inheritDoc} */
        @Override public void simulateNodeFailure() {
            delegate.simulateNodeFailure();
        }

        /** {@inheritDoc} */
        @Override public boolean supportsCommunicationFailureResolve() {
            return delegate.supportsCommunicationFailureResolve();
        }

        /** {@inheritDoc} */
        @Override public void resolveCommunicationFailure(ClusterNode node, Exception err) {
            delegate.resolveCommunicationFailure(node, err);
        }

        /** Delegated discovery data exchange. */
        private class DelegatedDiscoverySpiDataExchange implements DiscoverySpiDataExchange {
            /** Discovery data exchange delegate. */
            private final DiscoverySpiDataExchange delegate;

            /**
             * @param delegate Discovery data exchange delegate.
             */
            public DelegatedDiscoverySpiDataExchange(DiscoverySpiDataExchange delegate) {
                this.delegate = delegate;
            }

            /** {@inheritDoc} */
            @Override public DiscoveryDataBag collect(DiscoveryDataBag dataBag) {
                if (dataExchangeCollectClosure != null)
                    dataExchangeCollectClosure.accept(getLocalNode(), dataBag);

                return delegate.collect(dataBag);
            }

            /** {@inheritDoc} */
            @Override public void onExchange(DiscoveryDataBag dataBag) {
                delegate.onExchange(dataBag);
            }
        }
    }
}
