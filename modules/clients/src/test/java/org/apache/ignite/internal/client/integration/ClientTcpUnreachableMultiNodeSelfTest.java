/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.client.integration;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientCacheMode;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientNodeMetrics;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Test for TCP binary rest protocol with unreachable address.
 */
public class ClientTcpUnreachableMultiNodeSelfTest extends ClientTcpMultiNodeSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientLoadBalancer getBalancer() {
        final GridClientLoadBalancer b = super.getBalancer();

        return new TestGridClientLoadBalancer(b);
    }

    /** {@inheritDoc} */
    @Override protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cfg = super.clientConfiguration();

        // Setting low connection timeout to allow multiple threads
        // pass the unavailable address quickly.
        cfg.setConnectTimeout(100);

        return cfg;
    }

    /**
     *
     */
    private class TestGridClientLoadBalancer implements GridClientLoadBalancer, GridClientTopologyListener {
        /** */
        private final GridClientLoadBalancer b;

        /**
         * @param b Delegating balancer.
         */
        TestGridClientLoadBalancer(GridClientLoadBalancer b) {
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public GridClientNode balancedNode(Collection<? extends GridClientNode> nodes)
            throws GridClientException {
            final GridClientNode node = b.balancedNode(nodes);

            return new GridClientNode() {
                @Override public <T> T attribute(String name) {
                    return node.attribute(name);
                }

                @Override public Map<String, Object> attributes() {
                    return node.attributes();
                }

                @Override public Collection<InetSocketAddress> availableAddresses(GridClientProtocol proto,
                    boolean filterResolved) {
                    // Fake address first.
                    return F.asList(new InetSocketAddress("172.22.13.13", 65432),
                        F.first(node.availableAddresses(proto, filterResolved)));
                }

                @Override public Map<String, GridClientCacheMode> caches() {
                    return node.caches();
                }

                @Override public List<String> tcpAddresses() {
                    return node.tcpAddresses();
                }

                @Override public List<String> tcpHostNames() {
                    return node.tcpHostNames();
                }

                @Override public GridClientNodeMetrics metrics() {
                    return node.metrics();
                }

                @Override public UUID nodeId() {
                    return node.nodeId();
                }

                @Override public Object consistentId() {
                    return node.consistentId();
                }

                @Override public int tcpPort() {
                    return node.tcpPort();
                }

                @Override public boolean connectable() {
                    return node.connectable();
                }

                @Override public long order() {
                    return node.order();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void onNodeAdded(GridClientNode node) {
            if (b instanceof GridClientTopologyListener)
                ((GridClientTopologyListener)b).onNodeAdded(node);
        }

        /** {@inheritDoc} */
        @Override public void onNodeRemoved(GridClientNode node) {
            if (b instanceof GridClientTopologyListener)
                ((GridClientTopologyListener)b).onNodeRemoved(node);
        }
    }
}
