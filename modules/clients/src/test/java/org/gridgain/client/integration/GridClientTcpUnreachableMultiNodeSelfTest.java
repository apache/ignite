/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.integration;

import org.gridgain.client.*;
import org.gridgain.client.balancer.*;
import org.gridgain.grid.util.typedef.*;

import java.net.*;
import java.util.*;

/**
 * Test for TCP binary rest protocol with unreachable address.
 */
public class GridClientTcpUnreachableMultiNodeSelfTest extends GridClientTcpMultiNodeSelfTest {
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

                @Override public int replicaCount() {
                    return node.replicaCount();
                }

                @Override public List<String> jettyAddresses() {
                    return node.jettyAddresses();
                }

                @Override public List<String> jettyHostNames() {
                    return node.jettyHostNames();
                }

                @Override public int httpPort() {
                    return node.httpPort();
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
