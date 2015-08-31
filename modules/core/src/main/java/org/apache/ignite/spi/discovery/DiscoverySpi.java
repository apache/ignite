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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

/**
 * Grid discovery SPI allows to discover remote nodes in grid.
 * <p>
 * The default discovery SPI is {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
 * with default configuration which allows all nodes in local network
 * (with enabled multicast) to discover each other.
 * <p>
 * Ignite provides the following {@code GridDeploymentSpi} implementation:
 * <ul>
 * <li>{@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface DiscoverySpi extends IgniteSpi {
    /**
     * Gets collection of remote nodes in grid or empty collection if no remote nodes found.
     *
     * @return Collection of remote nodes.
     */
    public Collection<ClusterNode> getRemoteNodes();

    /**
     * Gets local node.
     *
     * @return Local node.
     */
    public ClusterNode getLocalNode();

    /**
     * Gets node by ID.
     *
     * @param nodeId Node ID.
     * @return Node with given ID or {@code null} if node is not found.
     */
    @Nullable public ClusterNode getNode(UUID nodeId);

    /**
     * Pings the remote node to see if it's alive.
     *
     * @param nodeId Node Id.
     * @return {@code true} if node alive, {@code false} otherwise.
     */
    public boolean pingNode(UUID nodeId);

    /**
     * Sets node attributes and node version which will be distributed in grid during
     * join process. Note that these attributes cannot be changed and set only once.
     *
     * @param attrs Map of node attributes.
     * @param ver Product version.
     */
    public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver);

    /**
     * Sets a listener for discovery events. Refer to
     * {@link org.apache.ignite.events.DiscoveryEvent} for a set of all possible
     * discovery events.
     * <p>
     * Note that as of Ignite 3.0.2 this method is called <b>before</b>
     * method {@link #spiStart(String)} is called. This is done to
     * avoid potential window when SPI is started but the listener is
     * not registered yet.
     *
     * @param lsnr Listener to discovery events or {@code null} to unset the listener.
     */
    public void setListener(@Nullable DiscoverySpiListener lsnr);

    /**
     * Sets a handler for initial data exchange between Ignite nodes.
     *
     * @param exchange Discovery data exchange handler.
     */
    public TcpDiscoverySpi setDataExchange(DiscoverySpiDataExchange exchange);

    /**
     * Sets discovery metrics provider. Use metrics provided by
     * {@link DiscoveryMetricsProvider#metrics()} method to exchange
     * dynamic metrics between nodes.
     *
     * @param metricsProvider Provider of metrics data.
     */
    public TcpDiscoverySpi setMetricsProvider(DiscoveryMetricsProvider metricsProvider);

    /**
     * Tells discovery SPI to disconnect from topology. This is very close to calling
     * {@link #spiStop()} with accounting that it is not a full stop,
     * but disconnect due to segmentation.
     *
     * @throws IgniteSpiException If any error occurs.
     */
    public void disconnect() throws IgniteSpiException;

    /**
     * Sets discovery SPI node authenticator. This method is called before SPI start() method.
     *
     * @param auth Discovery SPI authenticator.
     */
    public void setAuthenticator(DiscoverySpiNodeAuthenticator auth);

    /**
     * Gets start time of the very first node in the grid. This value should be the same
     * on all nodes in the grid and it should not change even if very first node fails
     * of leaves grid.
     *
     * @return Start time of the first node in grid or {@code 0} if SPI implementation
     *         does not support this method.
     */
    public long getGridStartTime();

    /**
     * Sends custom message across the ring.
     * @param msg Custom message.
     * @throws IgniteException if failed to marshal evt.
     */
    public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException;

    /**
     * Initiates failure of provided node.
     *
     * @param nodeId Node ID.
     * @param warning Warning to be shown on all cluster nodes.
     */
    public void failNode(UUID nodeId, @Nullable String warning);

    /**
     * Whether or not discovery is started in client mode.
     *
     * @return {@code true} if node is in client mode.
     * @throws IllegalStateException If discovery SPI has not started.
     */
    public boolean isClientMode() throws IllegalStateException;
}