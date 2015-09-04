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

package org.apache.ignite.spi;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.jetbrains.annotations.Nullable;

/**
 * SPI context provides common functionality for all SPI implementations.
 */
public interface IgniteSpiContext {
    /**
     * Gets a collection of remote grid nodes. Remote nodes are discovered via underlying
     * {@link org.apache.ignite.spi.discovery.DiscoverySpi} implementation used. Unlike {@link #nodes()},
     * this method does not include local grid node.
     *
     * @return Collection of remote grid nodes.
     * @see #localNode()
     * @see #nodes()
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public Collection<ClusterNode> remoteNodes();

    /**
     * Gets a collection of all grid nodes. Remote nodes are discovered via underlying
     * {@link org.apache.ignite.spi.discovery.DiscoverySpi} implementation used. Unlike {@link #remoteNodes()},
     * this method does include local grid node.
     *
     * @return Collection of remote grid nodes.
     * @see #localNode()
     * @see #remoteNodes()
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public Collection<ClusterNode> nodes();

    /**
     * Gets local grid node. Instance of local node is provided by underlying {@link org.apache.ignite.spi.discovery.DiscoverySpi}
     * implementation used.
     *
     * @return Local grid node.
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public ClusterNode localNode();

    /**
     * Gets a collection of all remote daemon nodes in topology. The daemon nodes are discovered via
     * underlying {@link org.apache.ignite.spi.discovery.DiscoverySpi} implementation used.
     *
     * @return Collection of all daemon nodes.
     * @see #localNode()
     * @see #remoteNodes()
     * @see #nodes()
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public Collection<ClusterNode> remoteDaemonNodes();

    /**
     * Gets a node instance based on its ID.
     *
     * @param nodeId ID of a node to get.
     * @return Node for a given ID or {@code null} is such not has not been discovered.
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    @Nullable public ClusterNode node(UUID nodeId);

    /**
     * Pings a remote node. The underlying communication is provided via
     * {@link org.apache.ignite.spi.discovery.DiscoverySpi#pingNode(UUID)} implementation.
     * <p>
     * Discovery SPIs usually have some latency in discovering failed nodes. Hence,
     * communication to remote nodes may fail at times if an attempt was made to
     * establish communication with a failed node. This method can be used to check
     * if communication has failed due to node failure or due to some other reason.
     *
     * @param nodeId ID of a node to ping.
     * @return {@code true} if node for a given ID is alive, {@code false} otherwise.
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public boolean pingNode(UUID nodeId);

    /**
     * Sends a message to a remote node. The underlying communication mechanism is defined by
     * {@link org.apache.ignite.spi.communication.CommunicationSpi} implementation used.
     *
     * @param node Node to send a message to.
     * @param msg Message to send.
     * @param topic Topic to send message to.
     * @throws IgniteSpiException If failed to send a message to remote node.
     */
    public void send(ClusterNode node, Serializable msg, String topic) throws IgniteSpiException;

    /**
     * Register a message listener to receive messages sent by remote nodes. The underlying
     * communication mechanism is defined by {@link org.apache.ignite.spi.communication.CommunicationSpi} implementation used.
     * <p>
     * This method can be used by jobs to communicate with other nodes in the grid. Remote nodes
     * can send messages by calling {@link #send(org.apache.ignite.cluster.ClusterNode, Serializable, String)} method.
     *
     * @param lsnr Message listener to register.
     * @param topic Topic to register listener for.
     */
    public void addMessageListener(GridMessageListener lsnr, String topic);

    /**
     * Removes a previously registered message listener.
     *
     * @param lsnr Message listener to remove.
     * @param topic Topic to unregister listener for.
     * @return {@code true} of message listener was removed, {@code false} if it was not
     *      previously registered.
     */
    public boolean removeMessageListener(GridMessageListener lsnr, String topic);

    /**
     * Adds an event listener for local events.
     *
     * @param lsnr Event listener for local events.
     * @param types Optional types for which this listener will be notified. If no types are provided
     *      this listener will be notified for all local events.
     * @see org.apache.ignite.events.Event
     */
    public void addLocalEventListener(GridLocalEventListener lsnr, int... types);

    /**
     * Removes local event listener.
     *
     * @param lsnr Local event listener to remove.
     * @return {@code true} if listener was removed, {@code false} otherwise.
     */
    public boolean removeLocalEventListener(GridLocalEventListener lsnr);

    /**
     * Checks whether all provided event types are recordable.
     *
     * @param types Event types to check.
     * @return Whether or not all provided event types are recordable..
     */
    public boolean isEventRecordable(int... types);

    /**
     * Records local event.
     *
     * @param evt Local grid event to record.
     */
    public void recordEvent(Event evt);

    /**
     * Registers open port.
     *
     * @param port Port.
     * @param proto Protocol.
     */
    public void registerPort(int port, IgnitePortProtocol proto);

    /**
     * Deregisters closed port.
     *
     * @param port Port.
     * @param proto Protocol.
     */
    public void deregisterPort(int port, IgnitePortProtocol proto);

    /**
     * Deregisters all closed ports.
     */
    public void deregisterPorts();

    /**
     * Gets object from cache.
     *
     * @param cacheName Cache name.
     * @param key Object key.
     * @return Cached object.
     * @throws CacheException Thrown if any exception occurs.
     */
    @Nullable public <K, V> V get(String cacheName, K key) throws CacheException;

    /**
     * Puts object in cache.
     *
     * @param cacheName Cache name.
     * @param key Object key.
     * @param val Cached object.
     * @param ttl Time to live, {@code 0} means the entry will never expire.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Previous value associated with specified key, possibly {@code null}.
     * @throws CacheException Thrown if any exception occurs.
     */
    @Nullable public <K, V> V put(String cacheName, K key, V val, long ttl) throws CacheException;

    /**
     * Puts object into cache if there was no previous object associated with
     * given key.
     *
     * @param cacheName Cache name.
     * @param key Cache key.
     * @param val Cache value.
     * @param ttl Time to live.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @return Either existing value or {@code null} if there was no value for given key.
     * @throws CacheException If put failed.
     */
    @Nullable public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) throws CacheException;

    /**
     * Removes object from cache.
     *
     * @param cacheName Cache name.
     * @param key Object key.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Previous value associated with specified key, possibly {@code null}.
     * @throws CacheException Thrown if any exception occurs.
     */
    @Nullable public <K, V> V remove(String cacheName, K key) throws CacheException;

    /**
     * Returns {@code true} if this cache contains a mapping for the specified key.
     *
     * @param cacheName Cache name.
     * @param key Object key.
     * @param <K> Key type.
     * @return {@code true} if this cache contains a mapping for the specified key.
     */
    public <K> boolean containsKey(String cacheName, K key);

    /**
     * Calculates partition number for given key.
     *
     * @param cacheName Cache name.
     * @param key Key.
     * @return Partition.
     */
    public int partition(String cacheName, Object key);

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable public IgniteNodeValidationResult validateNode(ClusterNode node);

    /**
     * Gets collection of authenticated subjects together with their permissions.
     *
     * @return Collection of authenticated subjects.
     * @throws IgniteException If any exception occurs.
     */
    public Collection<SecuritySubject> authenticatedSubjects() throws IgniteException;

    /**
     * Gets security subject based on subject ID.
     *
     * @param subjId Subject ID.
     * @return Authorized security subject.
     * @throws IgniteException If any exception occurs.
     */
    public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteException;

    /**
     * Gets message formatter.
     *
     * @return Message formatter.
     */
    public MessageFormatter messageFormatter();

    /**
     * Gets message factory.
     *
     * @return Message factory.
     */
    public MessageFactory messageFactory();

    /**
     * @return {@code True} if node started shutdown sequence.
     */
    public boolean isStopping();

    /**
     * @param nodeId Node ID.
     * @param warning Warning to be shown on all cluster nodes.
     * @return If node was failed.
     */
    public boolean tryFailNode(UUID nodeId, @Nullable String warning);

    /**
     * @param nodeId Node ID.
     * @param warning Warning to be shown on all cluster nodes.
     */
    public void failNode(UUID nodeId, @Nullable String warning);

    /**
     * @param c Timeout object.
     */
    public void addTimeoutObject(IgniteSpiTimeoutObject c);

    /**
     * @param c Timeout object.
     */
    public void removeTimeoutObject(IgniteSpiTimeoutObject c);
}