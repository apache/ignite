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

package org.apache.ignite.internal.client;

import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Descriptor of remote grid node. Use {@link GridClientCompute#nodes()} to
 * get a full view over remote grid nodes.
 */
public interface GridClientNode {
    /**
     * Gets ID of a remote node.
     *
     * @return Node ID.
     */
    public UUID nodeId();

    /**
     * Gets consistent globally unique node ID. Unlike {@link #nodeId()} method,
     * this method returns consistent node ID which survives node restarts.
     *
     * @return Consistent globally unique node ID.
     */
    public Object consistentId();

    /**
     * Gets list of REST TCP server addresses of remote node.
     *
     * @return REST TCP server addresses.
     */
    public List<String> tcpAddresses();

    /**
     * Gets list of REST TCP server host names of remote node.
     *
     * @return REST TCP server host names.
     */
    public List<String> tcpHostNames();

    /**
     * Gets list of REST HTTP server addresses of remote node.
     *
     * @return REST HTTP server addresses.
     */
    @Deprecated
    public List<String> jettyAddresses();

    /**
     * Gets list of REST HTTP server host names of remote node.
     *
     * @return REST HTTP server host names.
     */
    @Deprecated
    public List<String> jettyHostNames();

    /**
     * Gets client TCP port of remote node.
     *
     * @return Remote tcp port.
     */
    public int tcpPort();

    /**
     * Gets client HTTP port of remote node.
     *
     * @return Remote http port.
     */
    @Deprecated
    public int httpPort();

    /**
     * Gets all attributes of remote node. Note that all system and
     * environment properties are automatically includes in node
     * attributes. User can also attach custom attributes and then
     * use them to further filter remote nodes into virtual subgrids
     * for task execution.
     *
     * @return All node attributes.
     */
    public Map<String, Object> attributes();

    /**
     * Gets specific attribute of remote node.
     *
     * @param name Attribute name.
     * @return Attribute value.
     * @see #attributes()
     */
    @Nullable public <T> T attribute(String name);

    /**
     * Gets various dynamic metrics of remote node.
     *
     * @return Metrics of remote node.
     */
    public GridClientNodeMetrics metrics();

    /**
     * Gets all configured caches and their types on remote node.
     *
     * @return Map in which key is a configured cache name on the node,
     *      value is mode of configured cache.
     */
    public Map<String, GridClientCacheMode> caches();

    /**
     * Gets node replica count for consistent hash ring (valid only for
     * {@code PARTITIONED} caches).
     *
     * @return Node replica count for consistent hash ring.
     */
    public int replicaCount();

    /**
     * Gets collection of addresses on which REST binary protocol is bound.
     *
     * @param proto Protocol for which addresses are obtained.
     * @param filterResolved Whether to filter resolved addresses ( {@link InetSocketAddress#isUnresolved()}
     * returns {@code False} ) or not.
     * @return List of addresses.
     */
    public Collection<InetSocketAddress> availableAddresses(GridClientProtocol proto, boolean filterResolved);

    /**
     * Indicates whether client can establish direct connection with this node.
     * So it is guaranteed that that any request will take only one network
     * 'hop' before it will be processed by a Grid node.
     *
     * @return {@code true} if node can be directly connected,
     *  {@code false} if request may be passed through a router.
     */
    public boolean connectable();
}
