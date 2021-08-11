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

package org.apache.ignite.network;

import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * Network configuration of a node.
 *
 * TODO: migrate to common configuration class when it's available, see
 *  https://issues.apache.org/jira/browse/IGNITE-14496
 */
public class ClusterLocalConfiguration {
    /** The network alias of a node. */
    private final String name;

    /** The port. */
    private final int port;

    /** Node finder. */
    private final NodeFinder nodeFinder;

    /** Message mapper providers. */
    private final MessageSerializationRegistry serializationRegistry;

    /**
     * Constructor.
     *
     * @param name Local name.
     * @param port Local port.
     * @param nodeFinder Node finder for discovering the initial cluster members.
     * @param serializationRegistry Message serialization registry.
     */
    public ClusterLocalConfiguration(
        String name, int port, NodeFinder nodeFinder, MessageSerializationRegistry serializationRegistry
    ) {
        this.name = name;
        this.port = port;
        this.nodeFinder = nodeFinder;
        this.serializationRegistry = serializationRegistry;
    }

    /**
     * Returns the network alias of the node.
     *
     * @return Network alias of a node.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the local network port of the node.
     *
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the Node Finder for discovering the initial cluster members.
     *
     * @return Node Finder.
     */
    public NodeFinder getNodeFinder() {
        return nodeFinder;
    }

    /**
     * Returns the message serialization registry.
     *
     * @return Message serialization registry.
     */
    public MessageSerializationRegistry getSerializationRegistry() {
        return serializationRegistry;
    }
}
