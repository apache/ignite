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

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.network.message.MessageMapperProvider;

/**
 * Entry point for network module.
 */
public class Network {
    /** Message mapper providers, messageMapperProviders[message type] -> message mapper provider for message with message type. */
    private final MessageMapperProvider<?>[] messageMapperProviders = new MessageMapperProvider<?>[Short.MAX_VALUE << 1];

    /** Message handlers. */
    private final MessageHandlerHolder messageHandlerHolder = new MessageHandlerHolder();

    /** Cluster factory. */
    private final NetworkClusterFactory clusterFactory;

    /**
     * Constructor.
     * @param factory Cluster factory.
     */
    public Network(NetworkClusterFactory factory) {
        clusterFactory = factory;
    }

    /**
     * Register message mapper by message type.
     * @param type Message type.
     * @param mapperProvider Message mapper provider.
     */
    public void registerMessageMapper(short type, MessageMapperProvider mapperProvider) throws NetworkConfigurationException {
        if (this.messageMapperProviders[type] != null)
            throw new NetworkConfigurationException("Message mapper for type " + type + " is already defined");

        this.messageMapperProviders[type] = mapperProvider;
    }

    /**
     * Start new cluster.
     * @return Network cluster.
     */
    public NetworkCluster start() {
        //noinspection Java9CollectionFactory
        NetworkClusterContext context = new NetworkClusterContext(messageHandlerHolder, Collections.unmodifiableList(Arrays.asList(messageMapperProviders)));
        return clusterFactory.startCluster(context);
    }
}
