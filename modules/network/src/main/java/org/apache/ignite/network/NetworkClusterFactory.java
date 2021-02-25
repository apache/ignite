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

import java.util.List;
import java.util.stream.Collectors;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.net.Address;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeMessageHandler;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkCluster;

/**
 * Factory of different implementation of {@link NetworkCluster}.
 */
public class NetworkClusterFactory {
    /** Unique name of network member. */
    private final String localMemberName;

    /** Local port. */
    private final int localPort;

    /** Network addresses to find another members in cluster. */
    private final List<String> addresses;

    /**
     * @param localMemberName Unique name of network member.
     * @param port Local port.
     * @param addresses Network addresses to find another members in cluster.
     */
    public NetworkClusterFactory(String localMemberName, int port, List<String> addresses) {
        this.localMemberName = localMemberName;
        localPort = port;
        this.addresses = addresses;
    }

    /**
     * Implementation of {@link NetworkCluster} based on ScaleCube.
     *
     * @param memberResolver Member resolve which allows convert {@link org.apache.ignite.network.NetworkMember} to
     * inner ScaleCube type and otherwise.
     * @param messageHandlerHolder Holder of all cluster message handlers.
     * @return {@link NetworkCluster} instance.
     */
    public NetworkCluster startScaleCubeBasedCluster(
        ScaleCubeMemberResolver memberResolver,
        MessageHandlerHolder messageHandlerHolder
    ) {
        Cluster cluster = new ClusterImpl()
            .handler(cl -> new ScaleCubeMessageHandler(cl, memberResolver, messageHandlerHolder))
            .config(opts -> opts
                .memberAlias(localMemberName)
                .transport(trans -> trans.port(localPort))
            )
            .membership(opts -> opts.seedMembers(addresses.stream().map(Address::from).collect(Collectors.toList())))
            .startAwait();

        return new ScaleCubeNetworkCluster(cluster, memberResolver, messageHandlerHolder);
    }
}
