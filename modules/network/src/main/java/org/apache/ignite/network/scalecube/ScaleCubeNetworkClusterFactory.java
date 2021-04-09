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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.net.Address;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.network.MessageHandlerHolder;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterContext;
import org.apache.ignite.network.NetworkClusterFactory;
import org.apache.ignite.network.NetworkConfigurationException;

/**
 * Factory for ScaleCubeNetworkCluster.
 */
public class ScaleCubeNetworkClusterFactory implements NetworkClusterFactory {
    /** Unique name of network member. */
    private final String localMemberName;

    /** Local port. */
    private final int localPort;

    /** Network addresses to find another members in cluster. */
    private final List<Address> addresses;

    /**
     * Member resolver which allows convert {@link org.apache.ignite.network.NetworkMember} to inner ScaleCube type
     * and otherwise.
     */
    private final ScaleCubeMemberResolver memberResolver;

    /**
     * @param localMemberName Unique name of network member.
     * @param port Local port.
     * @param addresses Network addresses to find another members in cluster.
     */
    public ScaleCubeNetworkClusterFactory(
        String localMemberName,
        int port,
        List<String> addresses,
        ScaleCubeMemberResolver memberResolver
    ) {
        this.localMemberName = localMemberName;
        this.localPort = port;
        this.addresses = addresses.stream().map(address -> {
            try {
                return Address.from(address);
            }
            catch (IllegalStateException e) {
                throw new NetworkConfigurationException("Failed to parse address", e);
            }
        }).collect(Collectors.toList());
        this.memberResolver = memberResolver;
    }

    /**
     * Start ScaleCube network cluster.
     *
     * @param memberResolver Member resolve which allows convert {@link org.apache.ignite.network.NetworkMember} to
     * inner ScaleCube type and otherwise.
     * @param messageHandlerHolder Holder of all cluster message handlers.
     * @return {@link NetworkCluster} instance.
     */
    @Override public NetworkCluster startCluster(NetworkClusterContext clusterContext) {
        MessageHandlerHolder handlerHolder = clusterContext.messageHandlerHolder();

        Cluster cluster = new ClusterImpl()
            .handler(cl -> {
                return new ScaleCubeMessageHandler(cl, memberResolver, handlerHolder);
            })
            .config(opts -> opts
                .memberAlias(localMemberName)
                .transport(trans -> {
                    return trans.port(localPort);
                })
            )
            .membership(opts -> {
                return opts.seedMembers(addresses);
            })
            .startAwait();

        return new ScaleCubeNetworkCluster(cluster, memberResolver, handlerHolder);
    }

}
