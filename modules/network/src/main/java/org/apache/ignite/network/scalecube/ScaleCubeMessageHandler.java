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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.MessageHandlerHolder;

/**
 * Integration class for adapting {@link NetworkMessageHandler} and {@link NetworkClusterEventHandler} in terms of
 * ScaleCube.
 */
public class ScaleCubeMessageHandler implements ClusterMessageHandler {
    /** Instance of scalecube cluster. */
    private final Cluster cluster;

    /** Resolver from/to inner member to/from public one. */
    private final ScaleCubeMemberResolver scaleCubeMemberResolver;

    /** Storage of all handlers for execution. */
    private final MessageHandlerHolder messageHandlerHolder;

    /** Utility map for recognizing member for its address(scalecube doesn't provide such information in input message). */
    private final Map<Address, NetworkMember> addressMemberMap = new ConcurrentHashMap<>();

    /**
     * @param cluster Instance of scalecube cluster.
     * @param resolver Resolver from/to inner member to/from public one.
     * @param holder Storage of all handlers for execution.
     */
    public ScaleCubeMessageHandler(
        Cluster cluster,
        ScaleCubeMemberResolver resolver,
        MessageHandlerHolder holder
    ) {
        this.cluster = cluster;
        scaleCubeMemberResolver = resolver;
        messageHandlerHolder = holder;
    }

    /** {@inheritDoc} */
    @Override public void onMessage(Message message) {
        for (NetworkMessageHandler handler : messageHandlerHolder.messageHandlers()) {
            handler.onReceived(new NetworkMessage(message.data(), memberForAddress(message.sender())));
        }
    }

    /**
     * @param address Inet address.
     * @return Network member corresponded to input address.
     */
    private NetworkMember memberForAddress(Address address) {
        return addressMemberMap.computeIfAbsent(address,
            (key) -> cluster
                .members().stream()
                .filter(mem -> mem.address().equals(address))
                .map(scaleCubeMemberResolver::resolveNetworkMember)
                .findFirst()
                .orElse(null)
        );
    }

    /** {@inheritDoc} */
    @Override public void onMembershipEvent(MembershipEvent event) {
        for (NetworkClusterEventHandler lsnr : messageHandlerHolder.clusterEventHandlers()) {
            if (event.type() == MembershipEvent.Type.ADDED)
                lsnr.onAppeared(scaleCubeMemberResolver.resolveNetworkMember(event.member()));
            else if (event.type() == MembershipEvent.Type.LEAVING || event.type() == MembershipEvent.Type.REMOVED)
                lsnr.onDisappeared((scaleCubeMemberResolver.resolveNetworkMember(event.member())));
            else if (event.type() == MembershipEvent.Type.UPDATED) {
                //do nothing.
            }
            else
                throw new RuntimeException("This event is not supported: event = " + event);
        }
    }
}
