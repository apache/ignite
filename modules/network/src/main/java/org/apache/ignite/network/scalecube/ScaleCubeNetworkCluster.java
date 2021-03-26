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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.ignite.network.MessageHandlerHolder;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessageHandler;

import static io.scalecube.cluster.transport.api.Message.fromData;
import static java.time.Duration.ofMillis;

/**
 * Implementation of {@link NetworkCluster} based on ScaleCube.
 */
public class ScaleCubeNetworkCluster implements NetworkCluster {
    /** Inner representation of cluster of scalecube. */
    private final Cluster cluster;

    /** Resolver for scalecube specific member. */
    private final ScaleCubeMemberResolver memberResolver;

    /** Holder of all cluster handlers. */
    private final MessageHandlerHolder messageHandlerHolder;

    /**
     * @param cluster Inner representation of cluster of scalecube.
     * @param memberResolver Resolver for scalecube specific member.
     * @param messageHandlerHolder Holder of all cluster handlers.
     */
    public ScaleCubeNetworkCluster(
        Cluster cluster,
        ScaleCubeMemberResolver memberResolver,
        MessageHandlerHolder messageHandlerHolder
    ) {
        this.messageHandlerHolder = messageHandlerHolder;
        this.cluster = cluster;
        this.memberResolver = memberResolver;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() throws Exception {
        cluster.shutdown();

        cluster.onShutdown().block();
    }

    /** {@inheritDoc} */
    @Override public NetworkMember localMember() {
        return memberResolver.resolveNetworkMember(cluster.member());
    }

    /** {@inheritDoc} */
    @Override public Collection<NetworkMember> allMembers() {
        return cluster.members().stream()
            .map(memberResolver::resolveNetworkMember)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void weakSend(NetworkMember member, Object msg) {
        cluster.send(memberResolver.resolveMember(member), fromData(msg))
            .block();
    }

    /** {@inheritDoc} */
    @Override public Future<?> send(NetworkMember member, Object msg) {
        return cluster.send(memberResolver.resolveMember(member), fromData(msg)).toFuture();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<?> sendWithResponse(NetworkMember member, Object msg, long timeout) {
        return cluster.requestResponse(memberResolver.resolveMember(member), fromData(msg))
            .timeout(ofMillis(timeout)).toFuture().thenApply(m -> m.data());
    }

    /** {@inheritDoc} */
    @Override public void addHandlersProvider(NetworkHandlersProvider networkHandlersProvider) {
        NetworkClusterEventHandler lsnr = networkHandlersProvider.clusterEventHandler();

        if (lsnr != null)
            messageHandlerHolder.addClusterEventHandlers(lsnr);

        NetworkMessageHandler messageHandler = networkHandlersProvider.messageHandler();

        if (messageHandler != null)
            messageHandlerHolder.addmessageHandlers(messageHandler);
    }
}
