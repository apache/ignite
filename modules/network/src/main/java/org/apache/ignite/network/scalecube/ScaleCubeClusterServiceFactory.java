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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.network.AbstractClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NetworkConfigurationException;
import org.apache.ignite.network.internal.netty.ConnectionManager;
import org.apache.ignite.network.internal.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.network.internal.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * {@link ClusterServiceFactory} implementation that uses ScaleCube for messaging and topology services.
 * TODO: IGNITE-14538: This factory should use ScaleCube configuration instead of default parameters.
 */
public class ScaleCubeClusterServiceFactory implements ClusterServiceFactory {
    /** {@inheritDoc} */
    @Override public ClusterService createClusterService(ClusterLocalConfiguration context) {
        String consistentId = context.getName();

        var topologyService = new ScaleCubeTopologyService();

        var messagingService = new ScaleCubeMessagingService(topologyService);

        MessageSerializationRegistry registry = context.getSerializationRegistry();

        UUID launchId = UUID.randomUUID();

        var connectionManager = new ConnectionManager(
            context.getPort(),
            registry,
            consistentId,
            () -> new RecoveryServerHandshakeManager(launchId, consistentId),
            () -> new RecoveryClientHandshakeManager(launchId, consistentId)
        );

        ScaleCubeDirectMarshallerTransport transport = new ScaleCubeDirectMarshallerTransport(connectionManager, topologyService);

        var cluster = new ClusterImpl(defaultConfig())
            .handler(cl -> new ClusterMessageHandler() {
                /** {@inheritDoc} */
                @Override public void onMessage(Message message) {
                    messagingService.fireEvent(message);
                }

                /** {@inheritDoc} */
                @Override public void onMembershipEvent(MembershipEvent event) {
                    topologyService.onMembershipEvent(event);
                }
            })
            .config(opts -> opts.memberAlias(consistentId))
            .transport(opts -> opts.transportFactory(new DelegatingTransportFactory(messagingService, config -> transport)))
            .membership(opts -> opts.seedMembers(parseAddresses(context.getMemberAddresses())));

        // resolve cyclic dependencies
        messagingService.setCluster(cluster);

        return new AbstractClusterService(context, topologyService, messagingService) {
            /** {@inheritDoc} */
            @Override public void start() {
                connectionManager.start();

                cluster.startAwait();

                topologyService.setLocalMember(cluster.member());
            }

            /** {@inheritDoc} */
            @Override public void shutdown() {
                stopJmxMonitor();

                cluster.shutdown();
                cluster.onShutdown().block();
                connectionManager.stop();
            }

            /**
             * Removes the JMX MBean registered by the "io.scalecube.cluster.ClusterImpl#startJmxMonitor()" method.
             * Current ScaleCube implementation does not do that which leads to memory leaks.
             */
            private void stopJmxMonitor() {
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();

                try {
                    var pattern = new ObjectName("io.scalecube.cluster", "name", cluster.member().id() + "@*");

                    for (ObjectName name : server.queryNames(pattern, null))
                        server.unregisterMBean(name);
                }
                catch (MalformedObjectNameException | InstanceNotFoundException | MBeanRegistrationException ignore) {
                }
            }
        };
    }

    /**
     * @return The default configuration.
     */
    protected ClusterConfig defaultConfig() {
        return ClusterConfig.defaultConfig();
    }

    /**
     * Convert string addresses to ScaleCube's {@link Address}es.
     * @param addresses "host:port" formatted strings.
     * @return List of addresses.
     */
    private static List<Address> parseAddresses(List<String> addresses) {
        try {
            return addresses.stream()
                .map(Address::from)
                .collect(Collectors.toList());
        }
        catch (IllegalArgumentException e) {
            throw new NetworkConfigurationException("Failed to parse address", e);
        }
    }
}
