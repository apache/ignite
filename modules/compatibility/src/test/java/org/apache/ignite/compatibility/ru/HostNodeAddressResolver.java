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

package org.apache.ignite.compatibility.ru;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AddressResolver;

/**
 * Address resolver for a local (non-container) node joining a cluster with Docker containers.
 * <p>
 * On macOS with Docker Desktop, containers run inside a VM and cannot reach the host's
 * real IP addresses (e.g., 192.168.x.x). The only way to reach the host from a container
 * is via the special DNS name {@code host.docker.internal}, which Docker resolves to the
 * host gateway inside the VM.
 * <p>
 * This resolver maps internal addresses to {@code host.docker.internal} using a resolved
 * InetAddress so that Ignite discovery messages can serialize them properly.
 */
public class HostNodeAddressResolver implements AddressResolver {
    /** Special Docker DNS name that resolves to the host machine from inside containers. */
    private static final String HOST_DOCKER_INTERNAL = "host.docker.internal";

    /**
     * Creates a resolved InetAddress for host.docker.internal using a non-loopback dummy IP (255.255.255.255).
     * Using a non-loopback IP ensures that the address is NOT filtered out by TcpDiscoverySpi's
     * loopback address removal logic in getEffectiveNodeAddresses.
     * The hostname is preserved as host.docker.internal so that containers will resolve it
     * via Docker's DNS to the actual host gateway.
     */
    private static InetAddress createHostDockerAddress() {
        try {
            // Use 255.255.255.255 (broadcast) as dummy IP — it's non-loopback,
            // but the hostname "host.docker.internal" will be used for DNS resolution by containers.
            return InetAddress.getByAddress(HOST_DOCKER_INTERNAL, new byte[]{(byte) 255, (byte) 255, (byte) 255, (byte) 255});
        }
        catch (UnknownHostException e) {
            throw new RuntimeException("Failed to create " + HOST_DOCKER_INTERNAL + " address", e);
        }
    }

    /** */
    private static final InetAddress HOST_DOCKER_ADDR = createHostDockerAddress();

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) throws IgniteCheckedException {
        if (addr == null)
            return Collections.emptyList();

        // Map all local addresses to host.docker.internal so containers have a reachable address.
        // Local nodes will try the internal 192.168.x.x first (which works for local-to-local).
        return List.of(new InetSocketAddress(HOST_DOCKER_ADDR, addr.getPort()));
    }
}
