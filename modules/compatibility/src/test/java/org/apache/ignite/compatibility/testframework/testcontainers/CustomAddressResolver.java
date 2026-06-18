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

package org.apache.ignite.compatibility.testframework.testcontainers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.AddressResolver;

/**
 * Address resolver for container-to-host communication.
 * <p>
 * When a container receives a discovery/communication address from a local (non-container) node,
 * that address points to the host machine's real IP (e.g., 192.168.x.x), which from inside
 * a Docker container on macOS (Docker Desktop VM) is not reachable. This resolver maps
 * such addresses to {@code host.docker.internal}, a special Docker DNS name that resolves
 * to the host machine from within containers.
 */
public class CustomAddressResolver implements AddressResolver {
    /**
     * Special Docker DNS name that resolves to the host machine from inside containers.
     */
    private static final String HOST_DOCKER_INTERNAL = "host.docker.internal";

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) {
        if (addr == null)
            return Collections.emptyList();

        InetAddress internalAddr = addr.getAddress();

        if (internalAddr == null) {
            // Unresolved address — try to resolve it
            try {
                internalAddr = InetAddress.getByName(addr.getHostName());
            }
            catch (UnknownHostException ignored) {
                return List.of(addr);
            }
        }

        String ip = internalAddr.getHostAddress();

        // Map loopback to host.docker.internal.
        // When a local (non-container) node joins with setLocalHost("127.0.0.1"),
        // from inside the container 127.0.0.1 means the container itself, not the host.
        if (ip.equals("127.0.0.1")) {
            try {
                return List.of(new InetSocketAddress(
                    InetAddress.getByName(HOST_DOCKER_INTERNAL), addr.getPort()));
            }
            catch (UnknownHostException e) {
                return List.of(new InetSocketAddress(HOST_DOCKER_INTERNAL, addr.getPort()));
            }
        }

        // Map the host machine's private IP to host.docker.internal.
        // Docker containers on macOS (Docker Desktop VM) cannot reach the host's real IP (192.168.x.x).
        // Container IPs in Testcontainers network are typically 172.16.x.x-172.31.x.x, so we only map
        // 192.168.x.x which is typical for macOS host interfaces.
        if (ip.startsWith("192.168.")) {
            try {
                return List.of(new InetSocketAddress(InetAddress.getByName(HOST_DOCKER_INTERNAL), addr.getPort()));
            }
            catch (UnknownHostException e) {
                return List.of(new InetSocketAddress(HOST_DOCKER_INTERNAL, addr.getPort()));
            }
        }

        // All other addresses (e.g., Docker network 172.x.x.x) are returned as-is.
        // All other addresses are returned as-is.
        // If the local node advertises its real IP (e.g., 192.168.x.x), containers will try to connect directly.
        // If direct connection fails, they will fall back to other addresses including external ones.
        return List.of(addr);
    }
}
