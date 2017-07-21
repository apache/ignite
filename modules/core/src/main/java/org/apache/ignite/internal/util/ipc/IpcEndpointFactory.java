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

package org.apache.ignite.internal.util.ipc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.ipc.loopback.IpcClientTcpEndpoint;
import org.apache.ignite.internal.util.ipc.loopback.IpcServerTcpEndpoint;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryClientEndpoint;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Igfs endpoint factory for inter-process communication.
 */
public class IpcEndpointFactory {
    /**
     * Connects to open server IPC endpoint.
     *
     * @param endpointAddr Endpoint address.
     * @param log Log.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If failed to establish connection.
     */
    public static IpcEndpoint connectEndpoint(String endpointAddr, IgniteLogger log) throws IgniteCheckedException {
        A.notNull(endpointAddr, "endpointAddr");

        String[] split = endpointAddr.split(":");

        int port;

        if (split.length == 2) {
            try {
                port = Integer.parseInt(split[1]);
            }
            catch (NumberFormatException e) {
                throw new IgniteCheckedException("Failed to parse port number: " + endpointAddr, e);
            }
        }
        else
            // Use default port.
            port = -1;

        return "shmem".equalsIgnoreCase(split[0]) ?
            connectSharedMemoryEndpoint(port > 0 ? port : IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT, log) :
            connectTcpEndpoint(split[0], port > 0 ? port : IpcServerTcpEndpoint.DFLT_IPC_PORT);
    }

    /**
     * Connects loopback IPC endpoint.
     *
     * @param host Loopback host.
     * @param port Loopback endpoint port.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If connection failed.
     */
    private static IpcEndpoint connectTcpEndpoint(String host, int port) throws IgniteCheckedException {
       return new IpcClientTcpEndpoint(host, port);
    }

    /**
     * Connects IPC shared memory endpoint.
     *
     * @param port Endpoint port.
     * @param log Log.
     * @return Connected client endpoint.
     * @throws IgniteCheckedException If connection failed.
     */
    private static IpcEndpoint connectSharedMemoryEndpoint(int port, IgniteLogger log) throws IgniteCheckedException {
        return new IpcSharedMemoryClientEndpoint(port, log);
    }
}