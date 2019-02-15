/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.ipc.loopback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Loopback IPC endpoint based on socket.
 */
public class IpcClientTcpEndpoint implements IpcEndpoint {
    /** Client socket. */
    private Socket clientSock;

    /**
     * Creates connected client IPC endpoint.
     *
     * @param clientSock Connected client socket.
     */
    public IpcClientTcpEndpoint(Socket clientSock) {
        assert clientSock != null;

        this.clientSock = clientSock;
    }

    /**
     * Creates and connects client IPC endpoint.
     *
     * @param port Port.
     * @param host Host.
     * @throws IgniteCheckedException If connection fails.
     */
    public IpcClientTcpEndpoint(String host, int port) throws IgniteCheckedException {
        clientSock = new Socket();

        try {
            clientSock.connect(new InetSocketAddress(host, port));
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to connect to endpoint [host=" + host + ", port=" + port + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public InputStream inputStream() throws IgniteCheckedException {
        try {
            return clientSock.getInputStream();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream outputStream() throws IgniteCheckedException {
        try {
            return clientSock.getOutputStream();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(clientSock);
    }
}