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

package org.apache.ignite.internal.client.impl.connection;

import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.jetbrains.annotations.Nullable;

/**
 * Common interface for client connection managers.
 */
public interface GridClientConnectionManager {
    /**
     * Tries to open initial connection and fetch topology using given server addresses.
     *
     * @param srvs Collection<InetSocketAddress> server addresses.
     * @throws GridClientAuthenticationException If connection failed to authenticate on server.
     * @throws GridClientException If manager failed to initialise,
     * @throws InterruptedException If manager was interrupted while waiting for connection.
     */
    public void init(Collection<InetSocketAddress> srvs) throws GridClientException, InterruptedException;

    /**
     * Returns connection to the given node.
     *
     * @param node Node to connect with.
     * @return Connection to use for operations, targeted for the given node.
     * @throws GridServerUnreachableException If connection can't be established.
     * @throws InterruptedException If manager was interrupted while waiting for connection
     * to be established.
     * @throws org.apache.ignite.internal.client.GridClientClosedException If connection manager has been closed.
     */
    public GridClientConnection connection(GridClientNode node)
        throws GridServerUnreachableException, GridClientClosedException, InterruptedException;

    /**
     * Callback method, which should be called by clients when they get connectivity errors.
     * It's main purpose is to allow connection manager to terminate broken connection
     * early and, try to establish a new one for the consequent
     * {@link #connection(GridClientNode)} calls.
     *
     * @param conn Failed connection.
     * @param node Connected node.
     * @param e Error that caused connection termination.
     */
    public void terminateConnection(GridClientConnection conn, @Nullable GridClientNode node, Throwable e);

    /**
     * Stops this instance of connection manager and terminates all connections.
     * @param waitCompletion If {@code true} this method awaits termination of all connections
     *      (and receiving responses for all pending requests), otherwise it will return immediately.
     */
    public void stop(boolean waitCompletion);
}