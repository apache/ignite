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

package org.apache.ignite.internal.util.nio;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface GridCommunicationClient {
    /**
     * Executes the given handshake closure on opened client passing underlying IO streams.
     * This method pulled to client interface a handshake is only operation requiring access
     * to both output and input streams.
     *
     * @param handshakeC Handshake.
     * @throws IgniteCheckedException If handshake failed.
     */
    public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException;

    /**
     * @return {@code True} if client has been closed by this call,
     *      {@code false} if failed to close client (due to concurrent reservation or concurrent close).
     */
    public boolean close();

    /**
     * Forces client close.
     */
    public void forceClose();

    /**
     * @return {@code True} if client is closed;
     */
    public boolean closed();

    /**
     * @return {@code True} if client was reserved, {@code false} otherwise.
     */
    public boolean reserve();

    /**
     * Releases this client by decreasing reservations.
     */
    public void release();

    /**
     * Gets idle time of this client.
     *
     * @return Idle time of this client.
     */
    public long getIdleTime();

    /**
     * @param data Data to send.
     * @throws IgniteCheckedException If failed.
     */
    public void sendMessage(ByteBuffer data) throws IgniteCheckedException;

    /**
     * @param data Data to send.
     * @param len Length.
     * @throws IgniteCheckedException If failed.
     */
    public void sendMessage(byte[] data, int len) throws IgniteCheckedException;

    /**
     * @param nodeId Remote node ID. Provided only for sync clients.
     * @param msg Message to send.
     * @param c Ack closure.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if should try to resend message.
     */
    public boolean sendMessage(@Nullable UUID nodeId, Message msg, @Nullable IgniteInClosure<IgniteException> c)
        throws IgniteCheckedException;

    /**
     * @return {@code True} if send is asynchronous.
     */
    public boolean async();

    /**
     * @return Connection index.
     */
    public int connectionIndex();
}
