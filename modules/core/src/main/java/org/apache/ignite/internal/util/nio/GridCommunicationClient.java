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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

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
    void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException;

    /**
     * @return {@code True} if client has been closed by this call,
     *      {@code false} if failed to close client (due to concurrent reservation or concurrent close).
     */
    boolean close();

    /**
     * Forces client close.
     */
    void forceClose();

    /**
     * @return {@code True} if client is closed;
     */
    boolean closed();

    /**
     * @return {@code True} if client was reserved, {@code false} otherwise.
     */
    boolean reserve();

    /**
     * Releases this client by decreasing reservations.
     */
    void release();

    /**
     * @return {@code True} if client was reserved.
     */
    boolean reserved();

    /**
     * Gets idle time of this client.
     *
     * @return Idle time of this client.
     */
    long getIdleTime();

    /**
     * @param data Data to send.
     * @throws IgniteCheckedException If failed.
     */
    void sendMessage(ByteBuffer data) throws IgniteCheckedException;

    /**
     * @param data Data to send.
     * @param len Length.
     * @throws IgniteCheckedException If failed.
     */
    void sendMessage(byte[] data, int len) throws IgniteCheckedException;

    /**
     * @param nodeId Node ID (provided only if versions of local and remote nodes are different).
     * @param msg Message to send.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if should try to resend message.
     */
    boolean sendMessage(@Nullable UUID nodeId, Message msg) throws IgniteCheckedException;

    /**
     * @param timeout Timeout.
     * @throws IOException If failed.
     */
    void flushIfNeeded(long timeout) throws IOException;

    /**
     * @return {@code True} if send is asynchronous.
     */
    boolean async();
}
