/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;
import org.gridgain.grid.util.lang.*;

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
     * @throws GridException If handshake failed.
     */
    void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws GridException;

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
     * @throws GridException If failed.
     */
    void sendMessage(ByteBuffer data) throws GridException;

    /**
     * @param data Data to send.
     * @param len Length.
     * @throws GridException If failed.
     */
    void sendMessage(byte[] data, int len) throws GridException;

    /**
     * @param nodeId Node ID (provided only if versions of local and remote nodes are different).
     * @param msg Message to send.
     * @throws GridException If failed.
     */
    void sendMessage(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg) throws GridException;

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
