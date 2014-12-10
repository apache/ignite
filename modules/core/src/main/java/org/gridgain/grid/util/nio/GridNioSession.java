/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.jetbrains.annotations.*;

import java.net.*;

/**
 * This interface represents established or closed connection between nio server and remote client.
 */
public interface GridNioSession {
    /**
     * Gets local address of this session.
     *
     * @return Local network address or {@code null} if non-socket communication is used.
     */
    @Nullable public InetSocketAddress localAddress();

    /**
     * Gets address of remote peer on this session.
     *
     * @return Address of remote peer or {@code null} if non-socket communication is used.
     */
    @Nullable public InetSocketAddress remoteAddress();

    /**
     * Gets the total count of bytes sent since the session was created.
     *
     * @return Total count of bytes sent.
     */
    public long bytesSent();

    /**
     * Gets the total count of bytes received since the session was created.
     *
     * @return Total count of bytes received.
     */
    public long bytesReceived();

    /**
     * Gets the time when the session was created.
     *
     * @return Time when this session was created returned by {@link System#currentTimeMillis()}.
     */
    public long createTime();

    /**
     * If session is closed, this method will return session close time returned by {@link System#currentTimeMillis()}.
     * If session is not closed, this method will return {@code 0}.
     *
     * @return Session close time.
     */
    public long closeTime();

    /**
     * Returns the time when last read activity was performed on this session.
     *
     * @return Lats receive time.
     */
    public long lastReceiveTime();

    /**
     * Returns time when last send activity was performed on this session.
     *
     * @return Last send time.
     */
    public long lastSendTime();

    /**
     * Returns time when last send was scheduled on this session.
     *
     * @return Last send schedule time.
     */
    public long lastSendScheduleTime();

    /**
     * Performs a request for asynchronous session close.
     *
     * @return Future representing result.
     */
    public GridNioFuture<Boolean> close();

    /**
     * Performs a request for asynchronous data send.
     *
     * @param msg Message to be sent. This message will be eventually passed in to a parser plugged
     *            to the nio server.
     * @return Future representing result.
     */
    public GridNioFuture<?> send(Object msg);

    /**
     * Gets metadata associated with specified key.
     *
     * @param key Key to look up.
     * @return Associated meta object or {@code null} if meta was not found.
     */
    @Nullable public <T> T meta(int key);

    /**
     * Adds metadata associated with specified key.
     *
     * @param key Metadata Key.
     * @param val Metadata value.
     * @return Previously associated object or {@code null} if no objects were associated.
     */
    @Nullable public <T> T addMeta(int key, @Nullable T val);

    /**
     * Removes metadata with the specified key.
     *
     * @param key Metadata key.
     * @return Object that was associated with the key or {@code null}.
     */
    @Nullable public <T> T removeMeta(int key);

    /**
     * @return {@code True} if this connection was initiated from remote node.
     */
    public boolean accepted();

    /**
     * Resumes session reads.
     *
     * @return Future representing result.
     */
    public GridNioFuture<?> resumeReads();

    /**
     * Pauses reads.
     *
     * @return Future representing result.
     */
    public GridNioFuture<?> pauseReads();

    /**
     * Checks whether reads are paused.
     *
     * @return {@code True} if reads are paused.
     */
    public boolean readsPaused();

    /**
     * @param recoveryDesc Recovery descriptor.
     */
    public void recoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc);

    /**
     * @return Recovery descriptor if recovery is supported, {@code null otherwise.}
     */
    @Nullable public GridNioRecoveryDescriptor recoveryDescriptor();
}
