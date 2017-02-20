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

import java.net.InetSocketAddress;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

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
     * @param msg Message to be sent.
     */
    public void sendNoFuture(Object msg) throws IgniteCheckedException;

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
    public void outRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc);

    /**
     * @param recoveryDesc Recovery descriptor.
     */
    public void inRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc);

    /**
     * @return Recovery descriptor if recovery is supported, {@code null otherwise.}
     */
    @Nullable public GridNioRecoveryDescriptor outRecoveryDescriptor();

    /**
     * @return Recovery descriptor if recovery is supported, {@code null otherwise.}
     */
    @Nullable public GridNioRecoveryDescriptor inRecoveryDescriptor();

    /**
     * @param msg System message to send.
     */
    public void systemMessage(Object msg);
}