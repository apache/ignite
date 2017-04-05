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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MAX_KEYS_CNT;

/**
 *
 */
public class GridNioSessionImpl implements GridNioSession {
    /** Metadata "map". */
    private final Object[] meta = new Object[MAX_KEYS_CNT];

    /** Local connection address. */
    private final InetSocketAddress locAddr;

    /** Remote connection address */
    private final InetSocketAddress rmtAddr;

    /** Session create timestamp. */
    private long createTime;

    /** Session close timestamp. */
    private final AtomicLong closeTime = new AtomicLong();

    /** Sent bytes counter. */
    private volatile long bytesSent;

    /** Received bytes counter. */
    private volatile long bytesRcvd;

    /** Sent bytes since last NIO sessions balancing. */
    private volatile long bytesSent0;

    /** Received bytes since last NIO sessions balancing. */
    private volatile long bytesRcvd0;

    /** Last send schedule timestamp. */
    private volatile long sndSchedTime;

    /** Last send activity timestamp. */
    private volatile long lastSndTime;

    /** Last read activity timestamp. */
    private volatile long lastRcvTime;

    /** Reads paused flag. */
    private volatile boolean readsPaused;

    /** Filter chain that will handle write and close requests. */
    private GridNioFilterChain filterChain;

    /** Accepted flag. */
    private final boolean accepted;

    /**
     * @param filterChain Chain.
     * @param locAddr Local address.
     * @param rmtAddr Remote address.
     * @param accepted {@code True} if this session was initiated from remote host.
     */
    public GridNioSessionImpl(
        GridNioFilterChain filterChain,
        @Nullable InetSocketAddress locAddr,
        @Nullable InetSocketAddress rmtAddr,
        boolean accepted
    ) {
        this.filterChain = filterChain;
        this.locAddr = locAddr;
        this.rmtAddr = rmtAddr;
        this.accepted = accepted;

        long now = U.currentTimeMillis();

        sndSchedTime = now;
        createTime = now;
        lastSndTime = now;
        lastRcvTime = now;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> send(Object msg) {
        try {
            resetSendScheduleTime();

            return chain().onSessionWrite(this, msg, true);
        }
        catch (IgniteCheckedException e) {
            close();

            return new GridNioFinishedFuture<Object>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void sendNoFuture(Object msg) throws IgniteCheckedException {
        try {
            chain().onSessionWrite(this, msg, false);
        }
        catch (IgniteCheckedException e) {
            close();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> resumeReads() {
        try {
            return chain().onResumeReads(this);
        }
        catch (IgniteCheckedException e) {
            close();

            return new GridNioFinishedFuture<Object>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> pauseReads() {
        try {
            return chain().onPauseReads(this);
        }
        catch (IgniteCheckedException e) {
            close();

            return new GridNioFinishedFuture<Object>(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridNioFuture<Boolean> close() {
        try {
            return filterChain.onSessionClose(this);
        }
        catch (IgniteCheckedException e) {
            return new GridNioFinishedFuture<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public InetSocketAddress localAddress() {
        return locAddr;
    }

    /** {@inheritDoc} */
    @Override  @Nullable public InetSocketAddress remoteAddress() {
        return rmtAddr;
    }

    /** {@inheritDoc} */
    @Override public long bytesSent() {
        return bytesSent;
    }

    /** {@inheritDoc} */
    @Override public long bytesReceived() {
        return bytesRcvd;
    }

    /**
     * @return Sent bytes since last NIO sessions balancing.
     */
    public long bytesSent0() {
        return bytesSent0;
    }

    /**
     * @return Received bytes since last NIO sessions balancing.
     */
    public long bytesReceived0() {
        return bytesRcvd0;
    }

    /**
     *
     */
    public void reset0() {
        bytesSent0 = 0;
        bytesRcvd0 = 0;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long closeTime() {
        return closeTime.get();
    }

    /** {@inheritDoc} */
    @Override public long lastReceiveTime() {
        return lastRcvTime;
    }

    /** {@inheritDoc} */
    @Override public long lastSendTime() {
        return lastSndTime;
    }

    /** {@inheritDoc} */
    @Override public long lastSendScheduleTime() {
        return sndSchedTime;
    }

    /** {@inheritDoc} */
    @Override public <T> T meta(int key) {
        assert key < meta.length;

        return (T)meta[key];
    }

    /** {@inheritDoc} */
    @Override public <T> T addMeta(int key, @Nullable T val) {
        assert key < meta.length;

        Object prev = meta[key];

        meta[key] = val;

        return (T)prev;
    }

    /** {@inheritDoc} */
    @Override public <T> T removeMeta(int key) {
        assert key < meta.length;

        Object prev = meta[key];

        meta[key] = null;

        return (T)prev;
    }

    /** {@inheritDoc} */
    @Override public boolean accepted() {
        return accepted;
    }

    /**
     * @param <T> Chain type.
     * @return Filter chain.
     */
    @SuppressWarnings("unchecked")
    protected <T> GridNioFilterChain<T> chain() {
        return filterChain;
    }

    /**
     * Adds given amount of bytes to the sent bytes counter.
     * <p>
     * Note that this method is designed to be called in one thread only.
     *
     * @param cnt Number of bytes sent.
     */
    public void bytesSent(int cnt) {
        bytesSent += cnt;
        bytesSent0 += cnt;

        lastSndTime = U.currentTimeMillis();
    }

    /**
     * Adds given amount ob bytes to the received bytes counter.
     * <p>
     * Note that this method is designed to be called in one thread only.
     *
     * @param cnt Number of bytes received.
     */
    public void bytesReceived(int cnt) {
        bytesRcvd += cnt;
        bytesRcvd0 += cnt;

        lastRcvTime = U.currentTimeMillis();
    }

    /**
     * Resets send schedule time to avoid multiple idle notifications.
     */
    public void resetSendScheduleTime() {
        sndSchedTime = U.currentTimeMillis();
    }

    /**
     * Atomically moves this session into a closed state.
     *
     * @return {@code True} if session was moved to a closed state,
     *      {@code false} if session was already closed.
     */
    public boolean setClosed() {
        return closeTime.compareAndSet(0, U.currentTimeMillis());
    }

    /**
     * @return {@code True} if this session was closed.
     */
    public boolean closed() {
        return closeTime.get() != 0;
    }

    /**
     * @param readsPaused New reads paused flag.
     */
    public void readsPaused(boolean readsPaused) {
        this.readsPaused = readsPaused;
    }

    /**
     * @return Reads paused flag.
     */
    @Override public boolean readsPaused() {
        return readsPaused;
    }

    /** {@inheritDoc} */
    @Override public void outRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNioRecoveryDescriptor outRecoveryDescriptor() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void inRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNioRecoveryDescriptor inRecoveryDescriptor() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void systemMessage(Object msg) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioSessionImpl.class, this);
    }
}
