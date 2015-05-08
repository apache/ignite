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

package org.apache.ignite.internal.processors.rest.protocols.tcp;

import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.nio.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Mock nio session with disabled functionality for testing parser.
 */
public class MockNioSession extends GridMetadataAwareAdapter implements GridNioSession {
    /** Local address */
    private InetSocketAddress locAddr = new InetSocketAddress(0);

    /** Remote address. */
    private InetSocketAddress rmtAddr = new InetSocketAddress(0);

    /**
     * Creates empty mock session.
     */
    public MockNioSession() {
        // No-op.
    }

    /**
     * Creates new mock session with given addresses.
     *
     * @param locAddr Local address.
     * @param rmtAddr Remote address.
     */
    public MockNioSession(InetSocketAddress locAddr, InetSocketAddress rmtAddr) {
        this();

        this.locAddr = locAddr;
        this.rmtAddr = rmtAddr;
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress localAddress() {
        return locAddr;
    }

    /** {@inheritDoc} */
    @Override public InetSocketAddress remoteAddress() {
        return rmtAddr;
    }

    /** {@inheritDoc} */
    @Override public long bytesSent() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long bytesReceived() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long closeTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long lastReceiveTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long lastSendTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long lastSendScheduleTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> close() {
        return new GridNioFinishedFuture<>(true);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> send(Object msg) {
        return new GridNioFinishedFuture<>(true);
    }

    /** {@inheritDoc} */
    @Override public <T> T meta(int key) {
        return meta(new UUID(key, key));
    }

    /** {@inheritDoc} */
    @Override public <T> T addMeta(int key, T val) {
        return addMeta(new UUID(key, key), val);
    }

    /** {@inheritDoc} */
    @Override public <T> T removeMeta(int key) {
        return removeMeta(new UUID(key, key));
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Object> resumeReads() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Object> pauseReads() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean accepted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readsPaused() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void recoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNioRecoveryDescriptor recoveryDescriptor() {
        return null;
    }
}
