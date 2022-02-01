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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import java.nio.ByteBuffer;

import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

/**
 * Client event listener.
 */
class GridNioClientListener implements GridNioServerListener<ByteBuffer> {
    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        GridNioClientConnection conn = ses.meta(GridNioClientConnection.SES_META_CONN);

        // Conn can be null when connection fails during initialization in open method.
        if (conn != null)
            conn.onDisconnected(e);
    }

    /** {@inheritDoc} */
    @Override public void onMessageSent(GridNioSession ses, ByteBuffer msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, ByteBuffer msg) {
        GridNioClientConnection conn = ses.meta(GridNioClientConnection.SES_META_CONN);

        assert conn != null : "Session must have an associated connection";

        conn.onMessage(msg);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onFailure(FailureType failureType, Throwable failure) {
        // No-op.
    }
}
