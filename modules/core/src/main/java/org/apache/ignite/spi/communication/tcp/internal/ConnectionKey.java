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

package org.apache.ignite.spi.communication.tcp.internal;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Connection Key.
 */
public class ConnectionKey {
    /** */
    private final UUID nodeId;

    /** */
    private final int idx;

    /** */
    private final long connCnt;

    /** */
    private final boolean dummy;

    /**
     * @param nodeId Node ID. Should be not null.
     * @param idx Connection index.
     */
    public ConnectionKey(@NotNull UUID nodeId, int idx) {
        this(nodeId, idx, -1, false);
    }

    /**
     * Creates ConnectionKey with false value of dummy flag.
     *
     * @param nodeId Node ID. Should be not null.
     * @param idx Connection index.
     * @param connCnt Connection counter (set only for incoming connections).
     */
    public ConnectionKey(@NotNull UUID nodeId, int idx, long connCnt) {
        this(nodeId, idx, connCnt, false);
    }

    /**
     * @param nodeId Node ID. Should be not null.
     * @param idx Connection index.
     * @param connCnt Connection counter (set only for incoming connections).
     * @param dummy Indicates that session with this ConnectionKey is temporary
     *              (for now dummy sessions are used only for Communication Failure Resolving process).
     */
    public ConnectionKey(@NotNull UUID nodeId, int idx, long connCnt, boolean dummy) {
        this.nodeId = nodeId;
        this.idx = idx;
        this.connCnt = connCnt;
        this.dummy = dummy;
    }

    /**
     * @return Connection counter.
     */
    public long connectCount() {
        return connCnt;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Connection index.
     */
    public int connectionIndex() {
        return idx;
    }

    /**
     * @return {@code True} if this ConnectionKey is dummy and serves temporary session.
     */
    public boolean dummy() {
        return dummy;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConnectionKey key = (ConnectionKey) o;

        return idx == key.idx && nodeId.equals(key.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();
        res = 31 * res + idx;
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConnectionKey.class, this);
    }
}
