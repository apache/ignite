/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    private final Object consistentId;

    /** */
    private final UUID nodeId;

    /** */
    private final int idx;

    /** */
    private final long connCnt;

    /** */
    private final boolean dummy;

    /**
     * Creates ConnectionKey with false value of dummy flag.
     *
     * @param consistentId Consistent id of the node.
     * @param nodeId Node ID. Should be not null.
     * @param idx Connection index.
     * @param connCnt Connection counter (set only for incoming connections).
     */
    public ConnectionKey(@NotNull Object consistentId, @NotNull UUID nodeId, int idx, long connCnt) {
        this(consistentId, nodeId, idx, connCnt, false);
    }

    /**
     * @param consistentId Consistent id of the node.
     * @param nodeId Node ID. Should be not null.
     * @param idx Connection index.
     * @param connCnt Connection counter (set only for incoming connections).
     * @param dummy Indicates that session with this ConnectionKey is temporary
     *              (for now dummy sessions are used only for Communication Failure Resolving process).
     */
    public ConnectionKey(@NotNull Object consistentId, @NotNull UUID nodeId, int idx, long connCnt, boolean dummy) {
        this.consistentId = consistentId;
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

    /** */
    public Object consistentId() {
        return consistentId;
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
