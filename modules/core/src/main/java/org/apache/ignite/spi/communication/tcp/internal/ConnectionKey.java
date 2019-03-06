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
