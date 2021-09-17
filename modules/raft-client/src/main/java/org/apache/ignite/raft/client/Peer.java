/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.NetworkAddress;

/**
 * A participant of a replication group.
 */
// TODO: IGNITE-15506 Replace it by jraft Peer
public final class Peer implements Serializable {
    /**
     * Network address.
     */
    private final NetworkAddress addr;

    /**
     * Peer's local priority value, if node don't support priority election,
     * this value is {@link ElectionPriority#DISABLED}.
     */
    private final int priority;

    /**
     * @param peer Peer.
     */
    public Peer(Peer peer) {
        this.addr = peer.address();
        this.priority = peer.getPriority();
    }

    /**
     * @param addr The address.
     */
    public Peer(NetworkAddress addr) {
        this(addr, ElectionPriority.DISABLED);
    }

    /**
     * @param addr The address.
     * @param priority Election priority.
     */
    public Peer(NetworkAddress addr, int priority) {
        this.addr = addr;
        this.priority = priority;
    }

    /**
     * @return The address.
     */
    public NetworkAddress address() {
        return this.addr;
    }

    /**
     * @return Election priority.
     */
    public int getPriority() {
        return priority;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Peer peer = (Peer)o;
        return priority == peer.priority && addr.equals(peer.addr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(addr, priority);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Peer.class, this);
    }
}
