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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Message telling joining node that its authentication failed on coordinator.
 */
public class TcpDiscoveryAuthFailedMessage extends TcpDiscoveryAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Coordinator address. */
    private transient InetAddress addr;

    /** Node id for which authentication was failed. */
    private UUID targetNodeId;

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param addr Coordinator address.
     * @param targetNodeId Node for which authentication was failed.
     */
    public TcpDiscoveryAuthFailedMessage(UUID creatorNodeId, InetAddress addr, UUID targetNodeId) {
        super(creatorNodeId);

        this.addr = addr;
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return Node for which authentication was failed.
     */
    public UUID getTargetNodeId() {
        return targetNodeId;
    }

    /**
     * @return Coordinator address.
     */
    public InetAddress address() {
        return addr;
    }

    /**
     * Serialize this message.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        U.writeByteArray(out, addr.getAddress());
    }

    /**
     * Deserialize this message.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        addr = InetAddress.getByAddress(U.readByteArray(in));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryAuthFailedMessage.class, this, "super", super.toString());
    }
}