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

    /**
     * Constructor.
     *
     * @param creatorNodeId Creator node ID.
     * @param addr Coordinator address.
     */
    public TcpDiscoveryAuthFailedMessage(UUID creatorNodeId, InetAddress addr) {
        super(creatorNodeId);

        this.addr = addr;
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