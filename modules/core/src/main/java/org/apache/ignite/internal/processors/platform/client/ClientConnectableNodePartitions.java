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

package org.apache.ignite.internal.processors.platform.client;

import java.util.Collection;
import org.apache.ignite.binary.BinaryRawWriter;

/**
 * Address of the node, connectible for the thin client, associated with cache partitions info.
 */
public class ClientConnectableNodePartitions {
    /** Client listener port */
    private final int port;

    /** Addresses. */
    private final Collection<String> addrs;

    /** Cache partitions. */
    private final int[] parts;

    /**
     * @param port Client listener port.
     * @param addrs Node addresses.
     * @param parts Partitions.
     */
    public ClientConnectableNodePartitions(int port, Collection<String> addrs, int[] parts) {
        this.port = port;
        this.addrs = addrs;
        this.parts = parts;
    }

    /**
     * @return Client listener port of the node.
     */
    public int getPort() {
        return port;
    }

    /**
     * @return Node's addresses.
     */
    public Collection<String> getAddress() {
        return addrs;
    }

    /**
     * @return Cache partitions mapped to the node.
     */
    public int[] getPartitions() {
        return parts;
    }

    /**
     * Write using writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        writer.writeInt(port);

        writer.writeInt(addrs.size());
        for (String addr : addrs)
            writer.writeString(addr);

        writer.writeInt(parts.length);
        for (int part : parts)
            writer.writeInt(part);
    }
}
