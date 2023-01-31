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

package org.apache.ignite.client.monitoring;

import java.net.InetSocketAddress;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/** */
public class ConnectionDescription {
    /** Local connection address. */
    private final InetSocketAddress locAddr;

    /** Remote connection address. */
    private final InetSocketAddress rmtAddr;

    /** Server node id. */
    private final UUID srvNodeId;

    /** */
    private String protocol;

    /**
     * @param locAddr Local connection address.
     * @param rmtAddr Remote connection address.
     * @param protocol String representation of a connection protocol details.
     * @param srvNodeId Server node id.
     */
    public ConnectionDescription(InetSocketAddress locAddr, InetSocketAddress rmtAddr, String protocol, UUID srvNodeId) {
        this.locAddr = locAddr;
        this.rmtAddr = rmtAddr;
        this.protocol = protocol;
        this.srvNodeId = srvNodeId;
    }

    /**
     * Gets local address of this connection.
     *
     * @return Local network address or {@code null} if non-socket communication is used.
     */
    @Nullable public InetSocketAddress localAddress() {
        return locAddr;
    }

    /**
     * Gets address of remote peer of this connection.
     *
     * @return Address of remote peer or {@code null} if non-socket communication is used.
     */
    @Nullable public InetSocketAddress remoteAddress() {
        return rmtAddr;
    }

    /**
     * @return Server node id.
     */
    @Nullable public UUID serverNodeId() {
        return srvNodeId;
    }

    /**
     * @return String representation of connection protocol.
     */
    @Nullable public String protocol() {
        return protocol;
    }
}
