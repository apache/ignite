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

import java.net.InetAddress;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Socket address utility container message. Is not a pure {@link TcpDiscoveryAbstractMessage}. */
public class InetSocketAddressMessage extends InetAddressMessage {
    /** */
    @Order(2)
    private int port;

    /**
     * Default constructor for {@link DiscoveryMessageFactory}.
     */
    public InetSocketAddressMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param addr Address.
     * @param port Port.
     */
    public InetSocketAddressMessage(InetAddress addr, int port) {
        super(addr);

        this.port = port;
    }

    /** @return Port. */
    public int port() {
        return port;
    }

    /** @param port Port. */
    public void port(int port) {
        this.port = port;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -100;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InetSocketAddressMessage.class, this);
    }
}
