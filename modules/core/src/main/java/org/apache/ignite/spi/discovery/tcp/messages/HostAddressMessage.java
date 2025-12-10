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
import java.net.UnknownHostException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class HostAddressMessage implements Message {
    /** */
    @Order(0)
    private String hostName;

    /** */
    @Order(1)
    private int port;

    /** */
    @Order(value = 2, method = "addressBytes")
    private byte[] addrBytes;

    /**
     * Default constructor for {@link DiscoveryMessageFactory}.
     */
    public HostAddressMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param addr Address.
     * @param port Port.
     */
    public HostAddressMessage(InetAddress addr, int port) {
        hostName = addr.getHostName();
        this.port = port;
        addrBytes = addr.getAddress();
    }

    /** @return {@link InetAddress#getAddress()} */
    public byte[] addressBytes() {
        return addrBytes;
    }

    /** @param addrBytes {@link InetAddress#getAddress()} */
    public void addressBytes(byte[] addrBytes) {
        this.addrBytes = addrBytes;
    }

    /** @return port. */
    public int port() {
        return port;
    }

    /** @param port port. */
    public void port(int port) {
        this.port = port;
    }

    /** @return Host name. */
    public String hostName() {
        return hostName;
    }

    /** @param hostName Host name. */
    public void hostName(String hostName) {
        this.hostName = hostName;
    }

    /** @return {@link InetAddress#getByAddress(byte[])} */
    public InetAddress address() throws UnknownHostException {
        return addrBytes == null ? null : InetAddress.getByAddress(hostName, addrBytes);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -100;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HostAddressMessage.class, this);
    }
}
