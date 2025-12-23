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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Address utility container message. Is not a pure {@link TcpDiscoveryAbstractMessage}. */
public class InetAddressMessage implements Message {
    /** */
    @Order(0)
    private String hostName;

    /** */
    @Order(value = 1, method = "addressBytes")
    private byte[] addrBytes;

    /** Default constructor for {@link DiscoveryMessageFactory}. */
    public InetAddressMessage() {
        // No-op.
    }

    /** @param addr Address. */
    public InetAddressMessage(InetAddress addr) {
        hostName = addr.getHostName();
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

    /** @return {@link InetAddress#getByAddress(String, byte[])} */
    public InetAddress address() {
        try {
            return addrBytes == null ? null : InetAddress.getByAddress(hostName, addrBytes);
        }
        catch (UnknownHostException e) {
            throw new IgniteException("Failed to read host address.", e);
        }
    }

    /** @return Host name. */
    public String hostName() {
        return hostName;
    }

    /** @param hostName Host name. */
    public void hostName(String hostName) {
        this.hostName = hostName;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -101;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InetAddressMessage.class, this);
    }
}
