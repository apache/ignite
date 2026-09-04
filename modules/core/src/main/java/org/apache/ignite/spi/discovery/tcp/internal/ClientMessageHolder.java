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

package org.apache.ignite.spi.discovery.tcp.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jetbrains.annotations.Nullable;

/** */
public class ClientMessageHolder {
    /** */
    private final TcpDiscoveryAbstractMessage msg;

    /** */
    private byte[] msgBytes;

    /** */
    public ClientMessageHolder(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        this.msg = msg;
    }

    /** */
    public TcpDiscoveryAbstractMessage message() {
        return msg;
    }

    /** */
    public synchronized byte @Nullable [] messageBytes() {
        return msgBytes;
    }

    /** */
    public synchronized void serialize(TcpDiscoveryMessageSerializer ser) throws IgniteCheckedException {
        if (msgBytes == null)
            msgBytes = ser.serialize(msg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return msg.toString();
    }
}
