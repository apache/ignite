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

package org.apache.ignite.internal.processors.rest.client.message;

import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridMemcachedMessage.IGNITE_REQ_FLAG;

/**
 * Fictive ping packet.
 */
public class GridClientPingPacket extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ping message. */
    public static final GridClientMessage PING_MESSAGE = new GridClientPingPacket();

    /** Ping packet. */
    public static final byte[] PING_PACKET = new byte[] {IGNITE_REQ_FLAG, 0x00, 0x00, 0x00, 0x00};

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getName();
    }
}