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

package org.apache.ignite.internal.client;

import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Thin client payload input channel.
 */
public class PayloadInputChannel implements AutoCloseable {
    /** Client channel. */
    private final ClientChannel ch;

    /** Input stream. */
    private final ClientMessageUnpacker in;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param in Unpacker.
     */
    PayloadInputChannel(ClientChannel ch, ClientMessageUnpacker in) {
        this.in = in;
        this.ch = ch;
    }

    /**
     * Gets client channel.
     *
     * @return Client channel.
     */
    public ClientChannel clientChannel() {
        return ch;
    }

    /**
     * Gets the unpacker.
     *
     * @return Unpacker.
     */
    public ClientMessageUnpacker in() {
        return in;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        in.close();
    }
}
