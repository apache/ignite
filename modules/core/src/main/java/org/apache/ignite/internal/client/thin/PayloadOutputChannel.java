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

package org.apache.ignite.internal.client.thin;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryMemoryAllocator;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Thin client payload output channel.
 */
class PayloadOutputChannel implements AutoCloseable {
    /** Initial output stream buffer capacity. */
    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    /** Client channel. */
    private final ClientChannel ch;

    /** Output stream. */
    private final BinaryHeapOutputStream out;

    /** Close guard. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Constructor.
     */
    PayloadOutputChannel(ClientChannel ch) {
        // Disable AutoCloseable on the stream so that out callers don't release the pooled buffer before it is written to the socket.
        out = new BinaryHeapOutputStream(INITIAL_BUFFER_CAPACITY, BinaryMemoryAllocator.POOLED.chunk(), true);
        this.ch = ch;
    }

    /**
     * Gets client channel.
     */
    public ClientChannel clientChannel() {
        return ch;
    }

    /**
     * Gets output stream.
     */
    public BinaryOutputStream out() {
        return out;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Pooled buffer is reusable and should be released once and only once.
        // Releasing more than once potentially "steals" it from another request.
        if (closed.compareAndSet(false, true))
            out.release();
    }
}
