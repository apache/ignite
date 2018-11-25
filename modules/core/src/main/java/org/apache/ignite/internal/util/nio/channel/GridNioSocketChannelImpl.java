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

package org.apache.ignite.internal.util.nio.channel;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

/**
 *
 */
public class GridNioSocketChannelImpl implements GridNioSocketChannel {
    /** */
    private ConnectionKey key;

    /** */
    private SocketChannel channel;

    /** */
    private GridNioSocketChannelConfig cfg;



    /** {@inheritDoc} */
    @Override public ConnectionKey connectionKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SocketChannel channel() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridNioSocketChannelConfig configuration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isOpen() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isActive() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isWritable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInputShutdown() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> shutdownInput() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isOutputShutdown() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> shutdownOutput() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> closeFuture() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {

    }
}
