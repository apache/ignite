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

import java.io.Closeable;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

/**
 * Communication TCP/IP socket.
 */
public interface GridNioSocketChannel extends Closeable {
    /** */
    public ConnectionKey id();

    /** */
    public SocketChannel channel();

    /** */
    public GridNioSocketChannelConfig configuration();

    /** */
    public boolean isOpen();

    /** */
    public boolean isActive();

    /**
     * @see Socket#isInputShutdown().
     */
    public boolean isInputShutdown();

    /**
     * @see Socket#shutdownInput()
     */
    public GridNioFuture<Boolean> shutdownInput();

    /**
     * @see Socket#isOutputShutdown()
     */
    public boolean isOutputShutdown();

    /**
     * @see Socket#shutdownOutput()
     */
    public GridNioFuture<Boolean> shutdownOutput();

    /** */
    public GridNioFuture<Boolean> closeFuture();
}
