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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Registers a connected {@link SocketChannel} with a {@link Selector} while an NIO session is being
 * established.
 *
 * <p>Intended for testing only (the interface lives in the {@code internal} package and is not part
 * of the product API): a registrar may be installed via the {@code GridNioServer#setChannelRegistrar}
 * method to intercept the registration, for example to close the channel before or stall the
 * registration in a test. The production path never installs a registrar and registers the channel
 * directly.</p>
 *
 * <p>The interface is public because the tests that use it live in a different build module, where
 * package-private access to this module is not possible.</p>
 */
@FunctionalInterface
public interface NioChannelRegistrar {
    /**
     * @param channel Channel to register.
     * @param selector Target selector.
     * @param ops Interest operations.
     * @param attachment Attachment passed on to {@link SocketChannel#register(Selector, int, Object)}.
     * @return Selection key of the registered channel.
     * @throws IOException If the registration failed.
     */
    SelectionKey register(SocketChannel channel, Selector selector, int ops, Object attachment) throws IOException;
}
