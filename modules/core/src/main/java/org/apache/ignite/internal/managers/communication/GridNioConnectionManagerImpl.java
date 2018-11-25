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

package org.apache.ignite.internal.managers.communication;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionKey;

/**
 *
 */
public class GridNioConnectionManagerImpl implements GridNioConnectionManager {
    /** */
    private final Map<ConnectionKey, SocketChannel> channels = new ConcurrentHashMap<>();

    /** */
    private List<GridNioConnectionListener> connLsnrs;

    /** {@inheritDoc} */
    @Override public void addChannel(ConnectionKey key, SocketChannel sock) {
        assert key != null && sock != null : "An error with connection key: " + key;

        channels.putIfAbsent(key, sock);

        for (GridNioConnectionListener lsnr : connLsnrs)
            lsnr.onConnect(key.nodeId(), sock);
    }

    /** {@inheritDoc} */
    @Override public SocketChannel getChannel(ConnectionKey key) {
        return channels.get(key);
    }

    /** {@inheritDoc} */
    @Override public void closeChannel(ConnectionKey key) throws IgniteCheckedException {
        SocketChannel sock = channels.get(key);

        try {
            sock.close();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdown() throws IgniteCheckedException {
        for (ConnectionKey key : channels.keySet())
            closeChannel(key);
    }
}
