/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;

/**
 *
 */
public class TestTcpDiscoverySpi extends TcpDiscoverySpi {
    /** */
    public boolean ignorePingResponse;

    /** {@inheritDoc} */
    @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
        IgniteCheckedException {
        if (msg instanceof TcpDiscoveryPingResponse && ignorePingResponse)
            return;
        else
            super.writeToSocket(sock, out, msg, timeout);
    }

    /** {@inheritDoc} */
    @Override public void simulateNodeFailure() {
        super.simulateNodeFailure();
    }
}
