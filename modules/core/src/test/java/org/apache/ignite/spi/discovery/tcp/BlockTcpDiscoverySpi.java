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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;

import static org.junit.Assert.assertNotNull;

/**
 * Custom discovery SPI allowing to block custom messages transfer between nodes.
 */
public class BlockTcpDiscoverySpi extends TestTcpDiscoverySpi {
    /** Closure. */
    private volatile IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo;

    /**
     * @param clo Closure.
     */
    public void setClosure(IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo) {
        this.clo = clo;
    }

    /**
     * @param addr Address.
     * @param msg Message.
     */
    private synchronized void apply(ClusterNode addr, TcpDiscoveryAbstractMessage msg) {
        if (!(msg instanceof TcpDiscoveryCustomEventMessage))
            return;

        TcpDiscoveryCustomEventMessage msg0 = (TcpDiscoveryCustomEventMessage)msg;

        try {
            msg0.finishUnmarhal(marshaller(), U.gridClassLoader());

            assertNotNull(msg0.message());
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        if (clo != null)
            clo.apply(addr, GridTestUtils.unwrap(msg0.message()));
    }

    /** {@inheritDoc} */
    @Override protected void writeToSocket(
        Socket sock,
        TcpDiscoveryAbstractMessage msg,
        byte[] data,
        long timeout
    ) throws IOException {
        if (spiCtx != null)
            apply(spiCtx.localNode(), msg);

        super.writeToSocket(sock, msg, data, timeout);
    }

    /** {@inheritDoc} */
    @Override protected void writeMessage(TcpDiscoveryIoSession ses,
        TcpDiscoveryAbstractMessage msg,
        long timeout) throws IOException, IgniteCheckedException {
        if (spiCtx != null)
            apply(spiCtx.localNode(), msg);

        super.writeMessage(ses, msg, timeout);
    }
}
