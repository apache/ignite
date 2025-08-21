/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.DiscoverySpiListenerWrapper.wrap;

/**
 *
 */
public class TestTcpDiscoverySpi extends TcpDiscoverySpi {
    /** */
    public boolean ignorePingResponse;

    /** Interceptor of discovery messages. */
    private DiscoveryHook discoHook;

    /** */
    private IgniteDiscoverySpiInternalListener internalLsnr;

    /** {@inheritDoc} */
    @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
        IgniteCheckedException {
        if (msg instanceof TcpDiscoveryPingResponse && ignorePingResponse)
            return;

        if (internalLsnr != null) {
            if (msg instanceof TcpDiscoveryJoinRequestMessage)
                internalLsnr.beforeJoin(locNode, log);

            if (msg instanceof TcpDiscoveryClientReconnectMessage)
                internalLsnr.beforeReconnect(locNode, log);
        }

        super.writeToSocket(sock, out, msg, timeout);
    }

    /** {@inheritDoc} */
    @Override public void simulateNodeFailure() {
        super.simulateNodeFailure();
    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        super.setListener(lsnr == null || discoHook == null ? lsnr : wrap(lsnr, discoHook));
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null) {
            if (!internalLsnr.beforeSendCustomEvent(this, log, msg))
                return;
        }

        super.sendCustomEvent(msg);
    }

    /** */
    public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
        internalLsnr = lsnr;
    }

    /**
     * Sets interceptor of discovery messages. Note that {@link DiscoveryHook} must be set before SPI start.
     * Otherwise, this method call will take no effect.
     *
     * @param discoHook Interceptor of discovery messages.
     */
    public void discoveryHook(DiscoveryHook discoHook) {
        assert !started();

        this.discoHook = discoHook;
    }
}
