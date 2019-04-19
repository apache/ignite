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

package org.apache.ignite.internal.util.nio;

import java.net.Socket;
import java.nio.ByteOrder;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for new NIO server with SSL enabled.
 */
public class GridNioSslSelfTest extends GridNioSelfTest {
    /** Test SSL context. */
    private static SSLContext sslCtx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sslCtx = GridTestUtils.sslContext();
    }

    /** {@inheritDoc} */
    @Override protected Socket createSocket() throws IgniteCheckedException {
        try {
            return sslCtx.getSocketFactory().createSocket();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected GridNioServer.Builder<?> serverBuilder(int port,
        GridNioParser parser,
        GridNioServerListener lsnr)
        throws Exception
    {
        return GridNioServer.builder()
            .address(U.getLocalHost())
            .port(port)
            .listener(lsnr)
            .logger(log)
            .selectorCount(2)
            .igniteInstanceName("nio-test-grid")
            .tcpNoDelay(false)
            .directBuffer(true)
            .byteOrder(ByteOrder.nativeOrder())
            .socketSendBufferSize(0)
            .socketReceiveBufferSize(0)
            .sendQueueLimit(0)
            .filters(
                new GridNioCodecFilter(parser, log, false),
                new GridNioSslFilter(sslCtx, true, ByteOrder.nativeOrder(), log));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testWriteTimeout() throws Exception {
        // Skip base test because it enables "skipWrite" mode in the GridNioServer
        // which makes SSL handshake impossible.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testAsyncSendReceive() throws Exception {
        // No-op, do not want to mess with SSL channel.
    }
}
