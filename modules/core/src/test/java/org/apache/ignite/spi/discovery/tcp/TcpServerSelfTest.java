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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import junit.framework.TestCase;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.ssl.BlockingSslHandler;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Simple tests for {@link ServerImpl.TcpServer}.
 */
public class TcpServerSelfTest extends TestCase {
    /** */
    private SSLContext sslCtx;

    /** */
    @Override protected void setUp() throws Exception {
        super.setUp();

        sslCtx = GridTestUtils.sslContext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBlockingSslHandler() throws Exception {
        LinkedBlockingQueue<Byte> buf1 = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Byte> buf2 = new LinkedBlockingQueue<>();

        NullLogger log = new NullLogger();

        SSLEngine sslEngine1 = sslCtx.createSSLEngine();
        sslEngine1.setUseClientMode(false);

        SSLEngine sslEngine2 = sslCtx.createSSLEngine();
        sslEngine2.setUseClientMode(true);

        final BlockingSslHandler h1 = new TestSslHandler(sslEngine1, true, log, buf1, buf2);
        final BlockingSslHandler h2 = new TestSslHandler(sslEngine2, true, log, buf2, buf1);

        IgniteInternalFuture handshakeFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception{
                h1.handshake();

                return null;
            }
        }, "handshake");

        h2.handshake();

        Random random = new Random();

        for (int iter = 0; iter < 50; iter++) {
            int size = random.nextInt(65535);

            final byte[] data = new byte[size];
            byte[] data2 = new byte[size];

            random.nextBytes(data);

            GridTestUtils.runAsync(new Callable() {
                @Override public Void call() throws Exception {
                    h1.outputStream().write(data);

                    return null;
                }
            }, "write");

            h2.inputStream().read(data2);

            for (int i = 0; i < size; i++)
                assertEquals("bad data in pos " + i, data[i], data2[i]);
        }

        handshakeFut.get();
    }
    /**
     *
     */
    static class TestSslHandler extends BlockingSslHandler {
        /** */
        final LinkedBlockingQueue<Byte> in;

        /** */
        final LinkedBlockingQueue<Byte> out;

        /**
         * @param sslEngine Engine.
         * @param directBuf Direct buffer flag.
         * @param log Logger.
         * @param in In data queue.
         * @param out Out data queue.
         */
        public TestSslHandler(SSLEngine sslEngine,
            boolean directBuf,
            IgniteLogger log,
            LinkedBlockingQueue<Byte> in,
            LinkedBlockingQueue<Byte> out) {
            super(sslEngine, null, directBuf, ByteOrder.nativeOrder(), log);
            this.in = in;
            this.out = out;
        }

        /** {@inheritDoc} */
        @Override protected int doRead(ByteBuffer inBuf) throws IOException {
            int n = Math.min(in.size(), inBuf.remaining());

            for (int i = 0; i < n; i++)
                inBuf.put(in.poll());

            return n;
        }

        /** {@inheritDoc} */
        @Override protected int doWrite(ByteBuffer buf) throws IOException {
            int cnt = 0;

            while (buf.hasRemaining())
                out.add(buf.get());

            return cnt;
        }
    }
}