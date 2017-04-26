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

    /** */
    public void testBlockingSslHandler() throws Exception {
        LinkedBlockingQueue<Byte> buf1 = new LinkedBlockingQueue<>(65535);
        LinkedBlockingQueue<Byte> buf2 = new LinkedBlockingQueue<>(65535);

        NullLogger log = new NullLogger();

        SSLEngine sslEngine1 = sslCtx.createSSLEngine();
        sslEngine1.setUseClientMode(false);

        SSLEngine sslEngine2 = sslCtx.createSSLEngine();
        sslEngine2.setUseClientMode(true);

        final TestSocket sock1 = new TestSocket(buf1, buf2);
        final TestSocket sock2 = new TestSocket(buf2, buf1);

        SocketChannel ch1 = new TestChannel(sock1, buf1, buf2);
        SocketChannel ch2 = new TestChannel(sock2, buf2, buf1);

        final BlockingSslHandler h1 = new BlockingSslHandler(sslEngine1, ch1, true, ByteOrder.nativeOrder(), log);
        final BlockingSslHandler h2 = new BlockingSslHandler(sslEngine2, ch2, true, ByteOrder.nativeOrder(), log);

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
     * Test InputStream above BlockingQueue
     */
    private static class QueueInputStream extends InputStream {
        /** */
        private BlockingQueue<Byte> buf;

        /** */
        public QueueInputStream(BlockingQueue<Byte> buf) {
            this.buf = buf;
        }

        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            try {
                Byte res = buf.poll(1, TimeUnit.SECONDS);
                return res != null ? res : -1;
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            int end = offset + length;

            for (int i = offset; i < end; i++) {
                try {
                    bytes[i] = buf.take();
                }
                catch (InterruptedException e) {
                    return i - 1;
                }
            }

            return length;
        }

        /** {@inheritDoc} */
        @Override public int available() throws IOException {
            return buf.size();
        }
    }

    /**
     * Test Output Stream above BlockingQueue
     */
    private static class QueueOutputStream extends OutputStream {
        /**  */
        private BlockingQueue<Byte> buf;

        /**  */
        public QueueOutputStream(BlockingQueue<Byte> buf) {
            this.buf = buf;
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            try {
                buf.put((byte)b);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /** {@inheritDoc} */
        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            int end = offset + length;

            for (int i = offset; i < end; i++) {
                try {
                    buf.put(bytes[i]);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    /**
     * Test channel.
     */
    private static class TestChannel extends SocketChannel {
        /**  */
        private final Socket socket;
        /**  */
        private BlockingQueue<Byte> buf_in;
        /**  */
        private BlockingQueue<Byte> buf_out;

        /**  */
        protected TestChannel(TestSocket socket, BlockingQueue<Byte> buf_in, BlockingQueue<Byte> buf_out) {
            super(null);

            this.socket = socket;
            this.buf_in = buf_in;
            this.buf_out = buf_out;

            socket.channel(this);
        }

        @Override protected void implCloseSelectableChannel() throws IOException {

        }

        @Override protected void implConfigureBlocking(boolean block) throws IOException {

        }

        @Override public SocketChannel bind(SocketAddress local) throws IOException {
            return null;
        }

        @Override public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
            return null;
        }

        @Override public <T> T getOption(SocketOption<T> name) throws IOException {
            return null;
        }

        @Override public Set<SocketOption<?>> supportedOptions() {
            return null;
        }

        @Override public SocketChannel shutdownInput() throws IOException {
            return null;
        }

        @Override public SocketChannel shutdownOutput() throws IOException {
            return null;
        }

        @Override public Socket socket() {
            return socket;
        }

        @Override public boolean isConnected() {
            return true;
        }

        @Override public boolean isConnectionPending() {
            return false;
        }

        @Override public boolean connect(SocketAddress remote) throws IOException {
            return false;
        }

        @Override public boolean finishConnect() throws IOException {
            return false;
        }

        @Override public SocketAddress getRemoteAddress() throws IOException {
            return null;
        }

        @Override public int read(ByteBuffer dst) throws IOException {
            int n = Math.min(buf_in.size(), dst.capacity() - dst.position());

            for (int i = 0; i < n; i++)
                dst.put(buf_in.poll());

            return n;
        }

        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return 0;
        }

        @Override public int write(ByteBuffer src) throws IOException {
            int n = src.remaining();

            for (int i = 0; i < n; i++)
                try {
                    buf_out.put(src.get());
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

            return n;
        }

        @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return 0;
        }

        @Override public SocketAddress getLocalAddress() throws IOException {
            return null;
        }

    }

    /**
     * Test socket.
     */
    private static class TestSocket extends Socket {
        /**  */
        private BlockingQueue<Byte> buf_in;
        /**  */
        private BlockingQueue<Byte> buf_out;
        /**  */
        private SocketChannel ch;

        /**  */
        public TestSocket(BlockingQueue<Byte> buf_in, BlockingQueue<Byte> buf_out) {
            this.buf_in = buf_in;
            this.buf_out = buf_out;
        }

        @Override public InputStream getInputStream() throws IOException {
            return new QueueInputStream(buf_in);
        }

        @Override public OutputStream getOutputStream() throws IOException {
            return new QueueOutputStream(buf_out);
        }

        @Override public boolean isConnected() {
            return true;
        }

        @Override public SocketChannel getChannel() {
            return ch;
        }

        /**
         * Add channel.
         *
         * @param ch Channel.
         */
        public void channel(SocketChannel ch) {
            this.ch = ch;
        }

        @Override public synchronized void setSoTimeout(int timeout) throws SocketException {
            super.setSoTimeout(timeout);
        }
    }

}