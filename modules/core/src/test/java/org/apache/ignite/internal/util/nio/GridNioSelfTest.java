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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tests for new NIO server.
 */
public class GridNioSelfTest extends GridCommonAbstractTest {
    /** Test port. */
    private static final int PORT = 55443;

    /** Message count in test without reconnect. */
    private static final int MSG_CNT = 2000;

    /** Message id provider. */
    private static final AtomicInteger idProvider = new AtomicInteger(1);

    /** Message count per thread in reconnect test. This count should not be too large due to TIME_WAIT sock status. */
    private static final int RECONNECT_MSG_CNT = 100;

    /** Thread count. */
    private static final int THREAD_CNT = 5;

    /** Message size. */
    private static final int MSG_SIZE = 1024 * 128;

    /** Count of statistics segments. */
    private static final int STATISTICS_SEGMENTS_CNT = 10;

    /** Marshaller. */
    private static volatile Marshaller marsh;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        getTestResources().startThreads(true);

        marsh = getTestResources().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        getTestResources().stopThreads();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleMessages() throws Exception {
        final Collection<GridNioSession> sesSet = new GridConcurrentHashSet<>();

        final AtomicReference<Exception> err = new AtomicReference<>();

        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                // No-op.
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                if (e != null)
                    err.compareAndSet(null, e);
                else
                    sesSet.add(ses);
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                // Reply with echo.
                ses.send(msg);
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridPlainParser(), lsnr);

        try {
            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override
                public void run() {
                    byte[] msg = new byte[MSG_SIZE];

                    for (int i = 0; i < msg.length; i++)
                        msg[i] = (byte) (i ^ (i * i - 1)); // Some data

                    for (int i = 0; i < RECONNECT_MSG_CNT; i++)
                        validateSendMessage(msg);
                }
            }, THREAD_CNT);

            fut.get();

            U.sleep(100);

            assertNull("Exception occurred in server", err.get());

            assertEquals("Invalid count of sessions", RECONNECT_MSG_CNT * THREAD_CNT, sesSet.size());
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * Tests that server correctly closes client sockets on shutdown.
     *
     * @throws Exception if failed.
     */
    public void testServerShutdown() throws Exception {
        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                // No-op.
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                // No-op.
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                // Reply with echo.
                ses.send(msg);
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridPlainParser(), lsnr);

        Socket s = createSocket();

        s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

        try {
            byte[] msg = new byte[MSG_SIZE];

            s.getOutputStream().write(msg);

            int rcvd = 0;

            InputStream inputStream = s.getInputStream();

            while (rcvd < msg.length) {
                int cnt = inputStream.read(msg, rcvd, msg.length - rcvd);

                if (cnt == -1)
                    fail("Server closed connection before echo reply was fully sent");

                rcvd += cnt;
            }

            // Now stop the server, we must see correct socket shutdown.
            srvr.stop();

            U.sleep(100);

            assertEquals(-1, inputStream.read());
        }
        finally {
            s.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectSocketClose() throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                // No-op.
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                if (e != null)
                    err.compareAndSet(null, e);
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                // Reply with echo.
                ses.send(msg);
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridPlainParser(), lsnr);

        try {
            Socket s = createSocket();

            s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

            if (!(s instanceof SSLSocket)) {
                // These methods are not supported by SSL sockets.
                s.shutdownInput();

                s.shutdownOutput();
            }

            s.close();
        }
        finally {
            srvr.stop();
        }

        assertNull("Unexpected exception on socket close", err.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testThroughput() throws Exception {
        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                // No-op.
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                // No-op.
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                // Reply with echo.
                ses.send(msg);
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridPlainParser(), lsnr);

        final AtomicLong cnt = new AtomicLong();

        final AtomicBoolean running = new AtomicBoolean(true);

        try {
            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override
                public void run() {
                    try {
                        byte[] msg = new byte[MSG_SIZE];

                        for (int i = 0; i < msg.length; i++)
                            msg[i] = (byte) (i ^ (i * i - 1)); // Some data

                        try (Socket s = createSocket()) {
                            s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

                            OutputStream out = s.getOutputStream();

                            InputStream in = new BufferedInputStream(s.getInputStream());

                            while (running.get()) {
                                validateSendMessage0(msg, out, in);

                                cnt.incrementAndGet();
                            }
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, THREAD_CNT);

            long old = 0;

            long interval = 5000;

            for (int i = 0; i < 5; i++) {
                long l = cnt.get();

                U.sleep(interval);

                long msgRate = (l - old) * 1000 / interval;
                long netSaturation = (l - old) * MSG_SIZE * 2 * 8 / (1024 * 1024);

                System.out.println(">>>>>>> Rate=" + msgRate + " msg/sec, Sat=" + netSaturation + " MBit/s");

                old = l;
            }

            running.set(false);

            fut.get();
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseSession() throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        final AtomicReference<GridNioSession> sesRef = new AtomicReference<>();

        final CountDownLatch connectLatch = new CountDownLatch(1);

        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                info("On connected: " + ses);

                sesRef.set(ses);

                connectLatch.countDown();
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                if (e != null)
                    err.compareAndSet(null, e);
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                // Reply with echo.
                ses.send(msg);
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridPlainParser(), lsnr);

        try {
            Socket s = createSocket();

            s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

            // This is needed for SSL to begin handshake.
            s.getOutputStream().write(new byte[1]);

            try {
                U.await(connectLatch);

                GridNioSession ses = sesRef.get();

                assertNotNull(ses);

                assertTrue(ses.close().get());

                ses.send(new byte[2]).get();

                fail("Exception must be thrown");
            }
            catch (Exception e) {
                info("Caught exception: " + e);

                if (!X.hasCause(e, IOException.class)) {
                    error("Unexpected exception.", e);

                    fail();
                }
            }
            finally {
                s.close();
            }

            assertFalse(sesRef.get().close().get());
        }
        finally {
            srvr.stop();
        }

        assertNull("Unexpected exception on socket close", err.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendAfterServerStop() throws Exception {
        final AtomicReference<GridNioSession> sesRef = new AtomicReference<>();

        final CountDownLatch connectLatch = new CountDownLatch(1);

        GridNioServerListener lsnr = new GridNioServerListenerAdapter() {
            @Override public void onConnected(GridNioSession ses) {
                info("On connected: " + ses);

                sesRef.set(ses);

                connectLatch.countDown();
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                log.info("Message: " + msg);
            }
        };

        GridNioServer.Builder<?> builder = serverBuilder(PORT, new GridPlainParser(), lsnr);

        GridNioServer<?> srvr = builder.sendQueueLimit(5).build();

        srvr.start();

        try {
            Socket s = createSocket();

            s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

            s.getOutputStream().write(new byte[1]);

            U.await(connectLatch);

            GridNioSession ses = sesRef.get();

            assertNotNull(ses);

            ses.send(new byte[1]);

            srvr.stop();

            for (int i = 0; i < 10; i++)
                ses.send(new byte[1]);
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * Sends message and validates reply.
     *
     * @param msg Message to send.
     */
    private void validateSendMessage(byte[] msg) {
        try {
            Socket s = createSocket();

            s.connect(new InetSocketAddress(U.getLocalHost(), PORT), 1000);

            try {
                s.getOutputStream().write(msg);

                byte[] res = new byte[MSG_SIZE];

                int rcvd = 0;

                InputStream inputStream = s.getInputStream();

                while (rcvd < res.length) {
                    int cnt = inputStream.read(res, rcvd, res.length - rcvd);

                    if (cnt == -1)
                        fail("Server closed connection before echo reply was fully sent");

                    rcvd += cnt;
                }

                if (!(s instanceof SSLSocket)) {
                    s.shutdownOutput();

                    s.shutdownInput();
                }

                assertEquals(msg.length, res.length);

                for (int i = 0; i < msg.length; i++)
                    assertEquals("Mismatch in position " + i, msg[i], res[i]);
            }
            finally {
                s.close();
            }
        }
        catch (Exception e) {
            fail("Exception while sending message: " + e.getMessage());
        }
    }

    /**
     * Sends message and validates reply.
     *
     * @param msg Message to send.
     * @param out Out.
     * @param in In.
     * @throws Exception If failed.
     */
    private void validateSendMessage0(byte[] msg, OutputStream out, InputStream in) throws Exception {
        out.write(msg);

        byte[] res = new byte[MSG_SIZE];

        int rcvd = 0;

        while (rcvd < res.length) {
            int cnt = in.read(res, rcvd, res.length - rcvd);

            if (cnt == -1)
                fail("Server closed connection before echo reply was fully sent");

            rcvd += cnt;
        }

        assertEquals(msg.length, res.length);

        for (int i = 0; i < msg.length; i++)
            assertEquals("Mismatch in position " + i, msg[i], res[i]);
    }

    /**
     * Starts server with specified arguments.
     *
     * @param port Port to listen.
     * @param parser Parser to use.
     * @param lsnr Listener.
     * @return Started server.
     * @throws Exception If failed.
     */
    protected final GridNioServer<?> startServer(int port, GridNioParser parser, GridNioServerListener lsnr)
        throws Exception {
        GridNioServer<?> srvr = serverBuilder(port, parser, lsnr).build();

        srvr.start();

        return srvr;
    }

    /**
     * @param port Port to listen.
     * @param parser Parser to use.
     * @param lsnr Listener.
     * @return Server builder.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected GridNioServer.Builder<?> serverBuilder(int port,
        GridNioParser parser,
        GridNioServerListener lsnr)
        throws Exception
    {
        return GridNioServer.builder()
            .address(U.getLocalHost())
            .port(port)
            .listener(lsnr)
            .logger(log)
            .selectorCount(Runtime.getRuntime().availableProcessors())
            .gridName("nio-test-grid")
            .tcpNoDelay(true)
            .directBuffer(true)
            .byteOrder(ByteOrder.nativeOrder())
            .socketSendBufferSize(0)
            .socketReceiveBufferSize(0)
            .sendQueueLimit(0)
            .filters(new GridNioCodecFilter(parser, log, false));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSendReceive() throws Exception {
        CountDownLatch latch = new CountDownLatch(10);

        NioListener lsnr = new NioListener(latch);

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        TestClient client = null;

        try {
            for (int i = 0; i < 5; i++) {
                client = createClient(U.getLocalHost(), PORT, U.getLocalHost());

                client.sendMessage(createMessage(), MSG_SIZE);
                client.sendMessage(createMessage(), MSG_SIZE);

                client.close();
            }

            assert latch.await(30, SECONDS);

            assertEquals("Unexpected message count", 10, lsnr.getMessageCount());
        }
        finally {
            srvr.stop();

            if (client != null)
                client.close();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testAsyncSendReceive() throws Exception {
        CountDownLatch latch = new CountDownLatch(10);

        NioListener lsnr = new NioListener(latch);

        GridNioServer<?> srvr1 = startServer(PORT, new BufferedParser(false), lsnr);
        GridNioServer<?> srvr2 = startServer(PORT + 1, new BufferedParser(false), lsnr);

        GridNioSession ses = null;

        try {
            SocketChannel ch = SocketChannel.open(new InetSocketAddress(U.getLocalHost(), PORT + 1));

            GridNioFuture<GridNioSession> fut = srvr1.createSession(ch, null);

            ses = fut.get();

            for (int i = 0; i < 5; i++) {
                ses.send(createMessageWithSize());
                ses.send(createMessageWithSize());
            }

            assert latch.await(30, SECONDS);

            assertEquals("Unexpected message count", 10, lsnr.getMessageCount());
        }
        finally {
            if (ses != null)
                ses.close();

            srvr1.stop();
            srvr2.stop();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultiThreadedSendReceive() throws Exception {
        CountDownLatch latch = new CountDownLatch(MSG_CNT * THREAD_CNT);

        NioListener lsnr = new NioListener(latch);

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        try {
            final byte[] data = createMessage();

            multithreaded(new Runnable() {
                @Override public void run() {
                    TestClient client = null;

                    try {
                        client = createClient(U.getLocalHost(), PORT, U.getLocalHost());

                        for (int i = 0; i < MSG_CNT; i++)
                            client.sendMessage(data, data.length);
                    }
                    catch (Exception e) {
                        error("Failed to send message.", e);

                        assert false : "Message sending failed: " + e;
                    }
                    finally {
                        if (client != null)
                            client.close();
                    }
                }

            }, THREAD_CNT, "sender");

            assert latch.await(30, SECONDS);

            assertEquals("Unexpected message count", MSG_CNT * THREAD_CNT, lsnr.getMessageCount());
            assertFalse("Size check failed", lsnr.isSizeFailed());
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentConnects() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(THREAD_CNT);

        final AtomicReference<Exception> err = new AtomicReference<>();

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()),
            new EchoListener());

        try {
            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings("BusyWait")
                @Override public void run() {
                    try {
                        for (int i = 0; i < 100 && !Thread.currentThread().isInterrupted(); i++) {
                            TestClient client = null;

                            try {
                                client = createClient(U.getLocalHost(), PORT, U.getLocalHost());

                                MessageWithId msg = new MessageWithId(idProvider.getAndIncrement());

                                byte[] data = serializeMessage(msg);

                                for (int j = 0; j < 10; j++)
                                    client.sendMessage(data, data.length);

                                for (int j = 0; j < 10; j++) {
                                    byte[] res = client.receiveMessage();

                                    if (!Arrays.equals(data, res)) {
                                        info("Invalid response received.");

                                        err.compareAndSet(null, new IgniteCheckedException("Invalid response received."));

                                        barrier.reset();

                                        return;
                                    }
                                }
                            }
                            catch (IgniteCheckedException e) {
                                info("Encountered unexpected exception: " + e);

                                err.compareAndSet(null, e);

                                // Break the barrier.
                                barrier.reset();

                                break;
                            }
                            catch (IOException e) {
                                info("Encountered IO exception: " + e);

                                err.compareAndSet(null, e);

                                // Break the barrier.
                                barrier.reset();

                                break;
                            }
                            finally {
                                if (client != null)
                                    client.close();
                            }

                            if ("conn-tester-1".equals(Thread.currentThread().getName()) && i % 10 == 0 && i > 0)
                                info("Run " + i + " iterations.");

                            barrier.await();

                            Thread.sleep(100);
                        }
                    }
                    catch (InterruptedException ignored) {
                        barrier.reset();

                        info("Test thread was interrupted (will exit).");
                    }
                    catch (BrokenBarrierException ignored) {
                        info("Barrier was broken (will exit).");
                    }
                }
            }, THREAD_CNT, "conn-tester");

            fut.get();

            if (err.get() != null)
                throw err.get();
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * @throws Exception if test failed.
     */
    public void testDeliveryDuration() throws Exception {
        idProvider.set(1);

        CountDownLatch latch = new CountDownLatch(MSG_CNT * THREAD_CNT);

        final Map<Integer, Long> deliveryDurations = new ConcurrentHashMap<>();

        final Map<Integer, Long> sndTimes = new ConcurrentHashMap<>();

        DeliveryTimestampAwareNioListener lsnr = new DeliveryTimestampAwareNioListener(latch, deliveryDurations);

        final AtomicLong cntr = new AtomicLong();

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        try {
            multithreaded(new Runnable() {
                @Override public void run() {
                    TestClient client = null;

                    try {
                        client = createClient(U.getLocalHost(), PORT, U.getLocalHost());

                        while (cntr.getAndIncrement() < MSG_CNT * THREAD_CNT) {
                            MessageWithId msg = new MessageWithId(idProvider.getAndIncrement());

                            byte[] data = serializeMessage(msg);

                            long start = System.currentTimeMillis();

                            deliveryDurations.put(msg.getId(), start);

                            client.sendMessage(data, data.length);

                            long end = System.currentTimeMillis();

                            sndTimes.put(msg.getId(), end - start);
                        }
                    }
                    catch (Exception e) {
                        error("Failed to send message.", e);

                        assert false : "Message sending failed: " + e;
                    }
                    finally {
                        if (client != null)
                            client.close();
                    }
                }

            }, THREAD_CNT, "sender");

            assert latch.await(30, SECONDS);

            assertEquals("Unexpected message count", MSG_CNT * THREAD_CNT, lsnr.getMessageCount());
            assertFalse("Size check failed", lsnr.isSizeFailed());

            printDurationStatistics(deliveryDurations, sndTimes, MSG_CNT * THREAD_CNT, 300);
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSessionIdleTimeout() throws Exception {
        final int sesCnt = 20;

        final CountDownLatch latch = new CountDownLatch(sesCnt);

        GridNioServerListener<byte[]> lsnr = new GridNioServerListenerAdapter<byte[]>() {
            @Override public void onConnected(GridNioSession ses) {
                // No-op.
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                // No-op.
            }

            @Override public void onMessage(GridNioSession ses, byte[] msg) {
                // No-op.
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) {
                info("Session idle: " + ses);

                latch.countDown();

                ses.close();
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        srvr.idleTimeout(1000);

        try {
            multithreaded(new Runnable() {
                @Override public void run() {
                    try (TestClient ignored = createClient(U.getLocalHost(), PORT, U.getLocalHost())) {
                        info("Before sleep.");

                        U.sleep(4000);

                        info("After sleep.");
                    }
                    catch (Exception e) {
                        error("Failed to create client: " + e.getMessage());

                        fail("Failed to create client: " + e.getMessage());
                    }
                    finally {
                        info("Test thread finished.");
                    }
                }
            }, sesCnt);

            assert latch.await(30, SECONDS);
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteTimeout() throws Exception {
        final int sesCnt = 20;

        final CountDownLatch latch = new CountDownLatch(sesCnt);

        final byte[] bytes = "Reply.".getBytes();

        GridNioServerListener<byte[]> lsnr = new GridNioServerListenerAdapter<byte[]>() {
            @Override public void onConnected(GridNioSession ses) {
                ses.send(bytes);
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                // No-op.
            }

            @Override public void onMessage(GridNioSession ses, byte[] msg) {
                // No-op.
            }

            @Override public void onSessionWriteTimeout(GridNioSession ses) {
                info("Session write timed out: " + ses);

                latch.countDown();

                ses.close();
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) {
                assert false;
            }
        };

        GridNioServer<?> srvr = startServer(PORT, new GridBufferedParser(true, ByteOrder.nativeOrder()), lsnr);

        // Set flag using reflection.
        Field f = srvr.getClass().getDeclaredField("skipWrite");

        f.setAccessible(true);

        f.set(srvr, true);

        srvr.writeTimeout(500);

        try {
            multithreaded(new Runnable() {
                @Override public void run() {
                    try (TestClient ignored = createClient(U.getLocalHost(), PORT, U.getLocalHost())) {
                        info("Before sleep.");

                        U.sleep(4000);

                        info("After sleep.");
                    }
                    catch (Exception e) {
                        error("Failed to create client: ", e);

                        fail("Failed to create client: " + e.getMessage());
                    }
                    finally {
                        info("Test thread finished.");
                    }
                }
            }, sesCnt);

            assert latch.await(30, SECONDS);
        }
        finally {
            srvr.stop();
        }
    }

    /**
     * Prints statistics on message delivery duration time.
     *
     * @param deliveryDurations Map with durations.
     * @param sndDurations Map with send times.
     * @param maxMsgId The maximum message id sent.
     * @param guessedMaxDuration Estimated max duration registered. All values that greater then this value will
     * go to the last segment.
     */
    private void printDurationStatistics(Map<Integer, Long> deliveryDurations, Map<Integer, Long> sndDurations,
        int maxMsgId, long guessedMaxDuration) {
        DurationAccumulator overall = new DurationAccumulator();

        DurationAccumulator[] msgRange =collectStatistics(deliveryDurations, overall, maxMsgId);

        int[] durationRange = new int[STATISTICS_SEGMENTS_CNT];

        for (Map.Entry<Integer, Long> e : deliveryDurations.entrySet()) {
            long duration = e.getValue();

            int idx = (int)((duration - 1) * durationRange.length / guessedMaxDuration);

            if (idx < 0)
                idx = 0;

            if (idx >= durationRange.length)
                idx = durationRange.length - 1;

            durationRange[idx]++;
        }

        DurationAccumulator sndOverall = new DurationAccumulator();

        DurationAccumulator[] sndRange = collectStatistics(sndDurations, sndOverall, maxMsgId);

        info("Overall send statistics: " + sndOverall);

        info("Per message id statistics:");

        for (int i = 0; i < sndRange.length; i++) {
            int rangeMin = i * maxMsgId / sndRange.length + 1;

            int rangeMax = (i + 1) * maxMsgId / sndRange.length;

            info(">>> [" + rangeMin + '-' + rangeMax + "]: " + sndRange[i]);
        }

        info("Overall duration statistics: " + overall);

        info("Per message id statistics:");

        for (int i = 0; i < msgRange.length; i++) {
            int rangeMin = i * maxMsgId / msgRange.length + 1;

            int rangeMax = (i + 1) * maxMsgId / msgRange.length;

            info(">>> [" + rangeMin + '-' + rangeMax + "]: " + msgRange[i]);
        }

        info("Duration histogram:");

        for (int i = 0; i < msgRange.length; i++) {
            int rangeMin = (int)(i * guessedMaxDuration / durationRange.length + 1);

            int rangeMax = (int)((i + 1) * guessedMaxDuration / durationRange.length);

            float percents = (float) durationRange[i] * 100 / overall.count();

            info(">>> [" + rangeMin + '-' + rangeMax + "] ms: " + String.format("%.2f", percents) + "% (" +
                durationRange[i] + " messages)");
        }
    }

    /**
     * Creates new client.
     * @param addr Address to connect to.
     * @param port Port to connect to.
     * @param locHost Local host.
     * @return Created client.
     * @throws IgniteCheckedException If client cannot be created.
     */
    protected TestClient createClient(InetAddress addr, int port, InetAddress locHost) throws IgniteCheckedException {
        return new TestClient(createSocket(), addr, port, 0);
    }

    /**
     * @return Created socket.
     * @throws IgniteCheckedException If socket creation failed.
     */
    protected Socket createSocket() throws IgniteCheckedException {
        return new Socket();
    }

    /**
     * Collects statistics for a given map.
     *
     * @param data Map with durations.
     * @param overall Overall statistics accumulator.
     * @param maxMsgId Maximum message id sent.
     * @return Array with statistics per message id range.
     */
    private DurationAccumulator[] collectStatistics(Map<Integer, Long> data, DurationAccumulator overall,
                                                    int maxMsgId) {
        DurationAccumulator[] msgRange = new DurationAccumulator[STATISTICS_SEGMENTS_CNT];

        for (int i = 0; i < msgRange.length; i++)
            msgRange[i] = new DurationAccumulator();

        for (Map.Entry<Integer, Long> e : data.entrySet()) {
            long duration = e.getValue();

            int msgId = e.getKey();

            overall.duration(duration);

            assert msgId <= maxMsgId : "msgId=" + msgId + ", maxMsgId=" + maxMsgId;

            int idx = (msgId - 1) * msgRange.length / maxMsgId;

            if (idx >= msgRange.length)
                idx = msgRange.length - 1;

            msgRange[idx].duration(duration);
        }

        return msgRange;
    }

    /**
     * Class that calculates max, min and avg duration for a set of values.
     */
    private static class DurationAccumulator {
        /** Minimum registered value. */
        private long min = Long.MAX_VALUE;

        /** Maximum registered value. */
        private long max;

        /** Average registered value. */
        private long avg;

        /** Sum. */
        @GridToStringExclude
        private long sum;

        /** Registration count. */
        private long cnt;

        /**
         * Adds this value to statistics.
         *
         * @param duration Duration.
         */
        public void duration(long duration) {
            min = Math.min(min, duration);

            max = Math.max(max, duration);

            sum += duration;

            cnt++;
        }

        /**
         * @return Count of registered durations.
         */
        public long count() {
            return cnt;
        }

        /**
         * Calculates average based on statistics.
         *
         * @return Calculated average
         */
        public long average() {
            if (cnt > 0)
                avg = sum / cnt;

            return avg;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            average();

            return S.toString(DurationAccumulator.class, this);
        }
    }

    /**
     * Serializes given message to byte array.
     *
     * @param msg Message to serialize.
     * @return Serialized message.
     * @throws IgniteCheckedException If failed.
     */
    private <T extends Serializable> byte[] serializeMessage(T msg) throws IgniteCheckedException {
        return marsh.marshal(msg);
    }

    /**
     * Deserializes given byte sequence into a message of desired type.
     *
     * @param data Serialized data.
     * @param <T> Message type.
     * @return Deserialized message.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private <T> T deserializeMessage(byte[] data) throws IgniteCheckedException {
        return marsh.<T>unmarshal(data, getClass().getClassLoader());
    }

    /**
     * @return Test message.
     */
    private byte[] createMessage() {
        return new byte[MSG_SIZE];
    }

    /**
     * @return Test message.
     */
    private byte[] createMessageWithSize() {
        byte[] msg = new byte[MSG_SIZE];

        U.intToBytes(MSG_SIZE - 4, msg, 0);

        return msg;
    }

    /**
     *
     */
    private static class NioListener extends GridNioServerListenerAdapter<byte[]> {
        /** */
        private final AtomicInteger msgCnt = new AtomicInteger(0);

        /** */
        private final AtomicBoolean sizeFailed = new AtomicBoolean(false);

        /** */
        private final CountDownLatch latch;

        /**
         * @param latch The latch.
         */
        NioListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void onConnected(GridNioSession ses) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onMessage(GridNioSession ses, byte[] data) {
            msgCnt.incrementAndGet();

            int expMsgSize = getExpectedMessageSize();

            if (data == null || (expMsgSize != 0 && data.length != getExpectedMessageSize()))
                sizeFailed.set(true);

            if (latch != null)
                latch.countDown();
        }

        /**
         * Provides the expected message size for the message type listener deals with.
         *
         * @return Expected message size.
         */
        protected int getExpectedMessageSize() {
            return MSG_SIZE;
        }

        /**
         * @return Received message count.
         */
        public int getMessageCount() {
            return msgCnt.get();
        }

        /**
         * @return True if size test fails.
         */
        public boolean isSizeFailed() {
            return sizeFailed.get();
        }
    }

    /**
     * Echo listener.
     */
    private static class EchoListener extends GridNioServerListenerAdapter<byte[]> {
        /** {@inheritDoc} */
        @Override public void onConnected(GridNioSession ses) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
            if (e != null)
                fail("Unexpected exception occurred while handling connection: " + e);
        }

        /** {@inheritDoc} */
        @Override public void onMessage(GridNioSession ses, byte[] msg) {
            ses.send(msg);
        }
    }

    /**
     *
     */
    private class DeliveryTimestampAwareNioListener extends NioListener {
        /** */
        private final Map<Integer, Long> deliveryDurations;

        /**
         * @param latch Latch
         * @param deliveryDurations Delivery durations map.
         */
        DeliveryTimestampAwareNioListener(CountDownLatch latch, Map<Integer, Long> deliveryDurations) {
            super(latch);

            this.deliveryDurations = deliveryDurations;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(GridNioSession ses, byte[] data) {
            try {
                long deliveryTime = System.currentTimeMillis();

                MessageWithId msg = deserializeMessage(data);

                Integer id = msg.getId();

                deliveryDurations.put(id, deliveryTime - deliveryDurations.get(id));

                super.onMessage(ses, data);
            }
            catch (Exception e) {
                error("Failed to process Timestamped Message", e);
            }
        }

        /** {@inheritDoc} */
        @Override protected int getExpectedMessageSize() {
            // Disable message size check.
            return 0;
        }
    }

    /**
     * Test client to use instead of {@link GridTcpNioCommunicationClient}
     */
    private static class TestClient implements AutoCloseable {
        /** Socket implementation to use. */
        private Socket sock;

        /** Socket output stream */
        private OutputStream out;

        /** Socket input stream. */
        private InputStream in;

        /**
         * Creates test client.
         *
         * @param sock Socket to use.
         * @param addr Address to connect to.
         * @param port Port to connect to.
         * @param connTimeout Connection timeout.
         * @throws IgniteCheckedException If connect failed.
         */
        private TestClient(Socket sock, InetAddress addr, int port, int connTimeout) throws IgniteCheckedException {
            this.sock = sock;

            try {
                sock.connect(new InetSocketAddress(addr, port), connTimeout);

                if (sock instanceof SSLSocket)
                    ((SSLSocket)sock).startHandshake();

                out = sock.getOutputStream();

                in = sock.getInputStream();
            }
            catch (IOException e) {
                close();

                throw new IgniteCheckedException(e);
            }
        }

        /**
         * Send bytes over the socket prefixing them with the 4-byte array size.
         *
         * @param data Data to send.
         * @param len Count of bytes to write.
         * @throws IOException If send failed.
         */
        public void sendMessage(byte[] data, int len) throws IOException {
            out.write(U.intToBytes(len));
            out.write(data, 0, len);
        }

        /**
         * Reads prefixed bytes from socket input stream. Note that this method may block forever if there is
         * not enough bytes available in socket input stream.
         *
         * @return Read bytes.
         * @throws IOException If read failed or stream has been closed before full message has been read.
         */
        public byte[] receiveMessage() throws IOException {
            byte[] prefix = new byte[4];
            int idx = 0;

            while (idx < 4) {
                int read = in.read(prefix, idx, 4 - idx);

                if (read < 0)
                    throw new IOException("End of stream reached before message length was read.");

                idx += read;
            }

            int len = U.bytesToInt(prefix, 0);

            byte[] res = new byte[len];
            idx = 0;

            while (idx < len) {
                int read = in.read(res, idx, len - idx);

                if (read < 0)
                    throw new IOException("End of stream reached before message body was read.");

                idx += read;
            }

            return res;
        }

        /**
         * Closes the test client.
         */
        @Override public void close() {
            U.closeQuiet(sock);
        }
    }

    /**
     * Simple parser that converts byte buffer to byte array without any parsing.
     */
    private static class GridPlainParser implements GridNioParser {
        /** {@inheritDoc} */
        @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException {
            byte[] res = new byte[buf.remaining()];

            buf.get(res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer encode(GridNioSession ses, Object msg) {
            return ByteBuffer.wrap((byte[])msg);
        }
    }

    /**
     *
     */
    private static class MessageWithId implements Serializable {
        /** */
        private final int id;

        /**
         * @param id Message ID.
         */
        public MessageWithId(int id) {
            this.id = id;
        }

        /** */
        @SuppressWarnings({"unused"})
        private final byte[] body = new byte[MSG_SIZE];

        /**
         * @return The ID of the message.
         */
        public int getId() {
            return id;
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return super.getTestTimeout() * 5;
    }

    /**
     *
     */
    private static class BufferedParser extends GridBufferedParser {
        /**
         * @param directBuf Direct buf flag.
         */
        private BufferedParser(boolean directBuf) {
            super(directBuf, ByteOrder.nativeOrder());
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
            // IO manager creates array ready to send.
            return msg instanceof byte[] ? ByteBuffer.wrap((byte[])msg) : (ByteBuffer)msg;
        }
    }
}