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

package org.apache.ignite.loadtests.communication;
//
//import org.apache.ignite.*;
//import org.apache.ignite.lang.*;
//import org.apache.ignite.lang.utils.*;
//import org.apache.ignite.spi.*;
//import org.apache.ignite.spi.communication.*;
//import org.apache.ignite.spi.communication.tcp.*;
//import org.apache.ignite.typedef.*;
//import org.apache.ignite.typedef.internal.*;
//import org.apache.ignite.testframework.*;
//import org.apache.ignite.testframework.junits.*;
//import org.apache.ignite.testframework.junits.spi.*;
//
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.*;
//
///** */
//@GridSpiTest(spi = GridTcpCommunicationSpi.class, group = "TCP communication SPI benchmark.")
//public class GridTcpCommunicationBenchmark extends GridSpiAbstractTest<GridTcpCommunicationSpi> {
//    /** */
//    public static final int CONCUR_MSGS = 10 * 1024;
//
//    /** */
//    private static final int THREADS = 1;
//
//    /** */
//    private static final long TEST_TIMEOUT = 3 * 60 * 1000;
//
//    /** */
//    private final Collection<GridTestResources> spiRsrcs = new ArrayList<>();
//
//    /** */
//    private final Map<UUID, GridCommunicationSpi> spis = new HashMap<>();
//
//    /** */
//    private final Collection<GridNode> nodes = new ArrayList<>();
//
//    /**
//     * Disable automatic test SPI start.
//     */
//    public GridTcpCommunicationBenchmark() {
//        super(false);
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void beforeTest() throws Exception {
//        Map<GridNode, GridSpiTestContext> ctxs = new HashMap<>();
//
//        for (int i = 0; i < 2; i++) {
//            GridCommunicationSpi spi = getCommunication();
//
//            GridTestResources rsrcs = new GridTestResources();
//
//            GridTestNode node = new GridTestNode(rsrcs.getNodeId());
//
//            GridSpiTestContext ctx = initSpiContext();
//
//            ctx.setLocalNode(node);
//
//            spiRsrcs.add(rsrcs);
//
//            rsrcs.inject(spi);
//
//            node.setAttributes(spi.getNodeAttributes());
//
//            nodes.add(node);
//
//            spi.spiStart(getTestGridName() + (i + 1));
//
//            spis.put(rsrcs.getNodeId(), spi);
//
//            spi.onContextInitialized(ctx);
//
//            ctxs.put(node, ctx);
//        }
//
//        // For each context set remote nodes.
//        for (Map.Entry<GridNode, GridSpiTestContext> e : ctxs.entrySet()) {
//            for (GridNode n : nodes) {
//                if (!n.equals(e.getKey()))
//                    e.getValue().remoteNodes().add(n);
//            }
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void afterTest() throws Exception {
//        for (GridCommunicationSpi spi : spis.values()) {
//            spi.setListener(null);
//
//            spi.spiStop();
//        }
//
//        for (GridTestResources rsrcs : spiRsrcs)
//            rsrcs.stopThreads();
//    }
//
//    /**
//     * @param len Length.
//     * @return Test string.
//     */
//    private static String generateTestString(int len) {
//        assert len > 0;
//        SB sb = new SB();
//
//        for (int i = 0; i < len; i++)
//            sb.a(Character.forDigit(i % 10, 10));
//
//        return sb.toString();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @SuppressWarnings("deprecation")
//    public void testThroughput() throws Exception {
//        assert spis.size() == 2;
//        assert nodes.size() == 2;
//
//        Iterator<GridNode> it = nodes.iterator();
//
//        final GridNode sndNode = it.next();
//        final GridNode rcvNode = it.next();
//
//        final GridCommunicationSpi sndComm = spis.get(sndNode.id());
//        final GridCommunicationSpi rcvComm = spis.get(rcvNode.id());
//
//        final String testStr = generateTestString(66);
//
//        info("Test string length: " + testStr.length());
//        info("Senders: " + THREADS);
//        info("Messages: " + CONCUR_MSGS);
//
//        final Semaphore sem = new Semaphore(CONCUR_MSGS);
//        final LongAdder8 msgCntr = new LongAdder8();
//
//        rcvComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] msg, GridAbsClosure msgC) {
//                try {
//                    byte[] res = U.join(U.intToBytes(msg.length), msg);
//
//                    rcvComm.sendMessage(sndNode, res, 0, res.length);
//                }
//                catch (GridSpiException e) {
//                    log.error("Message echo failed.", e);
//                }
//                finally {
//                    msgC.apply();
//                }
//            }
//        });
//
//        sndComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] msg, GridAbsClosure msgC) {
//                msgCntr.increment();
//
//                sem.release();
//
//                msgC.apply();
//            }
//        });
//
//        Timer t = new Timer("results-reporter");
//
//        t.schedule(new TimerTask() {
//            private long ts = System.currentTimeMillis();
//
//            @Override public void run() {
//                long newTs = System.currentTimeMillis();
//                long qrys = msgCntr.sumThenReset();
//
//                long time = newTs - ts;
//
//                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
//                    ", executed=" + qrys + ", time=" + time + ']');
//
//                ts = newTs;
//            }
//        }, 10000, 10000);
//
//        final AtomicBoolean finish = new AtomicBoolean();
//
//        GridFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                try {
//                    while (!finish.get()) {
//                        GridUuid msgId = GridUuid.randomUuid();
//
//                        sem.acquire();
//
//                        // Loading message with additional data, to get results,
//                        // comparable with IoManager benchmark.
//                        GridTuple2<byte[], Integer> t = getTestResources().getMarshaller().
//                            marshalNoCopy(new GridTestMessage(msgId, testStr), 4);
//
//                        byte[] buf = t.get1();
//                        int len = t.get2();
//
//                        U.intToBytes(len - 4, buf, 0);
//
//                        sndComm.sendMessage(rcvNode, buf, 0, len);
//                    }
//                }
//                catch (IgniteCheckedException e) {
//                    X.println("Message send failed", e);
//                }
//                catch (InterruptedException ignored) {
//                    // No-op.
//                }
//
//                return null;
//            }
//        }, THREADS, "send-thread");
//
//        Thread.sleep(TEST_TIMEOUT);
//
//        finish.set(true);
//
//        sem.release(CONCUR_MSGS * 2);
//
//        t.cancel();
//
//        f.get();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @SuppressWarnings("deprecation")
//    public void testLatency() throws Exception {
//        assert spis.size() == 2;
//        assert nodes.size() == 2;
//
//        Iterator<GridNode> it = nodes.iterator();
//
//        final GridNode sndNode = it.next();
//        final GridNode rcvNode = it.next();
//
//        final GridCommunicationSpi sndComm = spis.get(sndNode.id());
//        final GridCommunicationSpi rcvComm = spis.get(rcvNode.id());
//
//        final String testStr = generateTestString(66);
//
//        info("Test string length: " + testStr.length());
//
//        final LongAdder8 msgCntr = new LongAdder8();
//
//        final Map<GridUuid, CountDownLatch> map = new ConcurrentHashMap8<>();
//
//        rcvComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] msg, GridAbsClosure msgC) {
//                try {
//                    byte[] res = U.join(U.intToBytes(msg.length), msg);
//
//                    rcvComm.sendMessage(sndNode, res, 0, res.length);
//                }
//                catch (GridSpiException e) {
//                    log.error("Message echo failed.", e);
//                }
//                finally {
//                    msgC.apply();
//                }
//            }
//        });
//
//        final ClassLoader clsLdr = getClass().getClassLoader();
//
//        sndComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] msg, GridAbsClosure msgC) {
//                try {
//                    GridTestMessage testMsg = getTestResources().getMarshaller().unmarshal(msg, clsLdr);
//
//                    map.get(testMsg.id()).countDown();
//                }
//                catch (IgniteCheckedException e) {
//                    U.error(log, "Failed to ", e);
//                }
//                finally {
//                    msgC.apply();
//                }
//            }
//        });
//
//        Timer t = new Timer("results-reporter");
//
//        t.schedule(new TimerTask() {
//            private long ts = System.currentTimeMillis();
//
//            @Override public void run() {
//                long newTs = System.currentTimeMillis();
//                long qrys = msgCntr.sumThenReset();
//
//                long time = newTs - ts;
//
//                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
//                    ", executed=" + qrys + ", time=" + time + ']');
//
//                ts = newTs;
//            }
//        }, 10000, 10000);
//
//        final AtomicBoolean finish = new AtomicBoolean();
//
//        GridFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                info("Test thread started.");
//
//                try {
//                    GridUuid msgId = GridUuid.randomUuid();
//
//                    GridTuple2<byte[], Integer> t = getTestResources().getMarshaller().
//                        marshalNoCopy(new GridTestMessage(msgId, testStr), 4);
//
//                    byte[] buf = t.get1();
//                    int len = t.get2();
//
//                    U.intToBytes(len - 4, buf, 0);
//
//                    while (!finish.get()) {
//                        // Loading message with additional data, to get results,
//                        // comparable with IoManager benchmark.
//                        CountDownLatch latch = new CountDownLatch(1);
//
//                        map.put(msgId, latch);
//
//                        sndComm.sendMessage(rcvNode, buf, 0, len);
//
//                        latch.await();
//
//                        msgCntr.increment();
//                    }
//                }
//                catch (IgniteCheckedException e) {
//                    X.println("Message send failed", e);
//                }
//                catch (InterruptedException ignored) {
//                    // No-op.
//                }
//
//                return null;
//            }
//        }, 2, "send-thread");
//
//        Thread.sleep(TEST_TIMEOUT);
//
//        finish.set(true);
//
//        t.cancel();
//
//        f.get();
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    @SuppressWarnings("deprecation")
//    public void testVariableLoad() throws Exception {
//        assert spis.size() == 2;
//        assert nodes.size() == 2;
//
//        Iterator<GridNode> it = nodes.iterator();
//
//        final GridNode sndNode = it.next();
//        final GridNode rcvNode = it.next();
//
//        final GridCommunicationSpi sndComm = spis.get(sndNode.id());
//        final GridCommunicationSpi rcvComm = spis.get(rcvNode.id());
//
//        final String testStr = generateTestString(16);
//
//        info("Test string length: " + testStr.length());
//        info("Senders: " + THREADS);
//        info("Messages: " + CONCUR_MSGS);
//
//        final Semaphore sem = new Semaphore(CONCUR_MSGS);
//        final LongAdder8 msgCntr = new LongAdder8();
//
//        final Map<GridUuid, CountDownLatch> latches = new ConcurrentHashMap8<>();
//
//        rcvComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] msg, GridAbsClosure msgC) {
//                try {
//                    byte[] res = U.join(U.intToBytes(msg.length), msg);
//
//                    rcvComm.sendMessage(sndNode, res, 0, res.length);
//                }
//                catch (GridSpiException e) {
//                    log.error("Message echo failed.", e);
//                }
//                finally {
//                    msgC.apply();
//                }
//            }
//        });
//
//        sndComm.setListener(new GridCommunicationListener() {
//            @Override public void onMessage(UUID nodeId, byte[] buf, GridAbsClosure msgC) {
//                msgCntr.increment();
//
//                sem.release();
//
//                GridTestMessage msg = null;
//
//                try {
//                    msg = getTestResources().getMarshaller().unmarshal(buf, U.gridClassLoader());
//                }
//                catch (IgniteCheckedException e) {
//                    U.error(log, "Failed to unmarshal message.", e);
//
//                    fail();
//                }
//                finally {
//                    msgC.apply();
//                }
//
//                CountDownLatch latch = latches.get(msg.id());
//
//                if (latch != null)
//                    latch.countDown();
//            }
//        });
//
//        final AtomicBoolean finish = new AtomicBoolean();
//        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();
//
//        GridFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                while (!finish.get()) {
//                    CountDownLatch latch = latchRef.get();
//
//                    if (latch != null)
//                        U.await(latch);
//
//                    GridUuid msgId = GridUuid.randomUuid();
//
//                    sem.acquire();
//
//                    // Loading message with additional data, to get results,
//                    // comparable with IoManager benchmark.
//                    GridTuple2<byte[], Integer> t = getTestResources().getMarshaller().
//                        marshalNoCopy(new GridTestMessage(msgId, testStr), 4);
//
//                    byte[] buf = t.get1();
//                    int len = t.get2();
//
//                    U.intToBytes(len - 4, buf, 0);
//
//                    sndComm.sendMessage(rcvNode, buf, 0, len);
//                }
//
//                return null;
//            }
//        }, THREADS, "send-thread");
//
//        GridFuture<?> f1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//            private long ts = System.currentTimeMillis();
//
//            @Override public Object call() throws Exception {
//                try {
//                    while (!finish.get()) {
//                        info(U.nl() + ">>>" + U.nl() + ">>> High load." + U.nl() + ">>>");
//
//                        U.sleep(15 * 1000);
//
//                        reportNumbers();
//
//                        info(U.nl() + ">>>" + U.nl() + ">>> Low load." + U.nl() + ">>>");
//
//                        CountDownLatch latch = new CountDownLatch(1);
//
//                        try {
//                            // Here will be a pause.
//                            latchRef.set(latch);
//
//                            U.sleep(7 * 1000);
//
//                            reportNumbers();
//                        }
//                        finally {
//                            latch.countDown();
//                        }
//                    }
//                }
//                catch (IgniteCheckedException e) {
//                    X.println("Message send failed", e);
//                }
//
//                return null;
//            }
//
//            /**
//             *
//             */
//            void reportNumbers() {
//                long newTs = System.currentTimeMillis();
//                long qrys = msgCntr.sumThenReset();
//
//                long time = newTs - ts;
//
//                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
//                    ", executed=" + qrys + ", time=" + time + ']');
//
//                ts = newTs;
//
//            }
//        }, 1, "load-dispatcher");
//
//        GridFuture<?> f2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                while (!finish.get()) {
//                    U.sleep(1000);
//
//                    GridUuid msgId = GridUuid.randomUuid();
//                    CountDownLatch latch = new CountDownLatch(1);
//
//                    latches.put(msgId, latch);
//
//                    GridTuple2<byte[], Integer> t = getTestResources().getMarshaller().
//                        marshalNoCopy(new GridTestMessage(msgId, testStr), 4);
//
//                    byte[] buf = t.get1();
//                    int len = t.get2();
//
//                    U.intToBytes(len - 4, buf, 0);
//
//                    sndComm.sendMessage(rcvNode, buf, 0, len);
//
//                    long start = System.currentTimeMillis();
//
//                    latch.await();
//
//                    info("Response time: " + (System.currentTimeMillis() - start));
//                }
//
//                return null;
//            }
//        }, THREADS, "low-loader");
//
//        Thread.sleep(TEST_TIMEOUT);
//
//        finish.set(true);
//
//        sem.release(CONCUR_MSGS * 2);
//
//        f.get();
//        f1.get();
//        f2.get();
//    }
//
//    /**
//     * @return SPI instance.
//     */
//    private GridCommunicationSpi getCommunication() {
//        GridTcpCommunicationSpi spi = new GridTcpCommunicationSpi();
//
//        spi.setSharedMemoryPort(-1);
//        spi.setNoDelay(true);
//        spi.setLocalAddress("127.0.0.1");
//
//        return spi;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected long getTestTimeout() {
//        return TEST_TIMEOUT + 60 * 1000;
//    }
//}