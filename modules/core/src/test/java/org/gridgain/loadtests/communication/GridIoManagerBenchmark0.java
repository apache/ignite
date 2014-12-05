/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.communication;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;

/**
 *
 */
public class GridIoManagerBenchmark0 extends GridCommonAbstractTest {
    /** */
    public static final int CONCUR_MSGS = 10 * 1024;

    /** */
    private static final int THREADS = 2;

    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        c.setCommunicationSpi(getCommunication());

        return c;
    }

    /**
     * @param len Length.
     * @return Test string.
     */
    private static String generateTestString(int len) {
        assert len > 0;
        SB sb = new SB();

        for (int i = 0; i < len; i++)
            sb.a(Character.forDigit(i % 10, 10));

        return sb.toString();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    public void testThroughput() throws Exception {
        final GridKernal sndKernal = (GridKernal)grid(0);
        final GridKernal rcvKernal = (GridKernal)grid(1);

        final ClusterNode sndNode = sndKernal.localNode();
        final ClusterNode rcvNode = rcvKernal.localNode();

        final GridIoManager snd = sndKernal.context().io();
        final GridIoManager rcv = rcvKernal.context().io();

        info("Senders: " + THREADS);
        info("Messages: " + CONCUR_MSGS);

        final Semaphore sem = new Semaphore(CONCUR_MSGS);
        final LongAdder msgCntr = new LongAdder();

        final String topic = "test-topic";

        rcv.addMessageListener(
            topic,
            new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    try {
                        rcv.send(sndNode, topic, (GridTcpCommunicationMessageAdapter)msg, PUBLIC_POOL);
                    }
                    catch (GridException e) {
                        error("Failed to send message.", e);
                    }
                }
            });

        snd.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                msgCntr.increment();

                sem.release();
            }
        });

        Timer t = new Timer("results-reporter");

        t.schedule(new TimerTask() {
            private long ts = System.currentTimeMillis();

            @Override public void run() {
                long newTs = System.currentTimeMillis();
                long qrys = msgCntr.sumThenReset();

                long time = newTs - ts;

                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
                    ", executed=" + qrys + ", time=" + time + ']');

                ts = newTs;
            }
        }, 10000, 10000);

        final AtomicBoolean finish = new AtomicBoolean();

        IgniteFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    IgniteUuid msgId = IgniteUuid.randomUuid();

                    while (!finish.get()) {
                        sem.acquire();

                        snd.send(rcvNode, topic, new GridTestMessage(msgId, (String)null), PUBLIC_POOL);
                    }
                }
                catch (GridException e) {
                    X.println("Message send failed", e);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                return null;
            }
        }, THREADS, "send-thread");

        Thread.sleep(TEST_TIMEOUT);

        finish.set(true);

        sem.release(CONCUR_MSGS * 2);

        t.cancel();

        f.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    public void testLatency() throws Exception {
        final GridKernal sndKernal = (GridKernal)grid(0);
        final GridKernal rcvKernal = (GridKernal)grid(1);

        final ClusterNode sndNode = sndKernal.localNode();
        final ClusterNode rcvNode = rcvKernal.localNode();

        final GridIoManager snd = sndKernal.context().io();
        final GridIoManager rcv = rcvKernal.context().io();

        final LongAdder msgCntr = new LongAdder();

        final Integer topic = 1;

        final Map<IgniteUuid, CountDownLatch> map = new ConcurrentHashMap8<>();

        rcv.addMessageListener(
            topic,
            new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    try {
                        rcv.send(sndNode, topic, (GridTcpCommunicationMessageAdapter)msg, PUBLIC_POOL);
                    }
                    catch (GridException e) {
                        error("Failed to send message.", e);
                    }
                }
            });

        snd.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                map.get(((GridTestMessage)msg).id()).countDown();
            }
        });

        Timer t = new Timer("results-reporter");

        t.schedule(new TimerTask() {
            private long ts = System.currentTimeMillis();

            @Override public void run() {
                long newTs = System.currentTimeMillis();
                long qrys = msgCntr.sumThenReset();

                long time = newTs - ts;

                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
                    ", executed=" + qrys + ", time=" + time + ']');

                ts = newTs;
            }
        }, 10000, 10000);

        final AtomicBoolean finish = new AtomicBoolean();

        IgniteFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    IgniteUuid msgId = IgniteUuid.randomUuid();

                    while (!finish.get()) {
                        CountDownLatch latch = new CountDownLatch(1);

                        map.put(msgId, latch);

                        snd.send(rcvNode, topic, new GridTestMessage(msgId, (String)null), PUBLIC_POOL);

                        latch.await();

                        msgCntr.increment();
                    }
                }
                catch (GridException e) {
                    X.println("Message send failed", e);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                return null;
            }
        }, 1, "send-thread");

        Thread.sleep(TEST_TIMEOUT);

        finish.set(true);

        t.cancel();

        f.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    public void testVariableLoad() throws Exception {
        final GridKernal sndKernal = (GridKernal)grid(0);
        final GridKernal rcvKernal = (GridKernal)grid(1);

        final ClusterNode sndNode = sndKernal.localNode();
        final ClusterNode rcvNode = rcvKernal.localNode();

        final GridIoManager snd = sndKernal.context().io();
        final GridIoManager rcv = rcvKernal.context().io();

        info("Senders: " + THREADS);
        info("Messages: " + CONCUR_MSGS);

        final Semaphore sem = new Semaphore(CONCUR_MSGS);
        final LongAdder msgCntr = new LongAdder();

        final String topic = "test-topic";

        final Map<IgniteUuid, CountDownLatch> latches = new ConcurrentHashMap8<>();

        rcv.addMessageListener(
            topic,
            new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    try {
                        rcv.send(sndNode, topic, (GridTcpCommunicationMessageAdapter)msg, PUBLIC_POOL);
                    }
                    catch (GridException e) {
                        error("Failed to send message.", e);
                    }
                }
            });

        snd.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                msgCntr.increment();

                sem.release();

                CountDownLatch latch = latches.get(((GridTestMessage)msg).id());

                if (latch != null)
                    latch.countDown();
            }
        });

        final AtomicBoolean finish = new AtomicBoolean();
        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

        IgniteFuture<?> f = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finish.get()) {
                    CountDownLatch latch = latchRef.get();

                    if (latch != null)
                        U.await(latch);

                    IgniteUuid msgId = IgniteUuid.randomUuid();

                    sem.acquire();

                    snd.send(rcvNode, topic, new GridTestMessage(msgId, (String)null), PUBLIC_POOL);
                }

                return null;
            }
        }, THREADS, "send-thread");

        IgniteFuture<?> f1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            private long ts = System.currentTimeMillis();

            @Override public Object call() throws Exception {
                try {
                    while (!finish.get()) {
                        info(U.nl() + ">>>" + U.nl() + ">>> High load." + U.nl() + ">>>");

                        U.sleep(15 * 1000);

                        reportNumbers();

                        info(U.nl() + ">>>" + U.nl() + ">>> Low load." + U.nl() + ">>>");

                        CountDownLatch latch = new CountDownLatch(1);

                        try {
                            // Here will be a pause.
                            latchRef.set(latch);

                            U.sleep(7 * 1000);

                            reportNumbers();
                        }
                        finally {
                            latch.countDown();
                        }
                    }
                }
                catch (GridException e) {
                    X.println("Message send failed", e);
                }

                return null;
            }

            /**
             *
             */
            void reportNumbers() {
                long newTs = System.currentTimeMillis();
                long qrys = msgCntr.sumThenReset();

                long time = newTs - ts;

                X.println("Communication benchmark [qps=" + qrys * 1000 / time +
                    ", executed=" + qrys + ", time=" + time + ']');

                ts = newTs;

            }
        }, 1, "load-dispatcher");

        IgniteFuture<?> f2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finish.get()) {
                    U.sleep(1000);

                    IgniteUuid msgId = IgniteUuid.randomUuid();
                    CountDownLatch latch = new CountDownLatch(1);

                    latches.put(msgId, latch);

                    snd.send(rcvNode, topic, new GridTestMessage(msgId, (String)null), PUBLIC_POOL);

                    long start = System.currentTimeMillis();

                    latch.await();

                    info("Response time: " + (System.currentTimeMillis() - start));
                }

                return null;
            }
        }, THREADS, "low-loader");

        Thread.sleep(TEST_TIMEOUT);

        finish.set(true);

        sem.release(CONCUR_MSGS * 2);

        f.get();
        f1.get();
        f2.get();
    }

    /**
     * @return SPI instance.
     */
    private CommunicationSpi getCommunication() {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setTcpNoDelay(true);
        spi.setSharedMemoryPort(-1);
        spi.setConnectionBufferSize(0);

        info("Comm SPI: " + spi);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT + 60 * 1000;
    }
}
