/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.testframework.junits.common.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Send message test.
 */
public class GridCommunicationSendMessageSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Sample count. */
    private static final int SAMPLE_CNT = 1;

    /** */
    private static final byte DIRECT_TYPE = (byte)202;

    /** */
    private int bufSize;

    static {
        GridTcpCommunicationMessageFactory.registerCustom(new GridTcpCommunicationMessageProducer() {
            @Override public GridTcpCommunicationMessageAdapter create(byte type) {
                return new TestMessage();
            }
        }, DIRECT_TYPE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setConnectionBufferSize(bufSize);

        c.setCommunicationSpi(commSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendMessage() throws Exception {
        try {
            startGridsMultiThreaded(2);

            doSend();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendMessageWithBuffer() throws Exception {
        bufSize = 8192;

        try {
            startGridsMultiThreaded(2);

            doSend();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doSend() throws Exception {
        GridIoManager mgr0 = ((GridKernal)grid(0)).context().io();
        GridIoManager mgr1 = ((GridKernal)grid(1)).context().io();

        String topic = "test-topic";

        final CountDownLatch latch = new CountDownLatch(SAMPLE_CNT);

        mgr1.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                latch.countDown();
            }
        });

        long time = System.nanoTime();

        for (int i = 1; i <= SAMPLE_CNT; i++) {
            mgr0.send(grid(1).localNode(), topic, new TestMessage(), GridIoPolicy.PUBLIC_POOL);

            if (i % 500 == 0)
                info("Sent messages count: " + i);
        }

        assert latch.await(3, SECONDS);

        time = System.nanoTime() - time;

        info(">>>");
        info(">>> send() time (ms): " + MILLISECONDS.convert(time, NANOSECONDS));
        info(">>>");
    }

    /** */
    private static class TestMessage extends GridTcpCommunicationMessageAdapter {
        /** {@inheritDoc} */
        @SuppressWarnings("CloneDoesntCallSuperClone")
        @Override public GridTcpCommunicationMessageAdapter clone() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            return commState.putByte(directType());
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return DIRECT_TYPE;
        }
    }
}
