/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.nio.*;
import java.util.concurrent.atomic.*;

/**
 * Tests filter chain event processing.
 */
public class GridNioFilterChainSelfTest extends GridCommonAbstractTest {
    /** Session opened event meta name. */
    private static final int OPENED_META_NAME = 11;

    /** Session closed event meta name. */
    private static final int CLOSED_META_NAME = 12;

    /** Exception caught. */
    private static final int EXCEPTION_CAUGHT_META_NAME = 13;

    /** Message received event meta name. */
    private static final int MESSAGE_RECEIVED_META_NAME = 14;

    /** Message write event meta name. */
    private static final int MESSAGE_WRITE_META_NAME = 15;

    /** Session close event meta name. */
    private static final int CLOSE_META_NAME = 16;

    /** Session idle timeout meta name. */
    private static final int IDLE_META_NAME = 17;

    /** Session write timeout meta name. */
    private static final int WRITE_TIMEOUT_META_NAME = 18;

    /**
     * @throws Exception If failed.
     */
    public void testChainEvents() throws Exception {
        final AtomicReference<String> connectedEvt = new AtomicReference<>();
        final AtomicReference<String> disconnectedEvt = new AtomicReference<>();
        final AtomicReference<String> msgEvt = new AtomicReference<>();
        final AtomicReference<String> idleEvt = new AtomicReference<>();
        final AtomicReference<String> writeTimeoutEvt = new AtomicReference<>();

        final AtomicReference<String> sndEvt = new AtomicReference<>();
        final AtomicReference<String> closeEvt = new AtomicReference<>();

        final AtomicReference<ByteBuffer> rcvdMsgObj = new AtomicReference<>();
        final AtomicReference<Object> sndMsgObj = new AtomicReference<>();

        GridNioServerListener<Object> testLsnr = new GridNioServerListenerAdapter<Object>() {
            @Override public void onConnected(GridNioSession ses) {
                connectedEvt.compareAndSet(null, ses.<String>meta(OPENED_META_NAME));
            }

            @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                disconnectedEvt.compareAndSet(null, ses.<String>meta(CLOSED_META_NAME));
            }

            @Override public void onMessage(GridNioSession ses, Object msg) {
                msgEvt.compareAndSet(null, ses.<String>meta(MESSAGE_RECEIVED_META_NAME));

                rcvdMsgObj.compareAndSet(null, (ByteBuffer)msg);
            }

            @Override public void onSessionWriteTimeout(GridNioSession ses) {
                writeTimeoutEvt.compareAndSet(null, ses.<String>meta(WRITE_TIMEOUT_META_NAME));
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) {
                idleEvt.compareAndSet(null, ses.<String>meta(IDLE_META_NAME));
            }
        };

        GridNioFilterAdapter testHead = new GridNioFilterAdapter("TestHead") {
            @Override public void onSessionOpened(GridNioSession ses) throws GridException {
                proceedSessionOpened(ses);
            }

            @Override public void onSessionClosed(GridNioSession ses) throws GridException {
                proceedSessionClosed(ses);
            }

            @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
                proceedExceptionCaught(ses, ex);
            }

            @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) {
                sndEvt.compareAndSet(null, ses.<String>meta(MESSAGE_WRITE_META_NAME));

                sndMsgObj.compareAndSet(null, msg);

                return null;
            }

            @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
                proceedMessageReceived(ses, msg);
            }

            @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
                closeEvt.compareAndSet(null, ses.<String>meta(CLOSE_META_NAME));

                return null;
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
                proceedSessionIdleTimeout(ses);
            }

            @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
                proceedSessionWriteTimeout(ses);
            }
        };

        GridNioFilterChain<Object> chain = new GridNioFilterChain<>(log,  testLsnr, testHead,
            new AppendingFilter("A"), new AppendingFilter("B"), new AppendingFilter("C"), new AppendingFilter("D"));

        GridNioSession ses = new MockNioSession();

        ByteBuffer snd = ByteBuffer.wrap(new byte[1]);
        ByteBuffer rcvd = ByteBuffer.wrap(new byte[1]);

        chain.onSessionOpened(ses);
        chain.onSessionClosed(ses);
        chain.onMessageReceived(ses, rcvd);
        chain.onSessionIdleTimeout(ses);
        chain.onSessionWriteTimeout(ses);
        assertNull(chain.onSessionClose(ses));
        assertNull(chain.onSessionWrite(ses, snd));

        assertEquals("DCBA", connectedEvt.get());
        assertEquals("DCBA", disconnectedEvt.get());
        assertEquals("DCBA", msgEvt.get());
        assertEquals("DCBA", idleEvt.get());
        assertEquals("DCBA", writeTimeoutEvt.get());

        assertEquals("ABCD", sndEvt.get());
        assertEquals("ABCD", closeEvt.get());

        assertTrue(snd == sndMsgObj.get());
        assertTrue(rcvd == rcvdMsgObj.get());
    }

    /**
     *
     */
    private static class AppendingFilter extends GridNioFilterAdapter {
        /** Param that will be appended to event trace. */
        private String param;

        /**
         * Constructs test filter.
         *
         * @param param Filter parameter.
         */
        private AppendingFilter(String param) {
            super(AppendingFilter.class.getSimpleName());

            this.param = param;
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) throws GridException {
            chainMeta(ses, OPENED_META_NAME);

            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) throws GridException {
            chainMeta(ses, CLOSED_META_NAME);

            proceedSessionClosed(ses);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
            chainMeta(ses, EXCEPTION_CAUGHT_META_NAME);

            proceedExceptionCaught(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
            chainMeta(ses, MESSAGE_WRITE_META_NAME);

            return proceedSessionWrite(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
            chainMeta(ses, MESSAGE_RECEIVED_META_NAME);

            proceedMessageReceived(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
            chainMeta(ses, CLOSE_META_NAME);

            return proceedSessionClose(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
            chainMeta(ses, IDLE_META_NAME);

            proceedSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
            chainMeta(ses, WRITE_TIMEOUT_META_NAME);

            proceedSessionWriteTimeout(ses);
        }

        /** {@inheritDoc} */
        public String toString() {
            return "AppendingFilter [param=" + param + ']';
        }

        /**
         * Appends parameter of this filter to a meta with specified name.
         *
         * @param ses Session.
         * @param metaKey Meta key.
         */
        private void chainMeta(GridNioSession ses, int metaKey) {
            String att = ses.meta(metaKey);

            att = (att == null ? "" : att) + param;

            ses.addMeta(metaKey, att);
        }
    }

    /**
     */
    public class MockNioSession extends GridMetadataAwareAdapter implements GridNioSession {
        /** Local address */
        private InetSocketAddress locAddr = new InetSocketAddress(0);

        /** Remote address. */
        private InetSocketAddress rmtAddr = new InetSocketAddress(0);

        /**
         * Creates empty mock session.
         */
        public MockNioSession() {
            // No-op.
        }

        /**
         * Creates new mock session with given addresses.
         *
         * @param locAddr Local address.
         * @param rmtAddr Remote address.
         */
        public MockNioSession(InetSocketAddress locAddr, InetSocketAddress rmtAddr) {
            this();

            this.locAddr = locAddr;
            this.rmtAddr = rmtAddr;
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress localAddress() {
            return locAddr;
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress remoteAddress() {
            return rmtAddr;
        }

        /** {@inheritDoc} */
        @Override public long bytesSent() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long bytesReceived() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long createTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long closeTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long lastReceiveTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long lastSendTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long lastSendScheduleTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> close() {
            return new GridNioFinishedFuture<>(true);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> send(Object msg) {
            return new GridNioFinishedFuture<>(true);
        }

        /** {@inheritDoc} */
        @Override public <T> T meta(int key) {
            return meta(String.valueOf(key));
        }

        /** {@inheritDoc} */
        @Override public <T> T addMeta(int key, T val) {
            return addMeta(String.valueOf(key), val);
        }

        /** {@inheritDoc} */
        @Override public <T> T removeMeta(int key) {
            return removeMeta(String.valueOf(key));
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Object> resumeReads() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Object> pauseReads() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean accepted() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readsPaused() {
            return false;
        }
    }
}
