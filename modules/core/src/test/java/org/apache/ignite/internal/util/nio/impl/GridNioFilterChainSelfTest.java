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

package org.apache.ignite.internal.util.nio.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFilterChain;
import org.apache.ignite.internal.util.nio.GridNioFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

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
            @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
                proceedSessionOpened(ses);
            }

            @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
                proceedSessionClosed(ses);
            }

            @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
                proceedExceptionCaught(ses, ex);
            }

            @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut) {
                sndEvt.compareAndSet(null, ses.<String>meta(MESSAGE_WRITE_META_NAME));

                sndMsgObj.compareAndSet(null, msg);

                return null;
            }

            @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
                proceedMessageReceived(ses, msg);
            }

            @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
                closeEvt.compareAndSet(null, ses.<String>meta(CLOSE_META_NAME));

                return null;
            }

            @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
                proceedSessionIdleTimeout(ses);
            }

            @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
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
        assertNull(chain.onSessionWrite(ses, snd, true));

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
        @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
            chainMeta(ses, OPENED_META_NAME);

            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
            chainMeta(ses, CLOSED_META_NAME);

            proceedSessionClosed(ses);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
            chainMeta(ses, EXCEPTION_CAUGHT_META_NAME);

            proceedExceptionCaught(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut) throws IgniteCheckedException {
            chainMeta(ses, MESSAGE_WRITE_META_NAME);

            return proceedSessionWrite(ses, msg, fut);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
            chainMeta(ses, MESSAGE_RECEIVED_META_NAME);

            proceedMessageReceived(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
            chainMeta(ses, CLOSE_META_NAME);

            return proceedSessionClose(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
            chainMeta(ses, IDLE_META_NAME);

            proceedSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
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
        @Override public void sendNoFuture(Object msg) {
            // No-op.
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

        /** {@inheritDoc} */
        @Override public void outRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridNioRecoveryDescriptor outRecoveryDescriptor() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void inRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridNioRecoveryDescriptor inRecoveryDescriptor() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void systemMessage(Object msg) {
            // No-op.
        }
    }
}