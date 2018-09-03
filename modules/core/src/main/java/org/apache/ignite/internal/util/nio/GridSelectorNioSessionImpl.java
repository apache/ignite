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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Session implementation bound to selector API and socket API.
 * Note that this implementation requires non-null values for local and remote
 * socket addresses.
 */
class GridSelectorNioSessionImpl extends GridNioSessionImpl {
    /** Pending write requests. */
    private final ConcurrentLinkedDeque8<SessionWriteRequest> queue = new ConcurrentLinkedDeque8<>();

    /** Selection key associated with this session. */
    @GridToStringExclude
    private SelectionKey key;

    /** Current worker thread. */
    private volatile GridNioWorker worker;

    /** Semaphore. */
    @GridToStringExclude
    private final Semaphore sem;

    /** Write buffer. */
    private ByteBuffer writeBuf;

    /** Read buffer. */
    private ByteBuffer readBuf;

    /** Incoming recovery data. */
    private GridNioRecoveryDescriptor inRecovery;

    /** Outgoing recovery data. */
    private GridNioRecoveryDescriptor outRecovery;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private List<GridNioServer.SessionChangeRequest> pendingStateChanges;

    /** */
    final AtomicBoolean procWrite = new AtomicBoolean();

    /** */
    private Object sysMsg;

    /**
     * Creates session instance.
     *
     * @param log Logger.
     * @param worker NIO worker thread.
     * @param filterChain Filter chain that will handle requests.
     * @param locAddr Local address.
     * @param rmtAddr Remote address.
     * @param accepted Accepted flag.
     * @param sndQueueLimit Send queue limit.
     * @param writeBuf Write buffer.
     * @param readBuf Read buffer.
     */
    GridSelectorNioSessionImpl(
        IgniteLogger log,
        GridNioWorker worker,
        GridNioFilterChain filterChain,
        InetSocketAddress locAddr,
        InetSocketAddress rmtAddr,
        boolean accepted,
        int sndQueueLimit,
        @Nullable ByteBuffer writeBuf,
        @Nullable ByteBuffer readBuf
    ) {
        super(filterChain, locAddr, rmtAddr, accepted);

        assert worker != null;
        assert sndQueueLimit >= 0;

        assert locAddr != null : "GridSelectorNioSessionImpl should have local socket address.";
        assert rmtAddr != null : "GridSelectorNioSessionImpl should have remote socket address.";

        assert log != null;

        this.log = log;

        this.worker = worker;

        sem = sndQueueLimit > 0 ? new Semaphore(sndQueueLimit) : null;

        if (writeBuf != null) {
            writeBuf.clear();

            this.writeBuf = writeBuf;
        }

        if (readBuf != null) {
            readBuf.clear();

            this.readBuf = readBuf;
        }
    }

    /**
     * @return Worker.
     */
    GridNioWorker worker() {
        return worker;
    }

    /**
     * Sets selection key for this session.
     *
     * @param key Selection key.
     */
    void key(SelectionKey key) {
        assert key != null;

        this.key = key;
    }

    /**
     * @return Write buffer.
     */
    public ByteBuffer writeBuffer() {
        return writeBuf;
    }

    /**
     * @return Read buffer.
     */
    public ByteBuffer readBuffer() {
        return readBuf;
    }

    /**
     * @return Registered selection key for this session.
     */
    SelectionKey key() {
        return key;
    }

    /**
     * @param from Current session worker.
     * @param fut Move future.
     * @return {@code True} if session move was scheduled.
     */
    boolean offerMove(GridNioWorker from, GridNioServer.SessionChangeRequest fut) {
        synchronized (this) {
            if (log.isDebugEnabled())
                log.debug("Offered move [ses=" + this + ", fut=" + fut + ']');

            GridNioWorker worker0 = worker;

            if (worker0 != from)
                return false;

            worker.offer(fut);
        }

        return true;
    }

    /**
     * @param fut Future.
     */
    void offerStateChange(GridNioServer.SessionChangeRequest fut) {
        synchronized (this) {
            if (log.isDebugEnabled())
                log.debug("Offered move [ses=" + this + ", fut=" + fut + ']');

            GridNioWorker worker0 = worker;

            if (worker0 == null) {
                if (pendingStateChanges == null)
                    pendingStateChanges = new ArrayList<>();

                pendingStateChanges.add(fut);
            }
            else
                worker0.offer(fut);
        }
    }

    /**
     * @param moveFrom Current session worker.
     */
    void startMoveSession(GridNioWorker moveFrom) {
        synchronized (this) {
            assert this.worker == moveFrom;

            if (log.isDebugEnabled())
                log.debug("Started moving [ses=" + this + ", from=" + moveFrom + ']');

            List<GridNioServer.SessionChangeRequest> sesReqs = moveFrom.clearSessionRequests(this);

            worker = null;

            if (sesReqs != null) {
                if (pendingStateChanges == null)
                    pendingStateChanges = new ArrayList<>();

                pendingStateChanges.addAll(sesReqs);
            }
        }
    }

    /**
     * @param moveTo New session worker.
     */
    void finishMoveSession(GridNioWorker moveTo) {
        synchronized (this) {
            assert worker == null;

            if (log.isDebugEnabled())
                log.debug("Finishing moving [ses=" + this + ", to=" + moveTo + ']');

            worker = moveTo;

            if (pendingStateChanges != null) {
                moveTo.offer(pendingStateChanges);

                pendingStateChanges = null;
            }
        }
    }

    /**
     * Adds write future at the front of the queue without acquiring back pressure semaphore.
     *
     * @param writeFut Write request.
     * @return Updated size of the queue.
     */
    int offerSystemFuture(SessionWriteRequest writeFut) {
        writeFut.messageThread(true);

        boolean res = queue.offerFirst(writeFut);

        assert res : "Future was not added to queue";

        return queue.sizex();
    }

    /**
     * Adds write future to the pending list and returns the size of the queue.
     * <p>
     * Note that separate counter for the queue size is needed because in case of concurrent
     * calls this method should return different values (when queue size is 0 and 2 concurrent calls
     * occur exactly one call will return 1)
     *
     * @param writeFut Write request to add.
     * @return Updated size of the queue.
     */
    int offerFuture(SessionWriteRequest writeFut) {
        boolean msgThread = GridNioBackPressureControl.threadProcessingMessage();

        if (sem != null && !msgThread)
            sem.acquireUninterruptibly();

        writeFut.messageThread(msgThread);

        boolean res = queue.offer(writeFut);

        assert res : "Future was not added to queue";

        return queue.sizex();
    }

    /**
     * @param futs Futures to resend.
     */
    void resend(Collection<SessionWriteRequest> futs) {
        assert queue.isEmpty() : queue.size();

        boolean add = queue.addAll(futs);

        assert add;
    }

    /**
     * @return Message that is in the head of the queue, {@code null} if queue is empty.
     */
    @Nullable SessionWriteRequest pollFuture() {
        SessionWriteRequest last = queue.poll();

        if (last != null) {
            if (sem != null && !last.messageThread())
                sem.release();

            if (outRecovery != null) {
                if (!outRecovery.add(last)) {
                    LT.warn(log, "Unacknowledged messages queue size overflow, will attempt to reconnect " +
                        "[remoteAddr=" + remoteAddress() +
                        ", queueLimit=" + outRecovery.queueLimit() + ']');

                    if (log.isDebugEnabled())
                        log.debug("Unacknowledged messages queue size overflow, will attempt to reconnect " +
                            "[remoteAddr=" + remoteAddress() +
                            ", queueSize=" + outRecovery.messagesRequests().size() +
                            ", queueLimit=" + outRecovery.queueLimit() + ']');

                    close();
                }
            }
        }

        return last;
    }

    /**
     * @param fut Future.
     * @return {@code True} if future was removed from queue.
     */
    boolean removeFuture(SessionWriteRequest fut) {
        assert closed();

        return queue.removeLastOccurrence(fut);
    }

    /**
     * Gets number of write requests in a queue that have not been processed yet.
     *
     * @return Number of write requests.
     */
    int writeQueueSize() {
        return queue.sizex();
    }

    /**
     * @return Write requests.
     */
    Collection<SessionWriteRequest> writeQueue() {
        return queue;
    }

    /** {@inheritDoc} */
    @Override public void outRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
        assert recoveryDesc != null;

        outRecovery = recoveryDesc;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNioRecoveryDescriptor outRecoveryDescriptor() {
        return outRecovery;
    }

    /** {@inheritDoc} */
    @Override public void inRecoveryDescriptor(GridNioRecoveryDescriptor recoveryDesc) {
        assert recoveryDesc != null;

        inRecovery = recoveryDesc;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNioRecoveryDescriptor inRecoveryDescriptor() {
        return inRecovery;
    }

    /** {@inheritDoc} */
    @Override public <T> T addMeta(int key, @Nullable T val) {
        if (!accepted() && val instanceof GridNioRecoveryDescriptor) {
            outRecovery = (GridNioRecoveryDescriptor)val;

            if (!outRecovery.pairedConnections())
                inRecovery = outRecovery;

            outRecovery.onConnected();

            return null;
        }
        else
            return super.addMeta(key, val);
    }

    /**
     *
     */
    void onServerStopped() {
        onClosed();
    }

    /**
     *
     */
    void onClosed() {
        if (sem != null)
            sem.release(1_000_000);
    }

    /** {@inheritDoc} */
    @Override public void systemMessage(Object sysMsg) {
        this.sysMsg = sysMsg;
    }

    /**
     * @return {@code True} if have pending system message to send.
     */
    boolean hasSystemMessage() {
        return sysMsg != null;
    }

    /**
     * Gets and clears pending system message.
     *
     * @return Pending system message.
     */
    Object systemMessage() {
        Object ret = sysMsg;

        sysMsg = null;

        return ret;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSelectorNioSessionImpl.class, this, super.toString());
    }
}
