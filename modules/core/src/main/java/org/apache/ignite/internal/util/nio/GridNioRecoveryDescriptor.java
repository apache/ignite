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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Recovery information for single node.
 */
public class GridNioRecoveryDescriptor {
    /** Number of acknowledged messages. */
    private long acked;

    /** Unacknowledged message futures. */
    private final ArrayDeque<GridNioFuture<?>> msgFuts;

    /** Number of messages to resend. */
    private int resendCnt;

    /** Number of received messages. */
    private long rcvCnt;

    /** Reserved flag. */
    private boolean reserved;

    /** Last acknowledged message. */
    private long lastAck;

    /** Node left flag. */
    private boolean nodeLeft;

    /** Target node. */
    private final ClusterNode node;

    /** Logger. */
    private final IgniteLogger log;

    /** Incoming connection request from remote node. */
    private IgniteBiTuple<Long, IgniteInClosure<Boolean>> handshakeReq;

    /** Connected flag. */
    private boolean connected;

    /** Number of outgoing connect attempts. */
    private long connectCnt;

    /** Maximum size of unacknowledged messages queue. */
    private final int queueLimit;

    /**
     * @param queueLimit Maximum size of unacknowledged messages queue.
     * @param node Node.
     * @param log Logger.
     */
    public GridNioRecoveryDescriptor(int queueLimit, ClusterNode node, IgniteLogger log) {
        assert !node.isLocal() : node;
        assert queueLimit > 0;

        msgFuts = new ArrayDeque<>(queueLimit);

        this.queueLimit = queueLimit;
        this.node = node;
        this.log = log;
    }

    /**
     * @return Connect count.
     */
    public long incrementConnectCount() {
        return connectCnt++;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * Increments received messages counter.
     *
     * @return Number of received messages.
     */
    public long onReceived() {
        rcvCnt++;

        return rcvCnt;
    }

    /**
     * @return Number of received messages.
     */
    public long received() {
        return rcvCnt;
    }

    /**
     * @param lastAck Last acknowledged message.
     */
    public void lastAcknowledged(long lastAck) {
        this.lastAck = lastAck;
    }

    /**
     * @return Last acknowledged message.
     */
    public long lastAcknowledged() {
        return lastAck;
    }

    /**
     * @return Received messages count.
     */
    public long receivedCount() {
        return rcvCnt;
    }

    /**
     * @return Maximum size of unacknowledged messages queue.
     */
    public int queueLimit() {
        return queueLimit;
    }

    /**
     * @param fut NIO future.
     * @return {@code False} if queue limit is exceeded.
     */
    public boolean add(GridNioFuture<?> fut) {
        assert fut != null;

        if (!fut.skipRecovery()) {
            if (resendCnt == 0) {
                msgFuts.addLast(fut);

                return msgFuts.size() < queueLimit;
            }
            else
                resendCnt--;
        }

        return true;
    }

    /**
     * @param rcvCnt Number of messages received by remote node.
     */
    public void ackReceived(long rcvCnt) {
        if (log.isDebugEnabled())
            log.debug("Handle acknowledgment [acked=" + acked + ", rcvCnt=" + rcvCnt +
                ", msgFuts=" + msgFuts.size() + ']');

        while (acked < rcvCnt) {
            GridNioFuture<?> fut = msgFuts.pollFirst();

            assert fut != null : "Missed message future [rcvCnt=" + rcvCnt +
                ", acked=" + acked +
                ", desc=" + this + ']';

            assert fut.isDone() : fut;

            if (fut.ackClosure() != null)
                fut.ackClosure().apply(null);

            acked++;
        }
    }

    /**
     * Node left callback.
     */
    public void onNodeLeft() {
        GridNioFuture<?>[] futs = null;

        synchronized (this) {
            nodeLeft = true;

            if (!reserved && !msgFuts.isEmpty()) {
                futs = msgFuts.toArray(new GridNioFuture<?>[msgFuts.size()]);

                msgFuts.clear();
            }
        }

        if (futs != null)
            completeOnNodeLeft(futs);
    }

    /**
     * @return Message futures for unacknowledged messages.
     */
    public Deque<GridNioFuture<?>> messagesFutures() {
        return msgFuts;
    }

    /**
     * @param node Node.
     * @return {@code True} if node is not null and has the same order as initial remtoe node.
     */
    public boolean nodeAlive(@Nullable ClusterNode node) {
        return node != null && node.order() == this.node.order();
    }

    /**
     * @throws InterruptedException If interrupted.
     * @return {@code True} if reserved.
     */
    public boolean reserve() throws InterruptedException {
        synchronized (this) {
            while (!connected && reserved)
                wait();

            if (!connected)
                reserved = true;

            return !connected;
        }
    }

    /**
     * @param rcvCnt Number of messages received by remote node.
     */
    public void onHandshake(long rcvCnt) {
        synchronized (this) {
            if (!nodeLeft)
                ackReceived(rcvCnt);

            resendCnt = msgFuts.size();
        }
    }

    /**
     *
     */
    public void connected() {
        synchronized (this) {
            assert reserved;
            assert !connected;

            connected = true;

            if (handshakeReq != null) {
                IgniteInClosure<Boolean> c = handshakeReq.get2();

                assert c != null;

                c.apply(false);

                handshakeReq = null;
            }

            notifyAll();
        }
    }

    /**
     *
     */
    public void release() {
        GridNioFuture<?>[] futs = null;

        synchronized (this) {
            connected = false;

            if (handshakeReq != null) {
                IgniteInClosure<Boolean> c = handshakeReq.get2();

                assert c != null;

                handshakeReq = null;

                c.apply(true);
            }
            else {
                reserved = false;

                notifyAll();
            }

            if (nodeLeft && !msgFuts.isEmpty()) {
                futs = msgFuts.toArray(new GridNioFuture<?>[msgFuts.size()]);

                msgFuts.clear();
            }
        }

        if (futs != null)
            completeOnNodeLeft(futs);
    }

    /**
     * @param id Handshake ID.
     * @param c Closure to run on reserve.
     * @return {@code True} if reserved.
     */
    public boolean tryReserve(long id, IgniteInClosure<Boolean> c) {
        synchronized (this) {
            if (connected) {
                c.apply(false);

                return false;
            }

            if (reserved) {
                if (handshakeReq != null) {
                    assert handshakeReq.get1() != null;

                    long id0 = handshakeReq.get1();

                    assert id0 != id : id0;

                    if (id > id0) {
                        IgniteInClosure<Boolean> c0 = handshakeReq.get2();

                        assert c0 != null;

                        c0.apply(false);

                        handshakeReq = new IgniteBiTuple<>(id, c);
                    }
                    else
                        c.apply(false);
                }
                else
                    handshakeReq = new IgniteBiTuple<>(id, c);

                return false;
            }
            else {
                reserved = true;

                return true;
            }
        }
    }

    /**
     * @param futs Futures to complete.
     */
    private void completeOnNodeLeft(GridNioFuture<?>[] futs) {
        for (GridNioFuture<?> msg : futs) {
            IOException e = new IOException("Failed to send message, node has left: " + node.id());

            ((GridNioFutureImpl)msg).onDone(e);

            if (msg.ackClosure() != null)
                msg.ackClosure().apply(new IgniteException(e));
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioRecoveryDescriptor.class, this);
    }
}