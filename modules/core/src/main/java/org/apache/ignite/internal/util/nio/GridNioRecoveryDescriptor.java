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

    /** Unacknowledged messages. */
    private final ArrayDeque<SessionWriteRequest> msgReqs;

    /** Number of messages to resend. */
    private int resendCnt;

    /** Number of received messages. */
    private long rcvCnt;

    /** Number of sent messages. */
    private long sentCnt;

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

    /** Number of descriptor reservations (for info purposes). */
    private int reserveCnt;

    /** */
    private final boolean pairedConnections;

    /**
     * @param pairedConnections {@code True} if in/out connections pair is used for communication with node.
     * @param queueLimit Maximum size of unacknowledged messages queue.
     * @param node Node.
     * @param log Logger.
     */
    public GridNioRecoveryDescriptor(
        boolean pairedConnections,
        int queueLimit,
        ClusterNode node,
        IgniteLogger log
    ) {
        assert !node.isLocal() : node;
        assert queueLimit > 0;

        msgReqs = new ArrayDeque<>(queueLimit);

        this.pairedConnections = pairedConnections;
        this.queueLimit = queueLimit;
        this.node = node;
        this.log = log;
    }

    /**
     * @return {@code True} if in/out connections pair is used for communication with node.
     */
    public boolean pairedConnections() {
        return pairedConnections;
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
     * @return Number of sent messages.
     */
    public long sent() {
        return sentCnt;
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
     * @return Maximum size of unacknowledged messages queue.
     */
    public int queueLimit() {
        return queueLimit;
    }

    /**
     * @param req Write request.
     * @return {@code False} if queue limit is exceeded.
     */
    public boolean add(SessionWriteRequest req) {
        assert req != null;

        if (!req.skipRecovery()) {
            if (resendCnt == 0) {
                msgReqs.addLast(req);

                sentCnt++;

                return msgReqs.size() < queueLimit;
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
                ", msgReqs=" + msgReqs.size() + ']');

        while (acked < rcvCnt) {
            SessionWriteRequest req = msgReqs.pollFirst();

            assert req != null : "Missed message [rcvCnt=" + rcvCnt +
                ", acked=" + acked +
                ", desc=" + this + ']';

            if (req.ackClosure() != null)
                req.ackClosure().apply(null);

            req.onAckReceived();

            acked++;
        }
    }

    /**
     * @return Last acked message by remote node.
     */
    public long acked() {
        return acked;
    }

    /**
     * Node left callback.
     *
     * @return {@code False} if descriptor is reserved.
     */
    public boolean onNodeLeft() {
        SessionWriteRequest[] reqs = null;

        synchronized (this) {
            nodeLeft = true;

            if (reserved)
                return false;

            if (!msgReqs.isEmpty()) {
                reqs = msgReqs.toArray(new SessionWriteRequest[msgReqs.size()]);

                msgReqs.clear();
            }
        }

        if (reqs != null)
            notifyOnNodeLeft(reqs);

        return true;
    }

    /**
     * @return Requests for unacknowledged messages.
     */
    public Deque<SessionWriteRequest> messagesRequests() {
        return msgReqs;
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

            if (!connected) {
                reserved = true;

                reserveCnt++;
            }

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

            resendCnt = msgReqs.size();
        }
    }

    /**
     *
     */
    public void onConnected() {
        synchronized (this) {
            assert reserved : this;
            assert !connected : this;

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
     * @return Connected flag.
     */
    public boolean connected() {
        synchronized (this) {
            return connected;
        }
    }

    /**
     * @return Reserved flag.
     */
    public boolean reserved() {
        synchronized (this) {
            return reserved;
        }
    }

    /**
     * @return Current handshake index.
     */
    public Long handshakeIndex() {
        synchronized (this) {
            return handshakeReq != null ? handshakeReq.get1() : null;
        }
    }

    /**
     *
     */
    public void release() {
        SessionWriteRequest[] futs = null;

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

            if (nodeLeft && !msgReqs.isEmpty()) {
                futs = msgReqs.toArray(new SessionWriteRequest[msgReqs.size()]);

                msgReqs.clear();
            }
        }

        if (futs != null)
            notifyOnNodeLeft(futs);
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

                reserveCnt++;

                return true;
            }
        }
    }

    /**
     * @return Number of descriptor reservations.
     */
    public int reserveCount() {
        synchronized (this) {
            return reserveCnt;
        }
    }

    /**
     * @param reqs Requests to notify about error.
     */
    private void notifyOnNodeLeft(SessionWriteRequest[] reqs) {
        IOException e = new IOException("Failed to send message, node has left: " + node.id());

        for (SessionWriteRequest req : reqs) {
            req.onError(e);

            if (req.ackClosure() != null)
                req.ackClosure().apply(new IgniteException(e));
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioRecoveryDescriptor.class, this);
    }
}
