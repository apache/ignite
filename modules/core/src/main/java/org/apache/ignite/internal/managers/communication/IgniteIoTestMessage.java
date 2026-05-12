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

package org.apache.ignite.internal.managers.communication;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;

/**
 *
 */
public class IgniteIoTestMessage implements MarshallableMessage {
    /** */
    private static final byte FLAG_PROC_FROM_NIO = 1;

    /** */
    @Order(0)
    long id;

    /** */
    @Order(1)
    byte flags;

    /** */
    @Order(2)
    boolean req;

    /** */
    @Order(3)
    byte[] payload;

    /** */
    @Order(4)
    long reqCreateTs;

    /** */
    @Order(5)
    long reqSndTs;

    /** */
    @Order(6)
    long reqSndTsMillis;

    /** */
    @Order(7)
    long reqRcvTs;

    /** */
    @Order(8)
    long reqRcvTsMillis;

    /** */
    @Order(9)
    long reqProcTs;

    /** */
    @Order(10)
    long resSndTs;

    /** */
    @Order(11)
    long resSndTsMillis;

    /** */
    @Order(12)
    long resRcvTs;

    /** */
    @Order(13)
    long resRcvTsMillis;

    /** */
    @Order(14)
    long resProcTs;

    /** */
    private UUID sndNodeId;

    /**
     *
     */
    public IgniteIoTestMessage() {
        // No-op.
    }

    /**
     * @param id Message ID.
     * @param req Request flag.
     * @param payload Payload.
     */
    public IgniteIoTestMessage(long id, boolean req, byte[] payload) {
        this.id = id;
        this.req = req;
        this.payload = payload;

        reqCreateTs = System.nanoTime();
    }

    /**
     * @return {@code True} if message should be processed from NIO thread
     * (otherwise message is submitted to system pool).
     */
    public boolean processFromNioThread() {
        return isFlag(FLAG_PROC_FROM_NIO);
    }

    /**
     * @param procFromNioThread {@code True} if message should be processed from NIO thread.
     */
    public void processFromNioThread(boolean procFromNioThread) {
        setFlag(procFromNioThread, FLAG_PROC_FROM_NIO);
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return {@code true} if this is request.
     */
    public boolean request() {
        return req;
    }

    /**
     * @return ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Request create timestamp.
     */
    public long requestCreateTs() {
        return reqCreateTs;
    }

    /**
     * @return Request send timestamp.
     */
    public long requestSendTs() {
        return reqSndTs;
    }

    /**
     * @return Request receive timestamp.
     */
    public long requestReceiveTs() {
        return reqRcvTs;
    }

    /**
     * @return Request process started timestamp.
     */
    public long requestProcessTs() {
        return reqProcTs;
    }

    /**
     * @return Response send timestamp.
     */
    public long responseSendTs() {
        return resSndTs;
    }

    /**
     * @return Response receive timestamp.
     */
    public long responseReceiveTs() {
        return resRcvTs;
    }

    /**
     * @return Request send timestamp (millis).
     */
    public long requestSendTsMillis() {
        return reqSndTsMillis;
    }

    /**
     * @return Request received timestamp (millis).
     */
    public long requestReceivedTsMillis() {
        return reqRcvTsMillis;
    }

    /**
     * @return Response received timestamp (millis).
     */
    public long responseReceivedTsMillis() {
        return resRcvTsMillis;
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onAfterRead() {
        if (req && reqRcvTs == 0) {
            reqRcvTs = System.nanoTime();

            reqRcvTsMillis = System.currentTimeMillis();
        }

        if (!req && resRcvTs == 0) {
            resRcvTs = System.nanoTime();

            resRcvTsMillis = System.currentTimeMillis();
        }
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onBeforeWrite() {
        if (req && reqSndTs == 0) {
            reqSndTs = System.nanoTime();

            reqSndTsMillis = System.currentTimeMillis();
        }

        if (!req && resSndTs == 0) {
            resSndTs = System.nanoTime();

            resSndTsMillis = System.currentTimeMillis();
        }
    }

    /**
     *
     */
    public void copyDataFromRequest(IgniteIoTestMessage req) {
        reqCreateTs = req.reqCreateTs;

        reqSndTs = req.reqSndTs;
        reqSndTsMillis = req.reqSndTsMillis;

        reqRcvTs = req.reqRcvTs;
        reqRcvTsMillis = req.reqRcvTsMillis;
    }

    /**
     *
     */
    public void onRequestProcessed() {
        reqProcTs = System.nanoTime();
    }

    /**
     *
     */
    public void onResponseProcessed() {
        resProcTs = System.nanoTime();
    }

    /**
     * @return Response processed timestamp.
     */
    public long responseProcessedTs() {
        return resProcTs;
    }

    /**
     * @return Sender node ID.
     */
    public UUID senderNodeId() {
        return sndNodeId;
    }

    /**
     * @param sndNodeId Sender node ID.
     */
    public void senderNodeId(UUID sndNodeId) {
        this.sndNodeId = sndNodeId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        onBeforeWrite();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        onAfterRead();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
