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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class IgniteIoTestMessage implements Message {
    /** */
    private static byte FLAG_PROC_FROM_NIO = 1;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long id;

    /** */
    private byte flags;

    /** */
    private boolean req;

    /** */
    private byte payload[];

    /** */
    private long reqCreateTs;

    /** */
    private long reqSndTs;

    /** */
    private long reqSndTsMillis;

    /** */
    private long reqRcvTs;

    /** */
    private long reqRcvTsMillis;

    /** */
    private long reqProcTs;

    /** */
    private long resSndTs;

    /** */
    private long resSndTsMillis;

    /** */
    private long resRcvTs;

    /** */
    private long resRcvTsMillis;

    /** */
    private long resProcTs;

    /** */
    @GridDirectTransient
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
     * @return Response process timestamp.
     */
    public long responseProcessTs() {
        return resProcTs;
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
     * @return Response send timestamp (millis).
     */
    public long responseSendTsMillis() {
        return resSndTsMillis;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        onBeforeWrite();

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("id", id))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("payload", payload))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeBoolean("req", req))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("reqCreateTs", reqCreateTs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("reqProcTs", reqProcTs))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("reqRcvTs", reqRcvTs))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("reqRcvTsMillis", reqRcvTsMillis))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong("reqSndTs", reqSndTs))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("reqSndTsMillis", reqSndTsMillis))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeLong("resProcTs", resProcTs))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeLong("resRcvTs", resRcvTs))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeLong("resRcvTsMillis", resRcvTsMillis))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeLong("resSndTs", resSndTs))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeLong("resSndTsMillis", resSndTsMillis))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                payload = reader.readByteArray("payload");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                req = reader.readBoolean("req");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                reqCreateTs = reader.readLong("reqCreateTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                reqProcTs = reader.readLong("reqProcTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                reqRcvTs = reader.readLong("reqRcvTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                reqRcvTsMillis = reader.readLong("reqRcvTsMillis");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                reqSndTs = reader.readLong("reqSndTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                reqSndTsMillis = reader.readLong("reqSndTsMillis");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                resProcTs = reader.readLong("resProcTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                resRcvTs = reader.readLong("resRcvTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                resRcvTsMillis = reader.readLong("resRcvTsMillis");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                resSndTs = reader.readLong("resSndTs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                resSndTsMillis = reader.readLong("resSndTsMillis");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        onAfterRead();

        return reader.afterMessageRead(IgniteIoTestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -43;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 15;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
