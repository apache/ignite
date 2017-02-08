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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.jetbrains.annotations.Nullable;
import sun.nio.ch.DirectBuffer;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;

/**
 * Parser for direct messages.
 */
public class GridDirectParser implements GridNioParser {
    /** Message metadata key. */
    static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Reader metadata key. */
    static final int READER_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final IgniteLogger log;

    /** */
    private final MessageFactory msgFactory;

    /** */
    private final GridNioMessageReaderFactory readerFactory;

    /**
     * @param log Logger.
     * @param msgFactory Message factory.
     * @param readerFactory Message reader factory.
     */
    public GridDirectParser(IgniteLogger log, MessageFactory msgFactory, GridNioMessageReaderFactory readerFactory) {
        assert msgFactory != null;
        assert readerFactory != null;

        this.log = log;
        this.msgFactory = msgFactory;
        this.readerFactory = readerFactory;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object decode(GridNioSession ses, ByteBuffer buf)
        throws IOException, IgniteCheckedException {

        Message msg = ses.removeMeta(MSG_META_KEY);

        if(msg instanceof GridIoMessageBuffer)
            return decodeMsgBuf(ses, buf, (GridIoMessageBuffer) msg);
        else if(msg != null) {
            return decodeMessage(ses, buf, msg);
        }

        byte type = buf.get();

        if(type != -45)
            return decodeMessage(ses, buf, msgFactory.create(type));

        GridIoMessageBuffer msgBuf = createMessageBuffer(buf);

        if(msgBuf != null)
            return decodeMsgBuf(ses, buf, msgBuf);

        return null;
    }

    /**
     * @param buf Buffer that contains input data.
     * @return Newly created {@link GridIoMessageBuffer} instance or null if the message header was not fully loaded
     */
    private GridIoMessageBuffer createMessageBuffer(ByteBuffer buf){
        if(buf.remaining() < 10) {
            buf.position(buf.position() - 1);
            return null;
        }

        byte[] bufArr = buf.isDirect() ? null : buf.array();
        long bufOff = buf.isDirect() ? ((DirectBuffer)buf).address() : BYTE_ARR_OFF;


        int pos = buf.position();

        byte plc = GridUnsafe.getByte(bufArr, bufOff + pos);
        int partition = GridUnsafe.getInt(bufArr, bufOff + pos + 1);
        boolean ordered = GridUnsafe.getBoolean(bufArr, bufOff + pos + 5);
        int topicOrdinal = GridUnsafe.getInt(bufArr, bufOff + pos + 6);

        buf.position(pos + 10);

        return new GridIoMessageBuffer(plc, partition, ordered, topicOrdinal);
    }

    /**
     * @param ses Session on which bytes are read.
     * @param buf Buffer that contains input data.
     * @param msg Message to populate.
     * @return Decoded message or {@code null} if the message was not fully loaded.
     * @throws IOException If exception occurred while reading data.
     * @throws IgniteCheckedException If any user-specific error occurred.
     */
    @Nullable private Object decodeMessage(GridNioSession ses, ByteBuffer buf, Message msg)
        throws IOException, IgniteCheckedException {

        MessageReader reader = ses.meta(READER_META_KEY);

        if (reader == null)
            ses.addMeta(READER_META_KEY, reader = readerFactory.reader(ses, msgFactory));

        try {
            boolean finished = false;

            if (buf.hasRemaining()) {
                if (reader != null)
                    reader.setCurrentReadClass(msg.getClass());

                finished = msg.readFrom(buf, reader);
            }

            if (finished) {
                if (reader != null)
                    reader.reset();

                return msg;
            }
            else {
                ses.addMeta(MSG_META_KEY, msg);

                return null;
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed to read message [msg=" + msg +
                ", buf=" + buf +
                ", reader=" + reader +
                ", ses=" + ses + "]",
                e);

            throw e;
        }
    }

    /**
     * @param ses Session on which bytes are read.
     * @param buf Buffer that contains input data.
     * @param msg Message to populate.
     * @return Decoded message or {@code null} if the message was not fully loaded.
     * @throws IOException If exception occurred while reading data.
     * @throws IgniteCheckedException If any user-specific error occurred.
     */
    @Nullable private Object decodeMsgBuf(GridNioSession ses, ByteBuffer buf, GridIoMessageBuffer msg)
        throws IOException, IgniteCheckedException {

        if(buf.remaining() < 5) {
            ses.addMeta(MSG_META_KEY, msg);

            return null;
        }


        byte[] bufArr = buf.isDirect() ? null : buf.array();
        long bufOff = buf.isDirect() ? ((DirectBuffer)buf).address() : BYTE_ARR_OFF;

        int pos = buf.position();

        int size = GridUnsafe.getInt(bufArr, bufOff + pos);

        if (size + 5 > buf.remaining()) {
            ses.addMeta(MSG_META_KEY, msg);

            return null;
        }

        boolean last = GridUnsafe.getBoolean(bufArr, bufOff + pos + 4);

        buf.position(pos + 5);

        msg.add(buf, size);

        if(last)
            return msg;

        ses.addMeta(MSG_META_KEY, msg);

        return null;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        // No encoding needed for direct messages.
        throw new UnsupportedEncodingException();
    }
}
