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

package org.apache.ignite.internal.direct;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.direct.state.DirectMessageState;
import org.apache.ignite.internal.direct.state.DirectMessageStateItem;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.managers.communication.CompressedMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageMapType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.jetbrains.annotations.Nullable;

/**
 * Message reader implementation.
 */
public class DirectMessageReader implements MessageReader {
    /** Empty buffer to release the payload reference from {@link #tmpReader} streams between compressed reads. */
    private static final ByteBuffer EMPTY_BUF = ByteBuffer.wrap(new byte[0]);

    /** State. */
    @GridToStringInclude
    private final DirectMessageState<StateItem> state;

    /** Message factory. */
    private final MessageFactory msgFactory;

    /** Cache object processor. */
    private final IgniteCacheObjectProcessor cacheObjProc;

    /** Buffer for reading. */
    private ByteBuffer buf;

    /**
     * Cached stream of the current state item. Avoids re-resolving {@code state.item().stream} on every primitive
     * read; updated only when the current item changes (buffer set / nested read enter or exit).
     */
    private DirectByteBufferStream curStream;

    /** Reusable reader for compressed payload deserialization. Lazily created; thread-confined like the reader itself. */
    private DirectMessageReader tmpReader;

    /** Whether last field was fully read. */
    private boolean lastRead;

    /**
     * @param msgFactory Message factory.
     * @param cacheObjProc Cache object processor.
     */
    public DirectMessageReader(final MessageFactory msgFactory, IgniteCacheObjectProcessor cacheObjProc) {
        this.msgFactory = msgFactory;
        this.cacheObjProc = cacheObjProc;

        state = new DirectMessageState<>(StateItem.class, new IgniteOutClosure<StateItem>() {
            @Override public StateItem apply() {
                return new StateItem(msgFactory, cacheObjProc);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        this.buf = buf;

        curStream = state.item().stream;

        curStream.setBuffer(buf);
    }

    /**
     * Gets but buffer to read from.
     *
     * @return Byte buffer.
     */
    public ByteBuffer getBuffer() {
        return buf;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() {
        DirectByteBufferStream stream = curStream;

        byte val = stream.readByte();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        DirectByteBufferStream stream = curStream;

        short val = stream.readShort();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        DirectByteBufferStream stream = curStream;

        int val = stream.readInt();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        DirectByteBufferStream stream = curStream;

        long val = stream.readLong();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        DirectByteBufferStream stream = curStream;

        float val = stream.readFloat();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        DirectByteBufferStream stream = curStream;

        double val = stream.readDouble();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        DirectByteBufferStream stream = curStream;

        char val = stream.readChar();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        DirectByteBufferStream stream = curStream;

        boolean val = stream.readBoolean();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() {
        DirectByteBufferStream stream = curStream;

        byte[] arr = stream.readByteArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() {
        DirectByteBufferStream stream = curStream;

        short[] arr = stream.readShortArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() {
        DirectByteBufferStream stream = curStream;

        int[] arr = stream.readIntArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() {
        DirectByteBufferStream stream = curStream;

        long[] arr = stream.readLongArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() {
        DirectByteBufferStream stream = curStream;

        float[] arr = stream.readFloatArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() {
        DirectByteBufferStream stream = curStream;

        double[] arr = stream.readDoubleArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() {
        DirectByteBufferStream stream = curStream;

        char[] arr = stream.readCharArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() {
        DirectByteBufferStream stream = curStream;

        boolean[] arr = stream.readBooleanArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Override public String readString() {
        DirectByteBufferStream stream = curStream;

        String val = stream.readString();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public BitSet readBitSet() {
        DirectByteBufferStream stream = curStream;

        BitSet val = stream.readBitSet();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public UUID readUuid() {
        DirectByteBufferStream stream = curStream;

        UUID val = stream.readUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid readIgniteUuid() {
        DirectByteBufferStream stream = curStream;

        IgniteUuid val = stream.readIgniteUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readAffinityTopologyVersion() {
        DirectByteBufferStream stream = curStream;

        AffinityTopologyVersion val = stream.readAffinityTopologyVersion();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Message> T readMessage(boolean compress) {
        DirectByteBufferStream stream = curStream;

        T msg;

        if (compress)
            msg = readCompressedMessageAndDeserialize(
                stream,
                r -> r.state.item().stream.readMessage(r)
            );
        else {
            msg = stream.readMessage(this);

            lastRead = stream.lastFinished();
        }

        return msg;
    }

    /** {@inheritDoc} */
    @Override public CacheObject readCacheObject() {
        DirectByteBufferStream stream = curStream;

        CacheObject val = stream.readCacheObject();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject readKeyCacheObject() {
        DirectByteBufferStream stream = curStream;

        KeyCacheObject key = stream.readKeyCacheObject();

        lastRead = stream.lastFinished();

        return key;
    }

    /** {@inheritDoc} */
    @Override public GridLongList readGridLongList() {
        DirectByteBufferStream stream = curStream;

        GridLongList ll = stream.readGridLongList();

        lastRead = stream.lastFinished();

        return ll;
    }

    /** {@inheritDoc} */
    @Override public <T> T[] readObjectArray(MessageArrayType type) {
        DirectByteBufferStream stream = curStream;

        T[] msg = stream.readObjectArray(type, this);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public <C extends Collection<?>> C readCollection(MessageCollectionType type) {
        DirectByteBufferStream stream = curStream;

        C col = stream.readCollection(type, this);

        lastRead = stream.lastFinished();

        return col;
    }

    /** {@inheritDoc} */
    @Override public <M extends Map<?, ?>> M readMap(MessageMapType type, boolean compress) {
        DirectByteBufferStream stream = curStream;

        M map;

        if (compress)
            map = readCompressedMessageAndDeserialize(
                stream,
                r -> r.state.item().stream.readMap(type, r)
            );
        else {
            map = stream.readMap(type, this);

            lastRead = stream.lastFinished();
        }

        return map;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion readIgniteProductVersion() {
        DirectByteBufferStream stream = curStream;

        IgniteProductVersion v = stream.readIgniteProductVersion();

        lastRead = stream.lastFinished();

        return v;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion readGridCacheVersion() {
        DirectByteBufferStream stream = curStream;

        GridCacheVersion v = stream.readGridCacheVersion();

        lastRead = stream.lastFinished();

        return v;
    }

    /** {@inheritDoc} */
    @Override public boolean isLastRead() {
        return lastRead;
    }

    /** {@inheritDoc} */
    @Override public int state() {
        return state.item().state;
    }

    /** {@inheritDoc} */
    @Override public void incrementState() {
        state.item().state++;
    }

    /** {@inheritDoc} */
    @Override public void decrementState() {
        state.item().state--;
    }

    /** {@inheritDoc} */
    @Override public void beforeNestedRead() {
        state.forward();

        curStream = state.item().stream;

        curStream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public void afterNestedRead(boolean finished) {
        state.backward(finished);

        curStream = state.item().stream;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        state.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectMessageReader.class, this);
    }

    /** @return Deserialized object. */
    private <T> T readCompressedMessageAndDeserialize(DirectByteBufferStream stream, Function<DirectMessageReader, T> fun) {
        Message msg = stream.readMessage(this);

        lastRead = stream.lastFinished();

        if (!lastRead || msg == null)
            return null;

        assert msg instanceof CompressedMessage : msg;

        CompressedMessage msg0 = (CompressedMessage)msg;

        if (msg0.dataSize() == 0)
            return null;

        // Reuse the temp reader across fields/messages instead of allocating a fresh state stack each time.
        if (tmpReader == null)
            tmpReader = new DirectMessageReader(msgFactory, cacheObjProc);
        else
            tmpReader.reset();

        tmpReader.setBuffer(ByteBuffer.wrap(msg0.uncompressed()));

        T res;

        boolean ok = false;

        try {
            res = fun.apply(tmpReader);

            // The payload buffer is always complete, so a partial read means a corrupted stream, not a lack of data.
            if (!tmpReader.state.item().stream.lastFinished())
                throw new IgniteException("Failed to deserialize compressed payload: uncompressed data ended " +
                    "unexpectedly [dataSize=" + msg0.dataSize() + ']');

            ok = true;
        }
        finally {
            // Don't reuse a reader with half-read state.
            if (!ok)
                tmpReader = null;
        }

        // Don't pin the payload: the reader lives as long as the connection.
        tmpReader.releasePayload();

        lastRead = true;

        return res;
    }

    /** Releases the payload buffer reference from all streams of this reader. */
    private void releasePayload() {
        buf = EMPTY_BUF;

        state.forEachItem(item -> item.stream.setBuffer(EMPTY_BUF));
    }

    /**
     */
    private static class StateItem implements DirectMessageStateItem {
        /** Stream. */
        private final DirectByteBufferStream stream;

        /** State. */
        private int state;

        /**
         * @param msgFactory Message factory.
         * @param cacheObjProc Cache object processor.
         */
        public StateItem(MessageFactory msgFactory, IgniteCacheObjectProcessor cacheObjProc) {
            stream = new DirectByteBufferStream(msgFactory, cacheObjProc);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            state = 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StateItem.class, this);
        }
    }
}
